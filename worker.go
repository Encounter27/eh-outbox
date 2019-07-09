package outbox

import (
	"context"
	"runtime"
	"sync"
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	eh "github.com/looplab/eventhorizon"
)

// Possible worker states.
const (
	Stopped = 0
	Paused  = 1
	Running = 2
)

type iWorker interface {
	start()
	run()
	setState(state int)
}

type worker struct {
	p     IProjector
	wg    sync.WaitGroup
	state chan int
}

func newWorker(p IProjector) iWorker {
	w := new(worker)
	w.p = p
	w.state = make(chan int, 1)

	return w
}

func (w *worker) setState(state int) {
	w.state <- state
}

func (w *worker) start() {
	go w.run()

	go func() {
		w.setState(Running)
	}()
}

func (w *worker) run() {
	w.wg.Add(1)
	defer w.wg.Done()

	ctx := context.Background()
	var docOffset projectorOffset
	docOffset.new(w.p.Name(), w.p.Bit())
	filter := docOffset.filterQuery(w.p.Bit())
	updateInprog := docOffset.updateInprog(w.p.Bit())
	updateDone := docOffset.updateDone(w.p.Bit())
	changeInprog := mgo.Change{Update: updateInprog, ReturnNew: true}
	changeDone := mgo.Change{Update: updateDone, ReturnNew: true}
	state := Paused

	for {
		select {
		case state = <-w.state:
			switch state {
			case Running:
				for {
					docOffset.read(ctx, w.p.OffsetRepo(), w.p.Name())

					var holdEvent HoldOutboxEvent
					if err := holdEvent.FindAndModify(ctx, w.p.OutboxRepo(), filter, changeInprog); err == nil {
						ehEvent := eh.NewEvent(holdEvent.EventType, holdEvent.Data, holdEvent.Timestamp)

						if data, err := w.p.Converter().ConvertToExternalEvent(ctx, ehEvent); err == nil {

							for {
								if err := w.p.WriteToReadside(ctx, string("key"), data); err == nil {
									docOffset.set(holdEvent.ID.Hex())
									docOffset.write(ctx, w.p.OffsetRepo(), w.p.Name())
									holdEvent.FindAndModify(ctx, w.p.OutboxRepo(), bson.M{"_id": holdEvent.ID}, changeDone)
									break
								}

								// Need ckt breaker for kafka down
							}
						} else if err == ErrSkipThisEvent {
							docOffset.set(holdEvent.ID.Hex())
							docOffset.write(ctx, w.p.OffsetRepo(), w.p.Name())
							holdEvent.FindAndModify(ctx, w.p.OutboxRepo(), bson.M{"_id": holdEvent.ID}, changeDone)
						}
					} else { // Needs to handle properly
						go w.setState(Paused)
						break
					}
				}
			case Paused:

			}
		default:
			runtime.Gosched()
		}
		time.Sleep(time.Second)
	}
}
