package projector

import (
	"context"
	"encoding/json"
	"runtime"
	"sync"
	"time"

	outbox "github.com/encounter27/eh-outbox"
	"github.com/globalsign/mgo"
	eh "github.com/looplab/eventhorizon"
	"github.com/looplab/eventhorizon/repo/mongodb"
	kafka "github.com/segmentio/kafka-go"
)

type KafkaProjector struct {
	ID            string
	OffsetRepo    *mongodb.Repo
	MsgOutBoxRepo *mongodb.Repo
	Writer        *kafka.Writer
	Wg            sync.WaitGroup
	State         chan int
	ec            outbox.Converter
}

// Private global singleton variable
// Each microservice will have only one such instance
var kafkaProjector *KafkaProjector

func InitiallizeKafkaProjector(projectorId string, offsetRepo *mongodb.Repo,
	outboxRepo *mongodb.Repo, writer *kafka.Writer, c outbox.Converter) {

	if kafkaProjector == nil {
		kafkaProjector = new(KafkaProjector)

		kafkaProjector.ID = projectorId
		kafkaProjector.OffsetRepo = offsetRepo
		kafkaProjector.MsgOutBoxRepo = outboxRepo
		kafkaProjector.Writer = writer
		kafkaProjector.State = make(chan int, 1)

		kafkaProjector.ec = c
		if c == nil {
			kafkaProjector.ec = new(outbox.DefaultConverter)
		}
	}

	outbox.GetReadProjectorGroup().RegisterProjector(outbox.ProjectorID(projectorId), kafkaProjector)
}

func GetKafkaProjector() (*KafkaProjector, error) {
	if kafkaProjector == nil {
		return nil, outbox.ErrProjectorNotInitialized
	}

	return kafkaProjector, nil
}

func (p *KafkaProjector) SetState(state int) {
	p.State <- state
}

// Need ckt breaker if kafka is not available
func (p *KafkaProjector) WriteToReadside(ctx context.Context, key string, data interface{}) error {
	if bytes, err := json.Marshal(data); err == nil {
		return p.Writer.WriteMessages(ctx,
			kafka.Message{
				Key:   []byte(key),
				Value: []byte(bytes),
			},
		)
	} else {
		return err
	}
}

func (p *KafkaProjector) Start() {
	go p.Worker()

	go func() {
		p.State <- outbox.Running
	}()
}

func (p *KafkaProjector) Worker() {
	p.Wg.Add(1)
	defer p.Wg.Done()

	ctx := context.Background()

	var docOffset outbox.ProjectorOffset
	docOffset.New(p.ID)
	state := outbox.Paused

	for {
		select {
		case state = <-p.State:
			switch state {
			case outbox.Running:
				for {
					docOffset.Read(ctx, p.OffsetRepo)
					filter := docOffset.Filter()
					update := docOffset.Update()
					change := mgo.Change{Update: update, ReturnNew: true}

					var holdEvent outbox.HoldOutboxEvent
					if err := holdEvent.FindAndModify(ctx, p.MsgOutBoxRepo, filter, change); err == nil {
						ehEvent := eh.NewEvent(holdEvent.EventType, holdEvent.Data, holdEvent.Timestamp)

						if data, err := p.ec.ConvertToExternalEvent(ctx, ehEvent); err == nil { // Ignore
							if err := p.WriteToReadside(ctx, string("key"), data); err == nil { // Need ckt breaker for kafka down
								// Update offset if successfully published
								docOffset.Set(holdEvent.ID.Hex())
								docOffset.Write(ctx, p.OffsetRepo)
							}
						} else if err == outbox.ErrSkipThisEvent {
							docOffset.Set(holdEvent.ID.Hex())
							docOffset.Write(ctx, p.OffsetRepo)
						}
					} else { // Needs to handle properly
						// Incase of Notfound errors stop the routine
						// Also set the projector state to Paused
						go p.SetState(outbox.Paused)
						break
					}
				}
			case outbox.Paused:
			}
		default:
			runtime.Gosched()
		}
		time.Sleep(time.Second)
	}
}
