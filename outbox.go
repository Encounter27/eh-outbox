package outbox

import (
	"context"
	"fmt"
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	eh "github.com/looplab/eventhorizon"
	"github.com/looplab/eventhorizon/repo/mongodb"
)

type EventOutbox struct {
	InProg    bool         `json:"inProg"         bson:"inProg"`
	Done      bool         `json:done             bson:"done"`
	EventType eh.EventType `json:"event_type"     bson:"event_type"`
	Data      interface{}  `json:"data"           bson:"data"`
	Timestamp time.Time    `json:"timestamp"      bson:"timestamp"`
}

func (eventOutbox *EventOutbox) SaveToOutbox(repo *mongodb.Repo, ctx context.Context,
	id interface{}, eventType eh.EventType) error {
	if err := repo.Collection(ctx, func(c *mgo.Collection) error {
		err := c.Insert(eventOutbox)

		return err
	}); err != nil {
		return fmt.Errorf("Failed to save Message in out box: %s", eventType)
	}

	// Set all readside projections to Running state Asynchronously
	rpg := GetReadProjectorGroup()
	go rpg.setStateToAllProjectors(Running)

	return nil
}

type Outbox interface {
	FindAndModify(ctx context.Context, repo *mongodb.Repo, filter bson.M, change mgo.Change) error
}

type HoldOutboxEvent struct {
	ID        bson.ObjectId `json:"_id"            bson:"_id,omitempty"`
	InProg    bool          `json:"inProg"         bson:"inProg"`
	Done      bool          `json:done             bson:"done"`
	EventType eh.EventType  `json:"event_type"     bson:"event_type"`
	Data      interface{}   `json:"data"           bson:"data"`
	Timestamp time.Time     `json:"timestamp"      bson:"timestamp"`
}

// Need ckt breaker to save db call incase there is no more event to be scan by projector
func (holdEvent *HoldOutboxEvent) FindAndModify(ctx context.Context, repo *mongodb.Repo, filter bson.M, change mgo.Change) error {
	err := repo.Collection(ctx, func(c *mgo.Collection) error {
		_, err := c.Find(filter).Apply(change, &holdEvent)

		return err
	})

	return err
}