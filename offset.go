package outbox

import (
	"context"
	"fmt"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/looplab/eventhorizon/repo/mongodb"
)

type Offset interface {
	New(id string)
	Set(offset string)
	FilterQuery() bson.M
	UpdateInprog(projectorBit int32) bson.M
	UpdateDone(projectorBit int32) bson.M
	Read(ctx context.Context, repo *mongodb.Repo)
	Write(ctx context.Context, repo *mongodb.Repo)
}

type ProjectorOffset struct {
	ID     string `json:"_id"         bson:"_id"`
	Offset string `json:"offset"      bson:"offset"`
}

func (k *ProjectorOffset) New(id string) {
	k.ID = id
	k.Offset = "000000000000000000000000"
}

func (k *ProjectorOffset) Set(offset string) {
	k.Offset = offset
}

func (k ProjectorOffset) FilterQuery(projectorBit int32) bson.M {
	return bson.M{
		"inProg": bson.M{"$bitsAllClear": projectorBit},
		"done":   bson.M{"$bitsAllClear": projectorBit},
		"_id": bson.M{
			"$gt": bson.ObjectIdHex(k.Offset),
		},
	}
}

// Specific projector is in progress
func (k ProjectorOffset) UpdateInprog(projectorBit int32) bson.M {
	return bson.M{
		"$bit": bson.M{"inProg": bson.M{"or": projectorBit}},
	}
}

// Specific projector is done with the projection
func (k ProjectorOffset) UpdateDone(projectorBit int32) bson.M {
	return bson.M{
		"$bit": bson.M{"done": bson.M{"or": projectorBit}},
	}
}

func (docOffset *ProjectorOffset) Read(ctx context.Context, repo *mongodb.Repo, projectorID string) {
	if err := repo.Collection(ctx, func(c *mgo.Collection) error {
		err := c.Find(bson.M{"_id": projectorID}).One(&docOffset)
		return err
	}); err != nil { // Need to handle properly
		docOffset.Offset = "000000000000000000000000"
	}
}

func (docOffset ProjectorOffset) Write(ctx context.Context, repo *mongodb.Repo, projectorID string) {
	if err := repo.Collection(ctx, func(c *mgo.Collection) error {
		_, err := c.Upsert(bson.M{"_id": projectorID}, docOffset)

		return err
	}); err != nil {
		fmt.Printf("Failed to update the offset for [%s]", projectorID)
	}
}
