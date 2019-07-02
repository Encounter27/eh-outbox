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
	Filter() bson.M
	Update() bson.M
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

func (k ProjectorOffset) Filter() bson.M {
	return bson.M{
		"inProg": false,
		"done":   false,
		"_id": bson.M{
			"$gt": bson.ObjectIdHex(k.Offset),
		},
	}
}

func (k ProjectorOffset) Update() bson.M {
	return bson.M{
		"$set": bson.M{
			"inProg": false,
		},
	}
}

func (docOffset *ProjectorOffset) Read(ctx context.Context, repo *mongodb.Repo) {
	if err := repo.Collection(ctx, func(c *mgo.Collection) error {
		err := c.Find(bson.M{"_id": "kafka_projector"}).One(&docOffset)
		return err
	}); err != nil { // Need to handle properly
		docOffset.Offset = "000000000000000000000000"
	}
}

func (docOffset ProjectorOffset) Write(ctx context.Context, repo *mongodb.Repo) {
	if err := repo.Collection(ctx, func(c *mgo.Collection) error {
		_, err := c.Upsert(bson.M{"_id": "kafka_projector"}, docOffset)

		return err
	}); err != nil {
		fmt.Println("Failed to update the offset for [kafka_projector]")
	}
}
