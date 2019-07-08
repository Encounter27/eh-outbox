package outbox

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/looplab/eventhorizon/repo/mongodb"
)

type iOffset interface {
	new(name string, bit string)
	set(offset string)
	filterQuery() bson.M
	updateInprog(projectorBit int32) bson.M
	updateDone(projectorBit int32) bson.M
	read(ctx context.Context, repo *mongodb.Repo)
	write(ctx context.Context, repo *mongodb.Repo)
}

type projectorOffset struct {
	NameID string `json:"_id"         bson:"_id"`
	Bit    int32  `json:"bit"         bson:"bit"`
	Offset string `json:"offset"      bson:"offset"`
}

func (docOffset *projectorOffset) new(name string, bit int32) {
	docOffset.NameID = name
	docOffset.Bit = bit
	docOffset.Offset = "000000000000000000000000"
}

func (docOffset *projectorOffset) set(offset string) {
	docOffset.Offset = offset
}

func (docOffset projectorOffset) filterQuery(projectorBit int32) bson.M {
	return bson.M{
		"inProg": bson.M{"$bitsAllClear": projectorBit},
		"done":   bson.M{"$bitsAllClear": projectorBit},
		"_id": bson.M{
			"$gt": bson.ObjectIdHex(docOffset.Offset),
		},
	}
}

// Specific projector is in progress
func (docOffset projectorOffset) updateInprog(projectorBit int32) bson.M {
	return bson.M{
		"$bit": bson.M{"inProg": bson.M{"or": projectorBit}},
	}
}

// Specific projector is done with the projection
func (docOffset projectorOffset) updateDone(projectorBit int32) bson.M {
	return bson.M{
		"$bit": bson.M{"done": bson.M{"or": projectorBit}},
	}
}

func (docOffset *projectorOffset) read(ctx context.Context, repo *mongodb.Repo, projectorID string) {
	if err := repo.Collection(ctx, func(c *mgo.Collection) error {
		err := c.Find(bson.M{"_id": projectorID}).One(&docOffset)
		return err
	}); err != nil { // Need to handle properly
		docOffset.Offset = "000000000000000000000000"
	}
}

func (docOffset projectorOffset) write(ctx context.Context, repo *mongodb.Repo, projectorID string) {
	if err := repo.Collection(ctx, func(c *mgo.Collection) error {
		_, err := c.Upsert(bson.M{"_id": projectorID}, docOffset)

		return err
	}); err != nil {
		fmt.Printf("Failed to update the offset for [%s]", projectorID)
	}
}

var assignProjectorBitMu sync.RWMutex

// AssignProjectorID will assign a bit position to the concrete projector at the time of projector registration
func AssignProjectorID(name string, offsetRepo *mongodb.Repo) int32 {
	assignProjectorBitMu.Lock()
	defer assignProjectorBitMu.Unlock()

	var (
		count int
		err   error
		d     projectorOffset
	)

	err = offsetRepo.Collection(context.Background(), func(c *mgo.Collection) error {
		err = c.Find(bson.M{"_id": name}).One(&d)
		count, _ = c.Count()

		return err
	})

	if err != nil && strings.Contains(err.Error(), mgo.ErrNotFound.Error()) {
		return int32(1 << uint(count))
	}

	return d.Bit
}
