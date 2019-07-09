package outbox

import (
	"context"

	"github.com/looplab/eventhorizon/repo/mongodb"
)

// IProjector is the interface for all the projector
type IProjector interface {
	Name() string
	Bit() int32
	AssignProjectorID(offsetRepo *mongodb.Repo)
	OutboxRepo() *mongodb.Repo
	OffsetRepo() *mongodb.Repo
	Converter() IConverter
	WriteToReadside(ctx context.Context, key string, data interface{}) error
}

type projectorID string
