package outbox

import (
	"context"

	"github.com/looplab/eventhorizon/repo/mongodb"
)

// Possible worker states.
const (
	Stopped = 0
	Paused  = 1
	Running = 2
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
