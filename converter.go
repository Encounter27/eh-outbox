package outbox

import (
	"context"

	eh "github.com/looplab/eventhorizon"
)

type Converter interface {
	ConvertToExternalEvent(ctx context.Context, event eh.Event) (interface{}, error)
}

type DefaultConverter struct {
}

func (d *DefaultConverter) ConvertToExternalEvent(ctx context.Context, event eh.Event) (interface{}, error) {
	if event.Data() == nil {
		return nil, ErrInvalidEventData
	}

	return event.Data(), nil
}
