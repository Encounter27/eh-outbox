package outbox

import (
	"context"

	eh "github.com/looplab/eventhorizon"
)

// IConverter is to inject logic to convert internal to external event
type IConverter interface {
	ConvertToExternalEvent(ctx context.Context, event eh.Event) (interface{}, error)
}

// DefaultConverter to publish the internal event as it is
type DefaultConverter struct {
}

// ConvertToExternalEvent for defautlt converter
func (d *DefaultConverter) ConvertToExternalEvent(ctx context.Context, event eh.Event) (interface{}, error) {
	if event.Data() == nil {
		return nil, ErrInvalidEventData
	}

	return event.Data(), nil
}
