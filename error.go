package outbox

import "errors"

var (
	// ErrInvalidEventData error returned when a events data is invalid
	ErrInvalidEventData = errors.New("Invalid Event data")
	// ErrConvertToExternalEvent error returned when converter fails to convert internal event to external event
	ErrConvertToExternalEvent = errors.New("Failed !! to convert to external event")
	// ErrSkipThisEvent error returned when custom converter skip this event, while pulishing to read side.
	ErrSkipThisEvent = errors.New("Info !! event skipped by converter implementation")
	// ErrProjectorNotInitialized error returned when a projector object not been initialized properly.
	ErrProjectorNotInitialized = errors.New("Error !! projector not initiallized")
)
