package outbox

import "errors"

var (
	ErrInvalidEventData        = errors.New("Invalid Event data")
	ErrConvertToExternalEvent  = errors.New("Failed !! to convert to external event.")
	ErrSkipThisEvent           = errors.New("Info !! event skipped by converter implementation.")
	ErrProjectorNotInitialized = errors.New("Error !! projector not initiallized.")
)
