package processor

import "context"

// Inbox makes the effects of a Handler idempotent: an observation may be handled again after a
// retry, a crash or a session change.
type Inbox interface {
	// ProcessOnce runs operation unless key was processed before, and records key as processed
	// only when operation succeeds: ideally in the same database transaction as the effects of
	// operation, which ctx may carry.
	ProcessOnce(ctx context.Context, key string, operation func(ctx context.Context) error) error
}

// Idempotent returns a Handler that calls next through inbox with the observation ID as the key.
func Idempotent(inbox Inbox, next Handler) Handler {
	return HandlerFunc(func(ctx context.Context, observation Observation) error {
		return inbox.ProcessOnce(ctx, observation.ID, func(ctx context.Context) error {
			return next.Handle(ctx, observation)
		})
	})
}
