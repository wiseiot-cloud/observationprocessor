package processor

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"
)

// Handler handles the observations of a processor. Within a partition, the calls follow the offset
// order, one at a time; up to Config.MaxConcurrentPartitions partitions call it at the same time,
// each for one page. The context ends after Config.HandlerTimeout, or when the session ends.
//
// Returning nil resolves the observation. Any other error stops the page and reads it again after a
// delay, without a commit; RetryAfter sets the least delay. Permanent resolves the observation once
// the FailureSink has recorded it. A panic is an ordinary error.
type Handler interface {
	Handle(ctx context.Context, observation Observation) error
}

// HandlerFunc is a Handler function.
type HandlerFunc func(ctx context.Context, observation Observation) error

func (f HandlerFunc) Handle(ctx context.Context, observation Observation) error {
	return f(ctx, observation)
}

// handlerError is an error that RetryAfter or Permanent classified.
type handlerError struct {
	err        error
	retryAfter time.Duration
	permanent  bool
}

func (e *handlerError) Error() string { return e.err.Error() }

func (e *handlerError) Unwrap() error { return e.err }

// RetryAfter returns err for a Handler to read the page again no sooner than delay.
func RetryAfter(err error, delay time.Duration) error {
	if err == nil {
		err = errors.New("the Handler asked for a retry")
	}
	return &handlerError{err: err, retryAfter: delay}
}

// Permanent returns err for a Handler that never succeeds with the observation: the observation is
// resolved once the FailureSink has recorded it. Without a FailureSink, Run ends with
// ErrorInvalidConfig.
func Permanent(err error) error {
	if err == nil {
		err = errors.New("the Handler failed permanently")
	}
	return &handlerError{err: err, permanent: true}
}

// FailureSink durably records the observations whose Handler returned a Permanent error, for
// example in a table of the application. A page whose record fails is read again.
type FailureSink interface {
	RecordFailure(ctx context.Context, record FailureRecord) error
}

// FailureSinkFunc is a FailureSink function.
type FailureSinkFunc func(ctx context.Context, record FailureRecord) error

func (f FailureSinkFunc) RecordFailure(ctx context.Context, record FailureRecord) error {
	return f(ctx, record)
}

// FailureRecord is an observation whose Handler returned a Permanent error.
type FailureRecord struct {
	ProcessorID string
	// Observation lacks its Payload unless Config.IncludePayloadInFailures.
	Observation Observation
	// Err is the error that the Handler passed to Permanent.
	Err      error
	FailedAt time.Time
}

// RetryPolicy spaces the retries after a failure.
type RetryPolicy interface {
	// RetryDelay returns the delay before retry attempt, 1 for the first retry after a success.
	RetryDelay(attempt int) time.Duration
}

// BoundedRetryPolicy waits Initial before the first retry and doubles the delay for each further
// one, up to Maximum, then waits a random 50 to 100% of it. Zero fields are 100 ms and 10 s.
type BoundedRetryPolicy struct {
	Initial time.Duration
	Maximum time.Duration
}

func (p BoundedRetryPolicy) RetryDelay(attempt int) time.Duration {
	delay, maximum := p.Initial, p.Maximum
	if delay <= 0 {
		delay = 100 * time.Millisecond
	}
	if maximum <= 0 {
		maximum = 10 * time.Second
	}
	for ; attempt > 1 && delay < maximum; attempt-- {
		delay *= 2
	}
	delay = min(delay, maximum)
	return delay/2 + rand.N(delay/2+1)
}

// classify returns how a Handler error resolves: the least retry delay of an ordinary error, or
// the cause of a Permanent one.
func classify(err error) (retryAfter time.Duration, permanent error) {
	var classified *handlerError
	if !errors.As(err, &classified) {
		return 0, nil
	}
	if classified.permanent {
		return 0, classified.err
	}
	return classified.retryAfter, nil
}
