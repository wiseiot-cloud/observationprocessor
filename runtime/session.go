package processor

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	wire "github.com/wiseiot-cloud/observationprocessor/internal/wire/processor"
)

// session is a session of the Processor Runtime API, which owns the processor.
type session struct {
	p       *Processor
	handler Handler
	id      string
	// slots bounds the partitions that call the Handler at the same time.
	slots chan struct{}
	// end ends the session with a fencing or a failure that retrying cannot fix; the first one is
	// the result of the session.
	end func(error)

	mu     sync.Mutex
	paused bool
	// retrying counts the partitions that wait to retry a failure.
	retrying int
	// changed closes when paused or retrying changes.
	changed chan struct{}
}

// serve processes the partitions in the session that opened, and returns once its Handler calls
// have returned: nil when ctx ended, and otherwise an *Error with ErrorSessionFenced or the code of
// the failure that retrying cannot fix. A session that ends for good closes.
func (p *Processor) serve(ctx context.Context, opened *wire.OpenSessionResponse, handler Handler) error {
	partitions := int(opened.GetPartitionCount())
	p.mu.Lock()
	p.snapshot.OwnerEpoch = int64(opened.GetOwnerEpoch())
	p.snapshot.Filter = filterFrom(opened.GetFilter())
	// A new session may follow a filter change or a checkpoint reset: the partitions start over.
	p.snapshot.Partitions = make([]PartitionSnapshot, partitions)
	for number := range p.snapshot.Partitions {
		p.snapshot.Partitions[number].Partition = number
	}
	p.mu.Unlock()
	s := &session{
		p: p, handler: handler, id: opened.GetSessionId(), slots: make(chan struct{}, p.config.MaxConcurrentPartitions),
		paused: opened.GetState() == wire.ProcessorState_PROCESSOR_STATE_PAUSED, changed: make(chan struct{}),
	}
	p.logger.Info("session opened", "sessionId", s.id, "ownerEpoch", opened.GetOwnerEpoch(), "paused", s.paused)
	p.observe(Event{Kind: EventOwnershipChanged, Result: "opened"})
	s.update(func() {})

	err := s.run(ctx, partitions, opened.GetHeartbeatInterval().AsDuration())

	p.observe(Event{Kind: EventOwnershipChanged, Result: "closed"})
	if errors.Is(err, ErrorSessionFenced) {
		p.logger.Info("the session was fenced: opening a new one", "sessionId", s.id, "error", err)
	} else {
		p.closeSession(s.id)
	}
	return err
}

// run heartbeats and processes every partition in a loop of its own until the session is fenced,
// ctx ends or a failure that retrying cannot fix, and returns once every loop, and so every Handler
// call, has returned: nil when ctx ended, and otherwise the error that ended the session.
func (s *session) run(ctx context.Context, partitions int, interval time.Duration) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var ended error
	var once sync.Once
	s.end = func(err error) {
		once.Do(func() { ended = err })
		cancel()
	}
	var loops sync.WaitGroup
	loops.Go(func() { s.heartbeat(ctx, interval) })
	for number := range partitions {
		loops.Go(func() { (&partition{s: s, number: number, committed: -1}).run(ctx) })
	}
	loops.Wait()
	return ended
}

// update changes the state of the session, and with it the State of the Processor.
func (s *session) update(change func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	paused := s.paused
	change()
	if s.paused != paused {
		s.p.logger.Info("the processor state changed", "paused", s.paused)
	}
	close(s.changed)
	s.changed = make(chan struct{})
	switch {
	case s.paused:
		s.p.setState(StatePaused)
	case s.retrying > 0:
		s.p.setState(StateDegraded)
	default:
		s.p.setState(StateRunning)
	}
}

// awaitActive waits while the processor is paused, and reports whether ctx goes on.
func (s *session) awaitActive(ctx context.Context) bool {
	for ctx.Err() == nil {
		s.mu.Lock()
		paused, changed := s.paused, s.changed
		s.mu.Unlock()
		if !paused {
			return true
		}
		select {
		case <-changed:
		case <-ctx.Done():
		}
	}
	return false
}

// heartbeat heartbeats the session every interval until ctx ends, and records whether the
// processor is paused. A failure that retrying may fix waits for the next heartbeat; any other ends
// the session.
func (s *session) heartbeat(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		callCtx, cancel := context.WithTimeout(ctx, callTimeout)
		answer, err := s.p.client.Heartbeat(callCtx, &wire.HeartbeatRequest{SessionId: s.id})
		cancel()
		if err == nil {
			s.update(func() { s.paused = answer.GetState() == wire.ProcessorState_PROCESSOR_STATE_PAUSED })
			continue
		}
		if ctx.Err() != nil {
			return
		}
		e := rpcError("heartbeat", err)
		if e.Code != ErrorUnavailable && e.Code != ErrorInternal {
			s.end(e)
			return
		}
		s.p.recordError(e.Code)
		s.p.logger.Warn("the heartbeat failed: retrying at the next one", "error", e)
		s.p.observe(Event{Kind: EventRetry, Result: "heartbeat", Code: e.Code, Duration: interval})
	}
}

// partition is a partition in a session.
type partition struct {
	s      *session
	number int
	// committed is the offset that the session committed last, -1 before its first commit.
	committed int64
	// failures counts the failed attempts since the last success.
	failures int
}

// run reads the next page of the partition, which waits up to LongPollTimeout for new
// observations while the partition has none, calls the Handler for its observations and commits
// it, until ctx ends. A failure waits for its retry in this loop, while the other partitions go on.
func (pt *partition) run(ctx context.Context) {
	s := pt.s
	for s.awaitActive(ctx) {
		page, err := s.read(ctx, pt.number)
		if err != nil {
			pt.failed(ctx, "page", rpcError("read", err))
			continue
		}
		highWatermark, next := int64(page.GetHighWatermark()), int64(page.GetNextOffset())
		observations := page.GetObservations()
		// A page counts in the lag until it commits.
		through := next
		if len(observations) > 0 {
			through = int64(observations[0].GetOffset()) - 1
		}
		s.p.update(func(snapshot *Snapshot) {
			partition := &snapshot.Partitions[pt.number]
			partition.HighWatermark, partition.Lag, partition.LastReadAt = highWatermark, highWatermark-through, time.Now()
		})
		if len(observations) > 0 {
			if retryAfter, err := s.handle(ctx, pt.number, observations); err != nil {
				pt.retry(ctx, "page", err, retryAfter)
				continue
			}
		} else if next == pt.committed {
			pt.succeeded()
			continue
		}
		pt.commit(ctx, next, highWatermark)
	}
}

// commit commits offset, and commits it again after a failure that retrying may fix, until it
// succeeds, the session ends or ctx ends. highWatermark is that of the Read of the page.
func (pt *partition) commit(ctx context.Context, offset, highWatermark int64) {
	s := pt.s
	for s.awaitActive(ctx) {
		callCtx, cancel := context.WithTimeout(ctx, callTimeout)
		_, err := s.p.client.Commit(callCtx, &wire.CommitRequest{SessionId: s.id, Partition: uint32(pt.number), Offset: uint64(offset)})
		cancel()
		if err != nil {
			pt.failed(ctx, "commit", rpcError("commit", err))
			continue
		}
		s.p.logger.Debug("committed", "partition", pt.number, "offset", offset)
		pt.committed = offset
		pt.succeeded()
		s.p.update(func(snapshot *Snapshot) {
			now, partition := time.Now(), &snapshot.Partitions[pt.number]
			partition.Checkpoint, partition.Lag, partition.LastCommitAt = offset, highWatermark-offset, now
			snapshot.LastCommitAt = now
		})
		s.p.observe(Event{Kind: EventCheckpointCommitted, Partition: pt.number, Lag: s.p.Snapshot().Lag})
		return
	}
}

// failed handles a failed call of the Processor Runtime API: processor_paused pauses the session, a
// failure that retrying may fix waits for its retry, and any other, such as a fencing, ends the
// session.
func (pt *partition) failed(ctx context.Context, what string, e *Error) {
	switch {
	case ctx.Err() != nil:
	case e.Code == ErrorProcessorPaused:
		pt.s.update(func() { pt.s.paused = true })
	case e.Code == ErrorUnavailable || e.Code == ErrorInternal:
		pt.s.p.recordError(e.Code)
		pt.retry(ctx, what, e, 0)
	default:
		pt.s.end(e)
	}
}

// retry waits after the failure err of what, page or commit, before the partition tries again: the
// delay of the RetryPolicy, and no less than least.
func (pt *partition) retry(ctx context.Context, what string, err error, least time.Duration) {
	s := pt.s
	if ctx.Err() != nil {
		return
	}
	pt.failures++
	if pt.failures == 1 {
		s.update(func() { s.retrying++ })
	}
	delay := max(s.p.config.RetryPolicy.RetryDelay(pt.failures), least)
	var code ErrorCode
	var e *Error
	if errors.As(err, &e) {
		code = e.Code
	}
	s.p.logger.Warn("retrying the "+what+" of a partition", "partition", pt.number, "retryIn", delay, "error", err)
	s.p.observe(Event{Kind: EventRetry, Result: what, Code: code, Partition: pt.number, Duration: delay})
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

// succeeded ends the retries of the partition.
func (pt *partition) succeeded() {
	if pt.failures > 0 {
		pt.failures = 0
		pt.s.update(func() { pt.s.retrying-- })
	}
}

// handle calls the Handler for the observations of a page in offset order, holding one of the
// MaxConcurrentPartitions slots, and returns nil once they all resolved. Otherwise it returns the
// failure of the Handler or of the FailureSink, after which the page is read again no sooner than
// the returned delay, or an error when the session ends.
func (s *session) handle(ctx context.Context, partition int, observations []*wire.Observation) (time.Duration, error) {
	select {
	case s.slots <- struct{}{}:
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	s.p.update(func(snapshot *Snapshot) { snapshot.ActivePartitions++ })
	defer func() {
		s.p.update(func(snapshot *Snapshot) { snapshot.ActivePartitions-- })
		<-s.slots
	}()
	for _, w := range observations {
		if ctx.Err() != nil {
			// The session ends; the page stays uncommitted.
			return 0, ctx.Err()
		}
		o := observationFrom(w)
		started := time.Now()
		err := s.call(ctx, o)
		handled := Event{Kind: EventObservationHandled, Result: "success", Partition: partition, Duration: time.Since(started)}
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		if err == nil {
			s.p.observe(handled)
			continue
		}
		retryAfter, permanent := classify(err)
		if permanent == nil {
			handled.Result = "retry"
			s.p.observe(handled)
			return retryAfter, fmt.Errorf("the Handler failed at offset %d: %w", o.Offset, err)
		}
		handled.Result = "permanent"
		s.p.observe(handled)
		if s.p.config.FailureSink == nil {
			err := &Error{Code: ErrorInvalidConfig, Op: "handle",
				Err: fmt.Errorf("the Handler returned a Permanent error at offset %d without a FailureSink: %w", o.Offset, permanent)}
			s.end(err)
			return 0, err
		}
		if !s.p.config.IncludePayloadInFailures {
			o.Payload = nil
		}
		err = s.p.config.FailureSink.RecordFailure(ctx, FailureRecord{ProcessorID: s.p.config.ProcessorID, Observation: o, Err: permanent, FailedAt: time.Now()})
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		if err != nil {
			s.p.observe(Event{Kind: EventFailureSink, Result: "failed", Partition: partition})
			return 0, fmt.Errorf("the FailureSink failed at offset %d: %w", o.Offset, err)
		}
		s.p.observe(Event{Kind: EventFailureSink, Result: "recorded", Partition: partition})
		s.p.logger.Warn("recorded a permanent failure of the Handler", "partition", partition, "offset", o.Offset, "error", permanent)
	}
	return 0, nil
}

// call calls the Handler, whose context ends after HandlerTimeout; a panic is an error.
func (s *session) call(ctx context.Context, o Observation) (err error) {
	ctx, cancel := context.WithTimeout(ctx, s.p.config.HandlerTimeout)
	defer cancel()
	s.p.update(func(snapshot *Snapshot) { snapshot.ActiveHandlers++ })
	defer s.p.update(func(snapshot *Snapshot) { snapshot.ActiveHandlers-- })
	defer func() {
		if recovered := recover(); recovered != nil {
			s.p.logger.Error("the Handler panicked", "partition", o.Partition, "offset", o.Offset, "panic", recovered,
				"stack", string(debug.Stack()))
			err = fmt.Errorf("the Handler panicked: %v", recovered)
		}
	}()
	return s.handler.Handle(ctx, o)
}

// read reads the next page of a partition, waiting up to LongPollTimeout while it has nothing new.
func (s *session) read(ctx context.Context, partition int) (*wire.ReadResponse, error) {
	wait := s.p.config.LongPollTimeout
	ctx, cancel := context.WithTimeout(ctx, wait+callTimeout)
	defer cancel()
	return s.p.client.Read(ctx, &wire.ReadRequest{
		SessionId: s.id, Partition: uint32(partition), Limit: uint32(s.p.config.PageSize), Wait: durationpb.New(wait),
	})
}
