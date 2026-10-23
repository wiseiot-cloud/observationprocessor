package processor

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	wire "github.com/wiseiot-cloud/observationprocessor/internal/wire/processor"
)

const (
	// callTimeout bounds a call of the Processor Runtime API, and a Read beyond its wait.
	callTimeout = 10 * time.Second
	// closeTimeout bounds the CloseSession of a session that ends.
	closeTimeout = 2 * time.Second
)

// Processor runs an Observation Processor: it owns the processor through a session of the Processor
// Runtime API and calls a Handler for the observations that match the processor's filter.
type Processor struct {
	config Config
	conn   *grpc.ClientConn
	client wire.ProcessorRuntimeServiceClient
	logger *slog.Logger

	mu       sync.Mutex
	started  bool
	shutdown bool
	// stop ends the context of Run.
	stop context.CancelFunc
	// done closes when Run returns.
	done chan struct{}
	// snapshot is the state of the Processor; its Lag stays 0, as Snapshot sums it.
	snapshot Snapshot
}

// New returns a Processor for config, or an *Error with ErrorInvalidConfig. It does not connect
// yet.
func New(config Config) (*Processor, error) {
	config, err := config.withDefaults()
	if err != nil {
		return nil, &Error{Code: ErrorInvalidConfig, Op: "new", Err: err}
	}
	transport := insecure.NewCredentials()
	if !config.AllowInsecure {
		transport = credentials.NewTLS(config.TLSConfig)
	}
	conn, err := grpc.NewClient(config.Endpoint, grpc.WithTransportCredentials(transport),
		grpc.WithPerRPCCredentials(bearer{provider: config.Credential, secure: !config.AllowInsecure}))
	if err != nil {
		return nil, &Error{Code: ErrorInvalidConfig, Op: "new", Err: err}
	}
	return &Processor{
		config:   config,
		conn:     conn,
		client:   wire.NewProcessorRuntimeServiceClient(conn),
		logger:   config.Logger.With("processorId", config.ProcessorID, "instanceId", config.InstanceID),
		done:     make(chan struct{}),
		snapshot: Snapshot{State: StateStarting, ProcessorID: config.ProcessorID, InstanceID: config.InstanceID},
	}, nil
}

// Run calls handler for the observations of the processor until Shutdown, the end of ctx or a
// failure that retrying cannot fix. It opens a session, which owns the processor and all its
// partitions, and opens a new one whenever the session is fenced; while another instance owns the
// processor, it waits as a standby. Run returns once every Handler call has returned: nil after
// Shutdown, ctx.Err() when ctx ends, and otherwise an *Error such as one with ErrorDataGap. Run may
// be called once.
func (p *Processor) Run(ctx context.Context, handler Handler) error {
	p.mu.Lock()
	if p.started {
		p.mu.Unlock()
		return &Error{Code: ErrorInvalidConfig, Op: "run", Err: errors.New("Run was called before")}
	}
	p.started = true
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	p.stop = stop
	shutdown := p.shutdown
	p.mu.Unlock()
	stopping := context.AfterFunc(runCtx, func() { p.setState(StateStopping) })
	defer stopping()

	var err error
	if !shutdown {
		err = p.run(runCtx, handler)
	}
	p.conn.Close()
	p.mu.Lock()
	shutdown = p.shutdown
	p.mu.Unlock()
	p.setState(StateStopped)
	close(p.done)
	switch {
	case err != nil:
		p.logger.Error("the processor stopped", "error", err)
		return err
	case shutdown:
		p.logger.Info("the processor stopped")
		return nil
	}
	p.logger.Info("the processor stopped", "error", ctx.Err())
	return ctx.Err()
}

// Shutdown stops Run: no page starts any more, the contexts of the Handler calls in progress end,
// and once these calls have returned, the session closes, so that another instance may take the
// processor over at once. Shutdown returns nil when Run has returned, or the error of ctx or of
// ShutdownTimeout, whichever comes first; Run keeps waiting for its Handler calls in any case.
func (p *Processor) Shutdown(ctx context.Context) error {
	p.mu.Lock()
	p.shutdown = true
	started, stop := p.started, p.stop
	p.mu.Unlock()
	if !started {
		p.conn.Close()
		p.setState(StateStopped)
		return nil
	}
	stop()
	ctx, cancel := context.WithTimeout(ctx, p.config.ShutdownTimeout)
	defer cancel()
	select {
	case <-p.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Snapshot returns the state of the Processor. It is safe for concurrent use.
func (p *Processor) Snapshot() Snapshot {
	p.mu.Lock()
	defer p.mu.Unlock()
	snapshot := p.snapshot
	snapshot.Partitions = slices.Clone(p.snapshot.Partitions)
	for _, partition := range snapshot.Partitions {
		snapshot.Lag += partition.Lag
	}
	return snapshot
}

// run opens and serves sessions until ctx ends, which returns nil, or a failure that retrying
// cannot fix.
func (p *Processor) run(ctx context.Context, handler Handler) error {
	for attempt := 1; ; attempt++ {
		opened, err := p.open(ctx)
		if err == nil {
			attempt = 0
			if err := p.serve(ctx, opened, handler); !errors.Is(err, ErrorSessionFenced) {
				return err
			}
			continue
		}
		if ctx.Err() != nil {
			return nil
		}
		e := rpcError("open_session", err)
		p.recordError(e.Code)
		delay := p.config.RetryPolicy.RetryDelay(attempt)
		switch e.Code {
		case ErrorOwnerActive:
			p.setState(StateStarting)
			level := slog.LevelDebug
			if attempt == 1 {
				level = slog.LevelInfo
			}
			p.logger.Log(ctx, level, "another instance owns the processor: waiting as a standby", "retryIn", delay)
		case ErrorUnavailable, ErrorInternal:
			p.setState(StateDegraded)
			p.logger.Warn("OpenSession failed: retrying", "retryIn", delay, "error", e)
		default:
			return e
		}
		p.observe(Event{Kind: EventRetry, Result: "session", Code: e.Code, Duration: delay})
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}

func (p *Processor) open(ctx context.Context) (*wire.OpenSessionResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	return p.client.OpenSession(ctx, &wire.OpenSessionRequest{ProcessorId: p.config.ProcessorID, InstanceId: p.config.InstanceID})
}

// closeSession closes a session as well as it can, so that another may open at once; otherwise the
// session expires by itself.
func (p *Processor) closeSession(id string) {
	ctx, cancel := context.WithTimeout(context.Background(), closeTimeout)
	defer cancel()
	if _, err := p.client.CloseSession(ctx, &wire.CloseSessionRequest{SessionId: id}); err != nil {
		p.logger.Warn("CloseSession failed: the session expires by itself", "sessionId", id, "error", err)
		return
	}
	p.logger.Info("session closed", "sessionId", id)
}

// setState sets the State and reports a change. Once stopping, the State only becomes stopped.
func (p *Processor) setState(state State) {
	p.mu.Lock()
	previous := p.snapshot.State
	if previous == state || previous == StateStopped || previous == StateStopping && state != StateStopped {
		p.mu.Unlock()
		return
	}
	p.snapshot.State = state
	p.mu.Unlock()
	p.observe(Event{Kind: EventStateChanged, Result: string(state)})
}

// update changes the snapshot.
func (p *Processor) update(change func(snapshot *Snapshot)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	change(&p.snapshot)
}

func (p *Processor) recordError(code ErrorCode) {
	p.update(func(snapshot *Snapshot) { snapshot.LastErrorCode, snapshot.LastErrorAt = code, time.Now() })
}

func (p *Processor) observe(event Event) {
	if p.config.Observer != nil {
		p.config.Observer.Observe(event)
	}
}
