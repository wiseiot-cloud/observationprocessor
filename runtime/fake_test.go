package processor_test

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	wire "github.com/wiseiot-cloud/observationprocessor/internal/wire/processor"
	processor "github.com/wiseiot-cloud/observationprocessor/runtime"
)

const (
	processorID = "01a10409-44de-7774-8171-faa812406bc6"
	token       = "acs_test-token"
	// skipped is the claimed protocol of the observations that the fake's filter does not match.
	skipped = "acme.skipped:1"
)

// fake is an in-process ProcessorRuntimeService with the semantics of core: one session at a time
// owns the processor; Read returns the observations after the checkpoint of a partition within a
// window of 4×limit offsets, those of the protocol skipped excepted, and waits for new ones of the
// partition; Commit moves the checkpoint.
type fake struct {
	wire.UnimplementedProcessorRuntimeServiceServer
	address string

	mu sync.Mutex
	// fail returns the error that answers a call, or nil to serve it.
	fail func(call) error
	// changed closes, per partition, when the partition gets new observations or the session or
	// the state changes, to end the waits of Reads.
	changed  []chan struct{}
	state    wire.ProcessorState
	interval time.Duration
	// filter is the filter that OpenSession answers.
	filter      *wire.Filter
	partitions  [][]*wire.Observation
	checkpoints []uint64
	// session is the session that owns the processor, "" for none.
	session string
	epoch   uint64
	calls   []call
	// reading tells the partitions with a Read in progress; overlaps counts the Reads that started
	// while another Read of the same partition was in progress.
	reading  map[int]bool
	overlaps int
}

// call is a call that the fake received.
type call struct {
	rpc           string
	authorization string
	session       string
	instance      string
	partition     int
	limit         int
	wait          time.Duration
	offset        uint64
}

// newFake serves a fake with partitions partitions on a loopback listener.
func newFake(t *testing.T, partitions int) *fake {
	t.Helper()
	f := &fake{
		changed: make([]chan struct{}, partitions), state: wire.ProcessorState_PROCESSOR_STATE_ACTIVE, interval: 50 * time.Millisecond,
		filter:     &wire.Filter{ProtocolNames: []string{"acme.meter"}},
		partitions: make([][]*wire.Observation, partitions), checkpoints: make([]uint64, partitions), reading: map[int]bool{},
	}
	for partition := range f.changed {
		f.changed[partition] = make(chan struct{})
	}
	server := grpc.NewServer()
	wire.RegisterProcessorRuntimeServiceServer(server, f)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f.address = listener.Addr().String()
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return f
}

// newProcessor returns a Processor of the fake's processor with quick retries, changed by change.
func (f *fake) newProcessor(t *testing.T, change func(*processor.Config)) *processor.Processor {
	t.Helper()
	config := processor.Config{
		Endpoint: f.address, AllowInsecure: true, ProcessorID: processorID, Credential: processor.StaticToken(token),
		InstanceID: "instance-1", LongPollTimeout: time.Second, RetryPolicy: quickRetries{}, ShutdownTimeout: 10 * time.Second,
	}
	if change != nil {
		change(&config)
	}
	p, err := processor.New(config)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return p
}

type quickRetries struct{}

func (quickRetries) RetryDelay(int) time.Duration { return 10 * time.Millisecond }

// start runs p with handler and returns where Run's result arrives; p shuts down when the test ends.
func start(t *testing.T, p *processor.Processor, handler processor.Handler) <-chan error {
	t.Helper()
	result := make(chan error, 1)
	go func() { result <- p.Run(context.Background(), handler) }()
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return result
}

// result waits for the result of Run.
func result(t *testing.T, results <-chan error) error {
	t.Helper()
	select {
	case err := <-results:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return")
		return nil
	}
}

// eventually waits until condition holds.
func eventually(t *testing.T, what string, condition func() bool) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); !condition(); time.Sleep(5 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting until %s", what)
		}
	}
}

// add appends observations with the payloads to a partition, of the protocol acme.meter:1 or, for a
// payload "skip", of the protocol skipped.
func (f *fake) add(partition int, payloads ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, payload := range payloads {
		offset := len(f.partitions[partition]) + 1
		protocol := "acme.meter:1"
		if payload == "skip" {
			protocol = skipped
		}
		f.partitions[partition] = append(f.partitions[partition], &wire.Observation{
			ObservationId: fmt.Sprintf("observation-%d-%d", partition, offset), Partition: uint32(partition), Offset: uint64(offset),
			PrincipalDeviceId: "principal", SessionId: "device-session", ConnectionEpoch: 3, DeliveryId: "delivery",
			ClaimedProtocol: protocol, ProtocolStatus: wire.ProtocolStatus_PROTOCOL_STATUS_UNREGISTERED, ClaimedSourceKey: "main",
			ResolvedSourceDeviceId: "principal", ResolvedSourceKey: "main", Label: "site-a",
			Tags: map[string]string{"site": "north"}, ObservedAt: "2026-10-04T08:00:00+08:00",
			ReceivedAt: timestamppb.New(time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)), Payload: []byte(payload),
		})
	}
	f.notify(partition)
}

// notify ends the waits of the Reads of a partition; f.mu is held.
func (f *fake) notify(partition int) {
	close(f.changed[partition])
	f.changed[partition] = make(chan struct{})
}

// notifyAll ends the waits of every Read; f.mu is held.
func (f *fake) notifyAll() {
	for partition := range f.changed {
		f.notify(partition)
	}
}

// fence ends the ownership of the session, as a filter change, a pause or a reset does.
func (f *fake) fence() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.session = ""
	f.notifyAll()
}

func (f *fake) setState(state wire.ProcessorState) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.state = state
	f.notifyAll()
}

// changeFilter changes the filter and fences the session, as a filter change over REST does.
func (f *fake) changeFilter(filter *wire.Filter) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.filter = filter
	f.session = ""
	f.notifyAll()
}

func (f *fake) setFail(fail func(call) error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fail = fail
}

func (f *fake) checkpoint(partition int) uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.checkpoints[partition]
}

// committed reports whether every partition is committed through its last observation.
func (f *fake) committed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for partition, observations := range f.partitions {
		if f.checkpoints[partition] != uint64(len(observations)) {
			return false
		}
	}
	return true
}

// callsOf returns the calls of an RPC, in order.
func (f *fake) callsOf(rpc string) []call {
	f.mu.Lock()
	defer f.mu.Unlock()
	var calls []call
	for _, c := range f.calls {
		if c.rpc == rpc {
			calls = append(calls, c)
		}
	}
	return calls
}

// commitsOf returns the offsets that the Commits of a partition asked for, in order.
func (f *fake) commitsOf(partition int) []uint64 {
	var offsets []uint64
	for _, c := range f.callsOf("Commit") {
		if c.partition == partition {
			offsets = append(offsets, c.offset)
		}
	}
	return offsets
}

// receive records a call and returns the error that fail answers.
func (f *fake) receive(ctx context.Context, c call) error {
	md, _ := metadata.FromIncomingContext(ctx)
	c.authorization = strings.Join(md.Get("authorization"), ", ")
	f.mu.Lock()
	f.calls = append(f.calls, c)
	fail := f.fail
	f.mu.Unlock()
	if fail != nil {
		return fail(c)
	}
	return nil
}

// failure returns an error status with an ErrorDetail, as core answers.
func failure(c codes.Code, code string) error {
	answer, _ := status.New(c, code).WithDetails(&wire.ErrorDetail{Code: code})
	return answer.Err()
}

func (f *fake) OpenSession(ctx context.Context, request *wire.OpenSessionRequest) (*wire.OpenSessionResponse, error) {
	if err := f.receive(ctx, call{rpc: "OpenSession", instance: request.GetInstanceId()}); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if request.GetProcessorId() != processorID {
		return nil, failure(codes.NotFound, "not_found")
	}
	if f.session != "" {
		return nil, failure(codes.AlreadyExists, "owner_active")
	}
	f.session = uuid.NewString()
	f.epoch++
	return &wire.OpenSessionResponse{
		SessionId: f.session, OwnerEpoch: f.epoch, State: f.state, PartitionCount: uint32(len(f.partitions)),
		HeartbeatInterval: durationpb.New(f.interval), Filter: f.filter,
	}, nil
}

func (f *fake) Heartbeat(ctx context.Context, request *wire.HeartbeatRequest) (*wire.HeartbeatResponse, error) {
	if err := f.receive(ctx, call{rpc: "Heartbeat", session: request.GetSessionId()}); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if request.GetSessionId() != f.session {
		return nil, failure(codes.FailedPrecondition, "session_fenced")
	}
	return &wire.HeartbeatResponse{State: f.state}, nil
}

func (f *fake) Read(ctx context.Context, request *wire.ReadRequest) (*wire.ReadResponse, error) {
	partition, limit, wait := int(request.GetPartition()), uint64(request.GetLimit()), request.GetWait().AsDuration()
	if err := f.receive(ctx, call{rpc: "Read", session: request.GetSessionId(), partition: partition, limit: int(limit), wait: wait}); err != nil {
		return nil, err
	}
	f.mu.Lock()
	if f.reading[partition] {
		f.overlaps++
	}
	f.reading[partition] = true
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		delete(f.reading, partition)
		f.mu.Unlock()
	}()
	for waited := false; ; waited = true {
		f.mu.Lock()
		if request.GetSessionId() != f.session {
			f.mu.Unlock()
			return nil, failure(codes.FailedPrecondition, "session_fenced")
		}
		if f.state == wire.ProcessorState_PROCESSOR_STATE_PAUSED {
			f.mu.Unlock()
			return nil, failure(codes.FailedPrecondition, "processor_paused")
		}
		checkpoint, observations := f.checkpoints[partition], f.partitions[partition]
		highWatermark := uint64(len(observations))
		if checkpoint == highWatermark && !waited && wait > 0 {
			changed := f.changed[partition]
			f.mu.Unlock()
			select {
			case <-changed:
			case <-time.After(wait):
			case <-ctx.Done():
				return nil, status.FromContextError(ctx.Err()).Err()
			}
			continue
		}
		page := &wire.ReadResponse{NextOffset: min(checkpoint+4*limit, highWatermark), HighWatermark: highWatermark}
		for offset := checkpoint + 1; offset <= page.NextOffset; offset++ {
			if o := observations[offset-1]; o.GetClaimedProtocol() != skipped {
				page.Observations = append(page.Observations, o)
				if uint64(len(page.Observations)) == limit {
					page.NextOffset = offset
					break
				}
			}
		}
		f.mu.Unlock()
		return page, nil
	}
}

func (f *fake) Commit(ctx context.Context, request *wire.CommitRequest) (*wire.CommitResponse, error) {
	partition := int(request.GetPartition())
	if err := f.receive(ctx, call{rpc: "Commit", session: request.GetSessionId(), partition: partition, offset: request.GetOffset()}); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	switch {
	case request.GetSessionId() != f.session:
		return nil, failure(codes.FailedPrecondition, "session_fenced")
	case f.state == wire.ProcessorState_PROCESSOR_STATE_PAUSED:
		return nil, failure(codes.FailedPrecondition, "processor_paused")
	case request.GetOffset() < f.checkpoints[partition] || request.GetOffset() > uint64(len(f.partitions[partition])):
		return nil, failure(codes.OutOfRange, "offset_out_of_range")
	}
	f.checkpoints[partition] = request.GetOffset()
	return &wire.CommitResponse{}, nil
}

func (f *fake) CloseSession(ctx context.Context, request *wire.CloseSessionRequest) (*wire.CloseSessionResponse, error) {
	if err := f.receive(ctx, call{rpc: "CloseSession", session: request.GetSessionId()}); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if request.GetSessionId() == f.session {
		f.session = ""
		f.notifyAll()
	}
	return &wire.CloseSessionResponse{}, nil
}
