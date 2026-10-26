package processor_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	wire "github.com/wiseiot-cloud/observationprocessor/internal/wire/processor"
	processor "github.com/wiseiot-cloud/observationprocessor/runtime"
	"github.com/wiseiot-cloud/observationprocessor/testkit"
)

// offsets records the offsets that a Handler handled, by partition.
type offsets struct {
	mu      sync.Mutex
	handled map[int][]int64
}

func (o *offsets) add(observation processor.Observation) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.handled == nil {
		o.handled = map[int][]int64{}
	}
	o.handled[observation.Partition] = append(o.handled[observation.Partition], observation.Offset)
}

func (o *offsets) of(partition int) []int64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	return slices.Clone(o.handled[partition])
}

// recording returns a Handler that records the offsets it handles.
func recording(handled *offsets) processor.Handler {
	return processor.HandlerFunc(func(_ context.Context, o processor.Observation) error {
		handled.add(o)
		return nil
	})
}

func TestPagesCommitOnceEveryObservationResolved(t *testing.T) {
	t.Parallel()
	f := newFake(t, 2)
	f.add(0, "1", "2", "3", "4", "5")
	f.add(1, "6", "7", "8")
	p := f.newProcessor(t, func(c *processor.Config) { c.PageSize = 2 })
	var handled offsets
	var first atomic.Pointer[processor.Observation]
	results := start(t, p, processor.HandlerFunc(func(_ context.Context, o processor.Observation) error {
		// Pages of 2 observations: the checkpoint stays before the page until all of it resolved.
		if checkpoint, want := f.checkpoint(o.Partition), uint64(o.Offset-1)/2*2; checkpoint != want {
			t.Errorf("checkpoint of partition %d = %d while handling offset %d, want %d", o.Partition, checkpoint, o.Offset, want)
		}
		if o.Partition == 0 && o.Offset == 1 {
			first.Store(&o)
		}
		handled.add(o)
		return nil
	}))
	// The fake records a commit before the processor learns of it and updates its lag.
	eventually(t, "every partition is committed without lag", func() bool { return f.committed() && p.Snapshot().Lag == 0 })
	if got := handled.of(0); !slices.Equal(got, []int64{1, 2, 3, 4, 5}) {
		t.Errorf("handled offsets of partition 0 = %v", got)
	}
	if got := handled.of(1); !slices.Equal(got, []int64{1, 2, 3}) {
		t.Errorf("handled offsets of partition 1 = %v", got)
	}
	if got, got1 := f.commitsOf(0), f.commitsOf(1); !slices.Equal(got, []uint64{2, 4, 5}) || !slices.Equal(got1, []uint64{2, 3}) {
		t.Errorf("commits = %v and %v, want [2 4 5] and [2 3]", got, got1)
	}
	want := processor.Observation{
		ID: "observation-0-1", Partition: 0, Offset: 1, PrincipalDeviceID: "principal", SessionID: "device-session",
		ConnectionEpoch: 3, DeliveryID: "delivery", ClaimedProtocol: "acme.meter:1", ProtocolStatus: processor.ProtocolUnregistered,
		ClaimedSourceKey: "main", ResolvedSourceDeviceID: "principal", ResolvedSourceKey: "main", Label: "site-a",
		Tags:       map[string]string{"site": "north"},
		ObservedAt: "2026-10-04T08:00:00+08:00", ReceivedAt: time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC), Payload: []byte("1"),
	}
	if got := first.Load(); got == nil || !got.ReceivedAt.Equal(want.ReceivedAt) ||
		!reflect.DeepEqual(func() processor.Observation { o := *got; o.ReceivedAt = want.ReceivedAt; return o }(), want) {
		t.Errorf("observation = %+v, want %+v", got, want)
	}
	for _, read := range f.callsOf("Read") {
		if read.limit != 2 || read.wait != time.Second {
			t.Errorf("Read of %d observations, waiting %v; want 2 and the LongPollTimeout of 1s", read.limit, read.wait)
		}
	}
	if s := p.Snapshot(); s.State != processor.StateRunning || s.OwnerEpoch != 1 || s.Lag != 0 || s.LastCommitAt.IsZero() ||
		s.ProcessorID != processorID || s.InstanceID != "instance-1" {
		t.Errorf("snapshot = %+v", s)
	}
	if err := p.Shutdown(context.Background()); err != nil {
		t.Errorf("Shutdown: %v", err)
	}
	if err := result(t, results); err != nil {
		t.Errorf("Run after Shutdown = %v", err)
	}
	if closed := f.callsOf("CloseSession"); len(closed) != 1 || p.Snapshot().State != processor.StateStopped {
		t.Errorf("CloseSession calls %v, state %s", closed, p.Snapshot().State)
	}
}

func TestPagesWithoutMatchesAdvanceTheCheckpoint(t *testing.T) {
	t.Parallel()
	f := newFake(t, 2)
	// With pages of 1 observation, Read scans windows of 4 offsets: (0, 4] matches nothing.
	f.add(0, "skip", "skip", "skip", "skip", "skip", "6")
	p := f.newProcessor(t, func(c *processor.Config) { c.PageSize = 1 })
	var handled offsets
	start(t, p, recording(&handled))
	eventually(t, "every partition is committed", f.committed)
	if got := handled.of(0); !slices.Equal(got, []int64{6}) {
		t.Errorf("handled offsets = %v, want [6]", got)
	}
	// An empty page commits when it moves the checkpoint, and an idle partition commits once in a
	// session.
	reads := len(f.callsOf("Read"))
	eventually(t, "the idle partitions are read again", func() bool { return len(f.callsOf("Read")) >= reads+3 })
	if got, got1 := f.commitsOf(0), f.commitsOf(1); !slices.Equal(got, []uint64{4, 6}) || !slices.Equal(got1, []uint64{0}) {
		t.Errorf("commits = %v and %v, want [4 6] and [0]", got, got1)
	}
}

func TestHandlerFailuresReadThePageAgain(t *testing.T) {
	t.Parallel()
	f := newFake(t, 1)
	f.add(0, "1", "2", "3")
	p := f.newProcessor(t, nil)
	var handled offsets
	var failures atomic.Int32
	var failedAt, retriedAt atomic.Int64
	start(t, p, processor.HandlerFunc(func(_ context.Context, o processor.Observation) error {
		handled.add(o)
		if o.Offset == 1 && failures.Load() == 2 && retriedAt.Load() == 0 {
			retriedAt.Store(time.Now().UnixNano())
		}
		if o.Offset != 2 {
			return nil
		}
		switch failures.Add(1) {
		case 1:
			return errors.New("not yet")
		case 2:
			failedAt.Store(time.Now().UnixNano())
			return processor.RetryAfter(errors.New("later"), 300*time.Millisecond)
		case 3:
			panic("broken")
		}
		return nil
	}))
	eventually(t, "the partition is committed", f.committed)
	if got := handled.of(0); !slices.Equal(got, []int64{1, 2, 1, 2, 1, 2, 1, 2, 3}) {
		t.Errorf("handled offsets = %v, want the page from its start after every failure", got)
	}
	if got := f.commitsOf(0); !slices.Equal(got, []uint64{3}) {
		t.Errorf("commits = %v, want [3] only", got)
	}
	if delay := time.Duration(retriedAt.Load() - failedAt.Load()); delay < 290*time.Millisecond {
		t.Errorf("the page was read again %v after RetryAfter 300ms", delay)
	}
}

func TestLagCountsAPageUntilItCommits(t *testing.T) {
	t.Parallel()
	f := newFake(t, 1)
	f.add(0, "1", "2", "3")
	var failing atomic.Bool
	failing.Store(true)
	p := f.newProcessor(t, nil)
	start(t, p, processor.HandlerFunc(func(context.Context, processor.Observation) error {
		if failing.Load() {
			return errors.New("not yet")
		}
		return nil
	}))
	eventually(t, "the page fails", func() bool { return p.Snapshot().State == processor.StateDegraded })
	if lag := p.Snapshot().Lag; lag != 3 {
		t.Errorf("lag while the page of 3 observations fails = %d, want 3", lag)
	}
	// Observations that arrive meanwhile count as well.
	f.add(0, "4", "5")
	eventually(t, "the lag counts the new observations", func() bool { return p.Snapshot().Lag == 5 })
	failing.Store(false)
	eventually(t, "the partition is committed", f.committed)
	eventually(t, "the lag drops to 0", func() bool { return p.Snapshot().Lag == 0 })
	if s := p.Snapshot(); s.State != processor.StateRunning || !slices.Equal(f.commitsOf(0), []uint64{5}) {
		t.Errorf("snapshot %+v after commits %v", s, f.commitsOf(0))
	}
}

func TestSnapshotReportsTheFilterAndThePartitions(t *testing.T) {
	t.Parallel()
	f := newFake(t, 2)
	f.add(0, "1", "2", "3")
	var failing, readsFail atomic.Bool
	failing.Store(true)
	f.setFail(func(c call) error {
		if c.rpc == "Read" && readsFail.Load() {
			return status.Error(codes.Unavailable, "connection reset")
		}
		return nil
	})
	p := f.newProcessor(t, nil)
	if s := p.Snapshot(); !reflect.DeepEqual(s.Filter, processor.Filter{}) || s.Partitions != nil {
		t.Errorf("snapshot before Run = %+v", s)
	}
	started := time.Now()
	start(t, p, processor.HandlerFunc(func(context.Context, processor.Observation) error {
		if failing.Load() {
			return errors.New("not yet")
		}
		return nil
	}))
	// Partition 0 retries its page of 3 observations; the empty partition 1 commits offset 0.
	eventually(t, "the page fails and partition 1 commits", func() bool {
		s := p.Snapshot()
		return s.State == processor.StateDegraded && len(s.Partitions) == 2 && !s.Partitions[1].LastCommitAt.IsZero()
	})
	s := p.Snapshot()
	if want := (processor.Filter{ClaimedProtocols: []string{"acme.meter:1"}}); !reflect.DeepEqual(s.Filter, want) {
		t.Errorf("filter = %+v, want %+v", s.Filter, want)
	}
	if got := s.Partitions[0]; got.Partition != 0 || got.HighWatermark != 3 || got.Lag != 3 || got.Checkpoint != 0 ||
		got.LastReadAt.Before(started) || !got.LastCommitAt.IsZero() {
		t.Errorf("partition 0 while its page fails = %+v", got)
	}
	if got := s.Partitions[1]; got.Partition != 1 || got.HighWatermark != 0 || got.Lag != 0 || got.Checkpoint != 0 ||
		got.LastReadAt.Before(started) || got.LastCommitAt.Before(started) {
		t.Errorf("partition 1 = %+v", got)
	}
	if s.Lag != 3 {
		t.Errorf("lag = %d, want the sum 3", s.Lag)
	}
	// The snapshot is a copy.
	s.Partitions[0].Lag = 100
	if p.Snapshot().Partitions[0].Lag == 100 {
		t.Error("changing a snapshot changed the processor")
	}

	failing.Store(false)
	eventually(t, "partition 0 commits", func() bool { return p.Snapshot().Partitions[0].Checkpoint == 3 })
	if s := p.Snapshot(); s.Lag != 0 || s.Partitions[0].Lag != 0 || s.Partitions[0].HighWatermark != 3 ||
		s.Partitions[0].LastCommitAt.IsZero() || !s.LastCommitAt.Equal(s.Partitions[0].LastCommitAt) {
		t.Errorf("snapshot after the commit = %+v", s)
	}

	// A new session answers the new filter, and its partitions start over.
	readsFail.Store(true)
	f.changeFilter(&wire.Filter{
		ClaimedProtocols: []string{"acme.meter:1"}, PrincipalDeviceIds: []string{"principal"},
		GatewayGroupIds: []string{"group"}, ResolvedSourceDeviceIds: []string{"source"},
		ProtocolStatuses: []wire.ProtocolStatus{wire.ProtocolStatus_PROTOCOL_STATUS_MATCHED, wire.ProtocolStatus_PROTOCOL_STATUS_MISMATCH},
		Labels:           []string{"site-a"},
	})
	eventually(t, "a new session retries its Reads", func() bool {
		s := p.Snapshot()
		return s.OwnerEpoch == 2 && s.State == processor.StateDegraded
	})
	s = p.Snapshot()
	want := processor.Filter{
		ClaimedProtocols: []string{"acme.meter:1"}, PrincipalDeviceIDs: []string{"principal"},
		GatewayGroupIDs: []string{"group"}, ResolvedSourceDeviceIDs: []string{"source"},
		ProtocolStatuses: []processor.ProtocolStatus{processor.ProtocolMatched, processor.ProtocolMismatch},
		Labels:           []string{"site-a"},
	}
	if !reflect.DeepEqual(s.Filter, want) {
		t.Errorf("filter of the new session = %+v, want %+v", s.Filter, want)
	}
	if fresh := []processor.PartitionSnapshot{{Partition: 0}, {Partition: 1}}; !reflect.DeepEqual(s.Partitions, fresh) || s.Lag != 0 {
		t.Errorf("partitions of the new session before a Read = %+v, lag %d", s.Partitions, s.Lag)
	}
	readsFail.Store(false)
	eventually(t, "the new session commits partition 0 again", func() bool {
		s := p.Snapshot()
		return s.Partitions[0].Checkpoint == 3 && s.Partitions[0].HighWatermark == 3 && !s.Partitions[0].LastCommitAt.IsZero()
	})
}

func TestPermanentFailures(t *testing.T) {
	t.Parallel()
	cause := errors.New("bad payload")
	permanentAt2 := func(handled *offsets) processor.Handler {
		return processor.HandlerFunc(func(_ context.Context, o processor.Observation) error {
			handled.add(o)
			if o.Offset == 2 {
				return processor.Permanent(cause)
			}
			return nil
		})
	}
	for _, includePayload := range []bool{false, true} {
		f := newFake(t, 1)
		f.add(0, "1", "2", "3")
		sink := &testkit.MemoryFailureSink{}
		p := f.newProcessor(t, func(c *processor.Config) {
			c.FailureSink, c.RequireFailureSink, c.IncludePayloadInFailures = sink, true, includePayload
		})
		var handled offsets
		start(t, p, permanentAt2(&handled))
		eventually(t, "the partition is committed", f.committed)
		records := sink.Records()
		if len(records) != 1 || records[0].ProcessorID != processorID || records[0].Observation.Offset != 2 ||
			!errors.Is(records[0].Err, cause) || records[0].FailedAt.IsZero() ||
			(string(records[0].Observation.Payload) == "2") != includePayload {
			t.Errorf("records with the payload %t = %+v", includePayload, records)
		}
		if got := handled.of(0); !slices.Equal(got, []int64{1, 2, 3}) || !slices.Equal(f.commitsOf(0), []uint64{3}) {
			t.Errorf("handled offsets = %v, commits %v", got, f.commitsOf(0))
		}
	}

	// A failure of the FailureSink reads the page again.
	f := newFake(t, 1)
	f.add(0, "1", "2", "3")
	var records atomic.Int32
	p := f.newProcessor(t, func(c *processor.Config) {
		c.FailureSink = processor.FailureSinkFunc(func(context.Context, processor.FailureRecord) error {
			if records.Add(1) == 1 {
				return errors.New("sink unavailable")
			}
			return nil
		})
	})
	var handled offsets
	start(t, p, permanentAt2(&handled))
	eventually(t, "the partition is committed", f.committed)
	if got := handled.of(0); !slices.Equal(got, []int64{1, 2, 1, 2, 3}) || records.Load() != 2 || !slices.Equal(f.commitsOf(0), []uint64{3}) {
		t.Errorf("handled offsets = %v after %d records, commits %v", got, records.Load(), f.commitsOf(0))
	}

	// Without a FailureSink, Run ends.
	f = newFake(t, 1)
	f.add(0, "1", "2", "3")
	p = f.newProcessor(t, nil)
	err := result(t, start(t, p, permanentAt2(&offsets{})))
	var e *processor.Error
	if !errors.Is(err, processor.ErrorInvalidConfig) || !errors.As(err, &e) || e.Op != "handle" || !errors.Is(err, cause) {
		t.Errorf("Run with a Permanent error and no FailureSink = %v", err)
	}
	if commits, closed := f.callsOf("Commit"), f.callsOf("CloseSession"); len(commits) != 0 || len(closed) != 1 {
		t.Errorf("commits %v and closes %v, want none and one", commits, closed)
	}
}

func TestFencedSessionsReopenAtOnce(t *testing.T) {
	t.Parallel()
	for _, rpc := range []string{"Heartbeat", "Read", "Commit"} {
		t.Run(rpc, func(t *testing.T) {
			t.Parallel()
			f := newFake(t, 1)
			f.add(0, "1", "2")
			var fenced atomic.Bool
			f.setFail(func(c call) error {
				// A heartbeat fences the session once the page is committed, Read and Commit at once.
				if c.rpc == rpc && (rpc != "Heartbeat" || f.committed()) && fenced.CompareAndSwap(false, true) {
					f.fence()
					return failure(codes.FailedPrecondition, "session_fenced")
				}
				return nil
			})
			p := f.newProcessor(t, nil)
			var handled offsets
			start(t, p, recording(&handled))
			// The fake records the second OpenSession before the processor learns its owner epoch.
			eventually(t, "a new session with owner epoch 2 is committed", func() bool {
				return len(f.callsOf("OpenSession")) == 2 && f.committed() && p.Snapshot().OwnerEpoch == 2
			})
			if closed := f.callsOf("CloseSession"); len(closed) != 0 {
				t.Errorf("the fenced session closed: %v", closed)
			}
			want := []int64{1, 2}
			if rpc == "Commit" {
				// The page whose commit was fenced is read again in the new session.
				want = []int64{1, 2, 1, 2}
			}
			if got := handled.of(0); !slices.Equal(got, want) {
				t.Errorf("handled offsets = %v, want %v", got, want)
			}
		})
	}
}

func TestPausedProcessorsOnlyHeartbeat(t *testing.T) {
	t.Parallel()
	// A session that opens paused reads once a heartbeat tells that the processor is active.
	f := newFake(t, 1)
	f.setState(wire.ProcessorState_PROCESSOR_STATE_PAUSED)
	f.add(0, "1")
	p := f.newProcessor(t, nil)
	var handled offsets
	start(t, p, recording(&handled))
	eventually(t, "two heartbeats", func() bool { return len(f.callsOf("Heartbeat")) >= 2 })
	if reads, s := f.callsOf("Read"), p.Snapshot(); len(reads) != 0 || s.State != processor.StatePaused {
		t.Errorf("reads %v in the state %s while paused", reads, s.State)
	}
	f.setState(wire.ProcessorState_PROCESSOR_STATE_ACTIVE)
	eventually(t, "the partition is committed", f.committed)
	if got, s := handled.of(0), p.Snapshot(); !slices.Equal(got, []int64{1}) || s.State != processor.StateRunning {
		t.Errorf("handled offsets %v in the state %s after the resume", got, s.State)
	}

	// processor_paused answering a Read stops the pages until a heartbeat tells that the processor
	// is active.
	f = newFake(t, 1)
	f.mu.Lock()
	f.interval = time.Second
	f.mu.Unlock()
	f.add(0, "1")
	p = f.newProcessor(t, nil)
	handled = offsets{}
	start(t, p, recording(&handled))
	eventually(t, "the partition is committed", f.committed)
	f.setState(wire.ProcessorState_PROCESSOR_STATE_PAUSED) // ends the waiting Read with processor_paused
	eventually(t, "the processor is paused", func() bool { return p.Snapshot().State == processor.StatePaused })
	f.add(0, "2")
	reads := len(f.callsOf("Read"))
	time.Sleep(300 * time.Millisecond)
	if got := len(f.callsOf("Read")); got != reads || len(handled.of(0)) != 1 {
		t.Errorf("%d reads and handled offsets %v while paused", got-reads, handled.of(0))
	}
	f.setState(wire.ProcessorState_PROCESSOR_STATE_ACTIVE)
	eventually(t, "the partition is committed", f.committed)
	if got := handled.of(0); !slices.Equal(got, []int64{1, 2}) {
		t.Errorf("handled offsets = %v", got)
	}
}

func TestStandbysWaitUntilTheOwnerLeaves(t *testing.T) {
	t.Parallel()
	f := newFake(t, 1)
	f.add(0, "1")
	f.mu.Lock()
	f.session = "another-instance"
	f.mu.Unlock()
	p := f.newProcessor(t, nil)
	var handled offsets
	start(t, p, recording(&handled))
	eventually(t, "OpenSession is retried", func() bool { return len(f.callsOf("OpenSession")) >= 3 })
	if s := p.Snapshot(); s.State != processor.StateStarting || s.LastErrorCode != processor.ErrorOwnerActive ||
		s.LastErrorAt.IsZero() || len(f.callsOf("Read")) != 0 {
		t.Errorf("standby snapshot = %+v", s)
	}
	f.fence() // the owner leaves
	eventually(t, "the partition is committed", f.committed)
	if s := p.Snapshot(); s.State != processor.StateRunning || s.OwnerEpoch != 1 || !slices.Equal(handled.of(0), []int64{1}) {
		t.Errorf("snapshot after the takeover = %+v", s)
	}
}

func TestDataGapsEndRun(t *testing.T) {
	t.Parallel()
	f := newFake(t, 2)
	f.add(0, "1")
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	f.setFail(func(c call) error {
		if c.rpc == "Read" && c.partition == 1 {
			<-started
			gap, _ := status.New(codes.FailedPrecondition, "data gap").WithDetails(&wire.ErrorDetail{
				Code: "data_gap", DataGap: &wire.DataGap{Partition: 1, Checkpoint: 4, HotFloor: 9},
			})
			return gap.Err()
		}
		return nil
	})
	p := f.newProcessor(t, func(c *processor.Config) { c.MaxConcurrentPartitions = 2 })
	var returned atomic.Bool
	results := start(t, p, processor.HandlerFunc(func(context.Context, processor.Observation) error {
		once.Do(func() { close(started) })
		<-release // ignores the end of its context
		returned.Store(true)
		return nil
	}))
	<-started
	time.Sleep(200 * time.Millisecond)
	select {
	case err := <-results:
		t.Fatalf("Run returned %v while a Handler call was in progress", err)
	default:
	}
	close(release)
	err := result(t, results)
	var e *processor.Error
	if !errors.Is(err, processor.ErrorDataGap) || !errors.As(err, &e) || e.Op != "read" || e.Partition != 1 ||
		e.Checkpoint != 4 || e.HotFloor != 9 || !returned.Load() {
		t.Errorf("Run = %v (%+v)", err, e)
	}
	if !strings.Contains(err.Error(), "partition 1") || !strings.Contains(err.Error(), "hot floor 9") {
		t.Errorf("error text = %q", err.Error())
	}
	if commits, closed := f.callsOf("Commit"), f.callsOf("CloseSession"); len(commits) != 0 || len(closed) != 1 {
		t.Errorf("commits %v and closes %v, want none and one", commits, closed)
	}
}

func TestUnauthenticatedEndsRun(t *testing.T) {
	t.Parallel()
	f := newFake(t, 1)
	f.setFail(func(c call) error {
		if c.authorization != "Bearer "+token {
			return failure(codes.Unauthenticated, "unauthenticated")
		}
		return nil
	})
	p := f.newProcessor(t, func(c *processor.Config) { c.Credential = processor.StaticToken("acs_wrong") })
	err := result(t, start(t, p, recording(&offsets{})))
	var e *processor.Error
	if !errors.Is(err, processor.ErrorUnauthenticated) || !errors.As(err, &e) || e.Op != "open_session" ||
		len(f.callsOf("OpenSession")) != 1 {
		t.Errorf("Run with a wrong token = %v after %d calls", err, len(f.callsOf("OpenSession")))
	}

	// So does a CredentialProvider that fails, before the call.
	p = f.newProcessor(t, func(c *processor.Config) {
		c.Credential = processor.CredentialProviderFunc(func(context.Context) (string, error) { return "", errors.New("vault sealed") })
	})
	err = result(t, start(t, p, recording(&offsets{})))
	if !errors.Is(err, processor.ErrorUnauthenticated) || !strings.Contains(err.Error(), "vault sealed") || len(f.callsOf("OpenSession")) != 1 {
		t.Errorf("Run with a failing CredentialProvider = %v", err)
	}
}

func TestPermissionDeniedEndsRun(t *testing.T) {
	t.Parallel()
	for name, denial := range map[string]error{
		"with the detail":    failure(codes.PermissionDenied, "permission_denied"),
		"by the status code": status.Error(codes.PermissionDenied, "no runtime scope"),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := newFake(t, 1)
			f.setFail(func(c call) error {
				if c.rpc == "OpenSession" {
					return denial
				}
				return nil
			})
			err := result(t, start(t, f.newProcessor(t, nil), recording(&offsets{})))
			var e *processor.Error
			if !errors.Is(err, processor.ErrorPermissionDenied) || !errors.As(err, &e) || e.Op != "open_session" ||
				len(f.callsOf("OpenSession")) != 1 {
				t.Errorf("Run with a token without the runtime scope = %v after %d calls", err, len(f.callsOf("OpenSession")))
			}
		})
	}
}

func TestEveryCallPresentsTheCurrentToken(t *testing.T) {
	t.Parallel()
	f := newFake(t, 1)
	f.add(0, "1")
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte(" acs_first\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	p := f.newProcessor(t, func(c *processor.Config) { c.Credential = processor.FileToken(path) })
	results := start(t, p, recording(&offsets{}))
	eventually(t, "a commit and a heartbeat", func() bool { return f.committed() && len(f.callsOf("Heartbeat")) > 0 })
	f.mu.Lock()
	rotated := len(f.calls)
	f.mu.Unlock()
	if err := os.WriteFile(path, []byte("acs_second"), 0o600); err != nil {
		t.Fatal(err)
	}
	eventually(t, "a heartbeat with the new token", func() bool {
		return slices.ContainsFunc(f.callsOf("Heartbeat"), func(c call) bool { return c.authorization == "Bearer acs_second" })
	})
	f.add(0, "2")
	eventually(t, "the partition is committed", f.committed)
	if err := p.Shutdown(context.Background()); err != nil || result(t, results) != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	rpcs := map[string]bool{}
	for i, c := range f.calls {
		rpcs[c.rpc] = true
		if i < rotated && c.authorization != "Bearer acs_first" {
			t.Errorf("call %d %s before the rotation presented %q", i, c.rpc, c.authorization)
		}
		if (c.rpc == "CloseSession" || c.rpc == "Commit" && c.offset == 2) && c.authorization != "Bearer acs_second" {
			t.Errorf("%s after the rotation presented %q", c.rpc, c.authorization)
		}
	}
	if len(rpcs) != 5 {
		t.Errorf("calls of %v, want all five RPCs", rpcs)
	}
}

func TestTransientCommitFailuresRetryTheSameCommit(t *testing.T) {
	t.Parallel()
	f := newFake(t, 1)
	f.add(0, "1", "2", "3")
	var commits atomic.Int32
	f.setFail(func(c call) error {
		if c.rpc != "Commit" {
			return nil
		}
		switch commits.Add(1) {
		case 1:
			return status.Error(codes.Unavailable, "connection reset") // a transport failure, without a detail
		case 2:
			return failure(codes.Internal, "internal")
		}
		return nil
	})
	p := f.newProcessor(t, nil)
	var handled offsets
	start(t, p, recording(&handled))
	eventually(t, "the partition is committed", f.committed)
	if got := f.commitsOf(0); !slices.Equal(got, []uint64{3, 3, 3}) || !slices.Equal(handled.of(0), []int64{1, 2, 3}) {
		t.Errorf("commits %v after handling %v, want the same commit three times after one page", got, handled.of(0))
	}
	if s := p.Snapshot(); s.LastErrorCode != processor.ErrorInternal {
		t.Errorf("last error = %s", s.LastErrorCode)
	}
}

func TestPartitionsShareTheHandlerSlots(t *testing.T) {
	t.Parallel()
	f := newFake(t, 16)
	for partition := range 16 {
		f.add(partition, "1", "2", "3")
	}
	p := f.newProcessor(t, func(c *processor.Config) { c.MaxConcurrentPartitions, c.PageSize = 4, 1 })
	var mu sync.Mutex
	active, most := map[int]bool{}, 0
	var handled offsets
	start(t, p, processor.HandlerFunc(func(_ context.Context, o processor.Observation) error {
		mu.Lock()
		if active[o.Partition] {
			t.Errorf("two pages of partition %d at once", o.Partition)
		}
		active[o.Partition] = true
		most = max(most, len(active))
		mu.Unlock()
		if s := p.Snapshot(); s.ActivePartitions > 4 || s.ActiveHandlers > 4 {
			t.Errorf("snapshot = %+v", s)
		}
		time.Sleep(5 * time.Millisecond)
		mu.Lock()
		delete(active, o.Partition)
		mu.Unlock()
		handled.add(o)
		return nil
	}))
	eventually(t, "every partition is committed", f.committed)
	mu.Lock()
	defer mu.Unlock()
	if most > 4 || most < 2 {
		t.Errorf("at most %d partitions in Handler calls at once, want 2 to 4", most)
	}
	f.mu.Lock()
	if f.overlaps != 0 {
		t.Errorf("%d Reads of a partition started while another one was in progress", f.overlaps)
	}
	f.mu.Unlock()
	for partition := range 16 {
		if got := handled.of(partition); !slices.Equal(got, []int64{1, 2, 3}) {
			t.Errorf("handled offsets of partition %d = %v", partition, got)
		}
	}
	for _, read := range f.callsOf("Read") {
		if read.limit != 1 || read.wait != time.Second {
			t.Errorf("Read of %d observations, waiting %v; want 1 and the LongPollTimeout of 1s", read.limit, read.wait)
		}
	}
}

func TestBusyPartitionsDoNotWaitForIdleOnes(t *testing.T) {
	t.Parallel()
	// The 15 idle partitions long-poll for the whole LongPollTimeout of 30 s, and one Handler call
	// runs at a time.
	f := newFake(t, 16)
	var payloads []string
	for offset := range 20 {
		payloads = append(payloads, strconv.Itoa(offset+1))
	}
	f.add(3, payloads...)
	p := f.newProcessor(t, func(c *processor.Config) {
		c.MaxConcurrentPartitions, c.PageSize, c.LongPollTimeout = 1, 2, 30*time.Second
	})
	var handled offsets
	started := time.Now()
	start(t, p, recording(&handled))
	eventually(t, "the 10 pages of partition 3 are committed", func() bool { return f.checkpoint(3) == 20 })
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Errorf("10 pages took %v", elapsed)
	}
	// Observations that arrive later do not wait either.
	added := time.Now()
	f.add(3, "21", "22", "23")
	eventually(t, "the new observations are committed", func() bool { return f.checkpoint(3) == 23 })
	if elapsed := time.Since(added); elapsed > 3*time.Second {
		t.Errorf("3 new observations took %v", elapsed)
	}
	var want []int64
	for offset := range 23 {
		want = append(want, int64(offset+1))
	}
	if got := handled.of(3); !slices.Equal(got, want) {
		t.Errorf("handled offsets = %v", got)
	}
	for _, read := range f.callsOf("Read") {
		if read.wait != 30*time.Second {
			t.Errorf("Read waiting %v, want the LongPollTimeout of 30s", read.wait)
		}
	}
}

func TestShutdownDrainsTheHandlersAndClosesTheSession(t *testing.T) {
	t.Parallel()
	f := newFake(t, 1)
	f.add(0, "1", "2")
	started, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	p := f.newProcessor(t, nil)
	results := start(t, p, processor.HandlerFunc(func(ctx context.Context, _ processor.Observation) error {
		if calls.Add(1) == 1 {
			close(started)
			<-ctx.Done() // Shutdown ends the context...
			<-release    // ...but the call takes its time
		}
		return nil
	}))
	<-started
	shutdown := make(chan error, 1)
	go func() { shutdown <- p.Shutdown(context.Background()) }()
	time.Sleep(200 * time.Millisecond)
	select {
	case err := <-shutdown:
		t.Fatalf("Shutdown returned %v while a Handler call was in progress", err)
	default:
	}
	if closed, s := f.callsOf("CloseSession"), p.Snapshot(); len(closed) != 0 || s.State != processor.StateStopping || s.ActiveHandlers != 1 {
		t.Errorf("closes %v and snapshot %+v while draining", closed, s)
	}
	close(release)
	if err := <-shutdown; err != nil {
		t.Errorf("Shutdown = %v", err)
	}
	if err := result(t, results); err != nil {
		t.Errorf("Run after Shutdown = %v", err)
	}
	if closed := f.callsOf("CloseSession"); calls.Load() != 1 || len(f.callsOf("Commit")) != 0 || len(closed) != 1 {
		t.Errorf("%d Handler calls, commits %v, closes %v; want 1, none and one", calls.Load(), f.callsOf("Commit"), closed)
	}

	// ShutdownTimeout bounds the wait of the caller, not that of Run.
	f = newFake(t, 1)
	f.add(0, "1")
	started, release = make(chan struct{}), make(chan struct{})
	p = f.newProcessor(t, func(c *processor.Config) { c.ShutdownTimeout = 100 * time.Millisecond })
	results = start(t, p, processor.HandlerFunc(func(context.Context, processor.Observation) error {
		close(started)
		<-release
		return nil
	}))
	<-started
	if err := p.Shutdown(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Shutdown past its timeout = %v", err)
	}
	select {
	case err := <-results:
		t.Fatalf("Run returned %v while a Handler call was in progress", err)
	default:
	}
	close(release)
	if err := result(t, results); err != nil {
		t.Errorf("Run after Shutdown = %v", err)
	}

	// When the context of Run ends, Run drains as well and returns the context's error.
	f = newFake(t, 1)
	f.add(0, "1")
	p = f.newProcessor(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	ended := make(chan error, 1)
	go func() { ended <- p.Run(ctx, recording(&offsets{})) }()
	eventually(t, "the partition is committed", f.committed)
	cancel()
	if err := result(t, ended); !errors.Is(err, context.Canceled) || len(f.callsOf("CloseSession")) != 1 {
		t.Errorf("Run after its context ended = %v, closes %v", err, f.callsOf("CloseSession"))
	}
}

func TestRunRunsOnce(t *testing.T) {
	t.Parallel()
	f := newFake(t, 1)
	p := f.newProcessor(t, nil)
	results := start(t, p, recording(&offsets{}))
	eventually(t, "a session", func() bool { return len(f.callsOf("Read")) > 0 })
	if err := p.Run(context.Background(), recording(&offsets{})); !errors.Is(err, processor.ErrorInvalidConfig) {
		t.Errorf("second Run = %v", err)
	}
	if err := p.Shutdown(context.Background()); err != nil || result(t, results) != nil {
		t.Errorf("Shutdown = %v", err)
	}

	// After Shutdown, Run returns at once.
	p = f.newProcessor(t, nil)
	if err := p.Shutdown(context.Background()); err != nil {
		t.Errorf("Shutdown before Run = %v", err)
	}
	opens := len(f.callsOf("OpenSession"))
	if err := p.Run(context.Background(), recording(&offsets{})); err != nil || len(f.callsOf("OpenSession")) != opens ||
		p.Snapshot().State != processor.StateStopped {
		t.Errorf("Run after Shutdown = %v", err)
	}
}

func TestObserverSeesTheEvents(t *testing.T) {
	t.Parallel()
	f := newFake(t, 1)
	f.add(0, "1", "2")
	var mu sync.Mutex
	var events []processor.Event
	var failed atomic.Bool
	p := f.newProcessor(t, func(c *processor.Config) {
		c.Observer = processor.ObserverFunc(func(e processor.Event) {
			mu.Lock()
			defer mu.Unlock()
			events = append(events, e)
		})
	})
	results := start(t, p, processor.HandlerFunc(func(_ context.Context, o processor.Observation) error {
		if o.Offset == 2 && failed.CompareAndSwap(false, true) {
			return errors.New("once")
		}
		return nil
	}))
	// The fake records a commit before the processor learns of it and reports it.
	eventually(t, "the partition is committed", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return f.committed() && slices.ContainsFunc(events, func(e processor.Event) bool { return e.Kind == processor.EventCheckpointCommitted })
	})
	if err := p.Shutdown(context.Background()); err != nil || result(t, results) != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	var kinds []string
	for _, e := range events {
		kind := string(e.Kind) + ":" + e.Result
		if !slices.Contains(kinds, kind) {
			kinds = append(kinds, kind)
		}
	}
	for _, want := range []string{
		"state_changed:running", "ownership_changed:opened", "observation_handled:success", "observation_handled:retry",
		"retry:page", "checkpoint_committed:", "state_changed:stopping", "ownership_changed:closed", "state_changed:stopped",
	} {
		if !slices.Contains(kinds, want) {
			t.Errorf("events %v lack %s", kinds, want)
		}
	}
}
