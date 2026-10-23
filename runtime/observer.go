package processor

import (
	"time"

	wire "github.com/wiseiot-cloud/observationprocessor/internal/wire/processor"
)

// State is the state of a Processor.
type State string

const (
	// StateStarting: Run opens a session, or waits as a standby while another instance owns the
	// processor.
	StateStarting State = "starting"
	// StateRunning: a session processes the partitions.
	StateRunning State = "running"
	// StatePaused: the processor is paused; the session only heartbeats.
	StatePaused State = "paused"
	// StateDegraded: Run retries after a failure: to open a session, or a page of a partition.
	StateDegraded State = "degraded"
	// StateStopping: Run drains its Handler calls after Shutdown or the end of its context.
	StateStopping State = "stopping"
	// StateStopped: Run returned.
	StateStopped State = "stopped"
)

// Snapshot is the state of a Processor at one moment.
type Snapshot struct {
	State       State
	ProcessorID string
	InstanceID  string
	// OwnerEpoch is the owner epoch of the current or last session, 0 before the first.
	OwnerEpoch int64
	// Filter is the filter of the processor as the OpenSession of the current or last session
	// answered it; the zero Filter before the first session. Its slices are shared: do not modify
	// them.
	Filter Filter
	// Partitions has one entry per partition of the current or last session, in partition order,
	// and is nil before the first session. A new session starts them over from zero values.
	Partitions []PartitionSnapshot
	// ActivePartitions is how many partitions call the Handler for a page, at most
	// MaxConcurrentPartitions.
	ActivePartitions int
	// ActiveHandlers is how many Handler calls run.
	ActiveHandlers int
	// Lag is how many offsets the processor is behind: the sum of the Lag of the Partitions.
	Lag int64
	// LastCommitAt is when the last commit succeeded.
	LastCommitAt time.Time
	// LastErrorCode and LastErrorAt are the code and the time of the last failure.
	LastErrorCode ErrorCode
	LastErrorAt   time.Time
}

// PartitionSnapshot is the state of one partition in the current or last session, as this instance
// saw it.
type PartitionSnapshot struct {
	Partition int
	// Checkpoint is the offset that this instance last committed in the session; it is meaningful
	// only once LastCommitAt is set. A Read does not tell the checkpoint, so before the first commit
	// of the session the SDK does not know it.
	Checkpoint int64
	// HighWatermark is the high watermark of the latest Read of the partition.
	HighWatermark int64
	// Lag is how many offsets the partition is behind: the high watermark of its latest Read minus
	// how far it got. A page counts until it commits, from the offset before its first observation;
	// an empty page counts as read through its next offset.
	Lag int64
	// LastReadAt and LastCommitAt are when the latest Read and commit of the partition in the
	// session succeeded, zero before the first.
	LastReadAt   time.Time
	LastCommitAt time.Time
}

// Filter is the filter of a processor: which raw observations it receives.
type Filter struct {
	ClaimedProtocols        []string
	ProtocolDefinitionIDs   []string
	PrincipalDeviceIDs      []string
	GatewayGroupIDs         []string
	ResolvedSourceDeviceIDs []string
	ProtocolStatuses        []ProtocolStatus
	// Labels matches the observations that carry one of these labels of their resolved source
	// device.
	Labels []string
}

func filterFrom(f *wire.Filter) Filter {
	var statuses []ProtocolStatus
	for _, status := range f.GetProtocolStatuses() {
		statuses = append(statuses, protocolStatuses[status])
	}
	return Filter{
		ClaimedProtocols:        f.GetClaimedProtocols(),
		ProtocolDefinitionIDs:   f.GetProtocolDefinitionIds(),
		PrincipalDeviceIDs:      f.GetPrincipalDeviceIds(),
		GatewayGroupIDs:         f.GetGatewayGroupIds(),
		ResolvedSourceDeviceIDs: f.GetResolvedSourceDeviceIds(),
		ProtocolStatuses:        statuses,
		Labels:                  f.GetLabels(),
	}
}

// Observer receives the events of a Processor, for metrics. It is called synchronously and
// concurrently, so it must be quick and safe for concurrent use.
type Observer interface {
	Observe(event Event)
}

// ObserverFunc is an Observer function.
type ObserverFunc func(event Event)

func (f ObserverFunc) Observe(event Event) { f(event) }

// EventKind is the kind of an Event.
type EventKind string

const (
	// EventStateChanged: the State changed to Result.
	EventStateChanged EventKind = "state_changed"
	// EventOwnershipChanged: a session opened or closed; Result is opened or closed.
	EventOwnershipChanged EventKind = "ownership_changed"
	// EventObservationHandled: a Handler call of Partition returned after Duration; Result is
	// success, retry or permanent.
	EventObservationHandled EventKind = "observation_handled"
	// EventCheckpointCommitted: a commit of Partition succeeded; Lag is the lag after it.
	EventCheckpointCommitted EventKind = "checkpoint_committed"
	// EventRetry: Run retries after Duration; Result is what it retries: session, heartbeat, page
	// or commit, with the Partition of a page or a commit and the Code of an error of Access.
	EventRetry EventKind = "retry"
	// EventFailureSink: the FailureSink recorded an observation of Partition, or failed; Result is
	// recorded or failed.
	EventFailureSink EventKind = "failure_sink"
)

// Event is something that a Processor did.
type Event struct {
	Kind      EventKind
	Result    string
	Code      ErrorCode
	Partition int
	Duration  time.Duration
	Lag       int64
}
