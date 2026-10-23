package processor

import (
	"bytes"
	"encoding/json"
	"time"

	wire "github.com/wiseiot-cloud/observationprocessor/internal/wire/processor"
)

// ProtocolStatus tells how the protocol that an observation claims relates to the protocols bound
// to its source.
type ProtocolStatus string

const (
	// ProtocolMatched: the source is bound to the claimed protocol.
	ProtocolMatched ProtocolStatus = "matched"
	// ProtocolUnregistered: the claimed protocol is not registered.
	ProtocolUnregistered ProtocolStatus = "unregistered"
	// ProtocolSourceUnresolved: no source route resolves the physical source.
	ProtocolSourceUnresolved ProtocolStatus = "source_unresolved"
	// ProtocolUnbound: the source is bound to no protocol.
	ProtocolUnbound ProtocolStatus = "unbound"
	// ProtocolMismatch: the source is bound to other protocols, but not to the claimed one.
	ProtocolMismatch ProtocolStatus = "mismatch"
	// ProtocolSystem: a system observation that Access itself produced, such as a session event
	// under the reserved protocol wiz.access.session:1; it has no source and no classification.
	ProtocolSystem ProtocolStatus = "system"
)

// Observation is a raw observation that Access accepted. Absent optional values are empty.
type Observation struct {
	ID        string
	Partition int
	Offset    int64
	// PrincipalDeviceID is the device that published the observation; for a radio reception, the
	// gateway that received it first.
	PrincipalDeviceID string
	// GatewayGroupID is the gateway group of a radio reception.
	GatewayGroupID  string
	SessionID       string
	ConnectionEpoch int64
	// DeliveryID is the deliveryId of the ObservationBatch, or the publicationId of a radio
	// reception.
	DeliveryID             string
	EntryIndex             int
	ClaimedProtocol        string
	ProtocolStatus         ProtocolStatus
	ProtocolDefinitionID   string
	ClaimedSourceKey       string
	PhysicalSourceKey      string
	ResolvedSourceDeviceID string
	ResolvedSourceKey      string
	// Label is the label that the resolved source device had when Access accepted the observation;
	// empty when it had none or the source is unresolved. Relabeling the device later leaves it.
	Label string
	// Tags are the tags that the resolved source device had when Access accepted the observation;
	// nil when it had none or the source is unresolved. Retagging the device later leaves them.
	Tags map[string]string
	// ObservedAt is the producer's original RFC 3339 text: parse a copy when a time is needed.
	ObservedAt string
	// ReceivedAt is when Access accepted the observation.
	ReceivedAt time.Time
	// Payload is the exact bytes of the payload JSON value.
	Payload []byte
}

// EffectiveDeviceID returns the device that the observation is about: the resolved source device,
// or the principal device when the source is unresolved.
func (o Observation) EffectiveDeviceID() string {
	if o.ResolvedSourceDeviceID != "" {
		return o.ResolvedSourceDeviceID
	}
	return o.PrincipalDeviceID
}

// DecodeJSON decodes the payload into target, as json.Unmarshal does.
func (o Observation) DecodeJSON(target any) error {
	return json.Unmarshal(o.Payload, target)
}

// DecodeJSONStrict is DecodeJSON that fails on an object member without a field in target.
func (o Observation) DecodeJSONStrict(target any) error {
	decoder := json.NewDecoder(bytes.NewReader(o.Payload))
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}

func observationFrom(o *wire.Observation) Observation {
	return Observation{
		ID:                     o.GetObservationId(),
		Partition:              int(o.GetPartition()),
		Offset:                 int64(o.GetOffset()),
		PrincipalDeviceID:      o.GetPrincipalDeviceId(),
		GatewayGroupID:         o.GetGatewayGroupId(),
		SessionID:              o.GetSessionId(),
		ConnectionEpoch:        int64(o.GetConnectionEpoch()),
		DeliveryID:             o.GetDeliveryId(),
		EntryIndex:             int(o.GetEntryIndex()),
		ClaimedProtocol:        o.GetClaimedProtocol(),
		ProtocolStatus:         protocolStatuses[o.GetProtocolStatus()],
		ProtocolDefinitionID:   o.GetProtocolDefinitionId(),
		ClaimedSourceKey:       o.GetClaimedSourceKey(),
		PhysicalSourceKey:      o.GetPhysicalSourceKey(),
		ResolvedSourceDeviceID: o.GetResolvedSourceDeviceId(),
		ResolvedSourceKey:      o.GetResolvedSourceKey(),
		Label:                  o.GetLabel(),
		Tags:                   o.GetTags(),
		ObservedAt:             o.GetObservedAt(),
		ReceivedAt:             o.GetReceivedAt().AsTime(),
		Payload:                o.GetPayload(),
	}
}

var protocolStatuses = map[wire.ProtocolStatus]ProtocolStatus{
	wire.ProtocolStatus_PROTOCOL_STATUS_MATCHED:           ProtocolMatched,
	wire.ProtocolStatus_PROTOCOL_STATUS_UNREGISTERED:      ProtocolUnregistered,
	wire.ProtocolStatus_PROTOCOL_STATUS_SOURCE_UNRESOLVED: ProtocolSourceUnresolved,
	wire.ProtocolStatus_PROTOCOL_STATUS_UNBOUND:           ProtocolUnbound,
	wire.ProtocolStatus_PROTOCOL_STATUS_MISMATCH:          ProtocolMismatch,
	wire.ProtocolStatus_PROTOCOL_STATUS_SYSTEM:            ProtocolSystem,
}
