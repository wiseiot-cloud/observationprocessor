package processor

import (
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	wire "github.com/wiseiot-cloud/observationprocessor/internal/wire/processor"
)

// ErrorCode is the machine code of an Error. It is an error itself, so that
// errors.Is(err, ErrorDataGap) tells whether err is an *Error with that code.
type ErrorCode string

const (
	// ErrorInvalidConfig: New refused the Config, Run was called again, or a Handler returned a
	// Permanent error without a FailureSink.
	ErrorInvalidConfig ErrorCode = "invalid_config"
	// ErrorUnauthenticated: Access refused the API token, or the CredentialProvider failed. Run
	// ends.
	ErrorUnauthenticated ErrorCode = "unauthenticated"
	// ErrorPermissionDenied: the API token lacks the runtime scope, or is limited to device labels
	// that do not cover the labels of the processor's filter. Run ends.
	ErrorPermissionDenied ErrorCode = "permission_denied"
	// ErrorProcessorNotFound: the processor does not exist. Run ends.
	ErrorProcessorNotFound ErrorCode = "processor_not_found"
	// ErrorOwnerActive: another instance owns the processor. Run waits as a standby and retries.
	ErrorOwnerActive ErrorCode = "owner_active"
	// ErrorSessionFenced: the session no longer owns the processor, after a filter change, a
	// pause, a checkpoint reset, a takeover or its expiry. Run opens a new session at once.
	ErrorSessionFenced ErrorCode = "session_fenced"
	// ErrorProcessorPaused: the processor is paused. Run only heartbeats until it resumes.
	ErrorProcessorPaused ErrorCode = "processor_paused"
	// ErrorDataGap: the checkpoint of a partition is below its hot floor, so observations after it
	// were deleted from hot storage. Run ends; an operator resets the checkpoints over REST.
	ErrorDataGap ErrorCode = "data_gap"
	// ErrorOffsetOutOfRange: Access refused a commit. Run ends.
	ErrorOffsetOutOfRange ErrorCode = "offset_out_of_range"
	// ErrorInvalidArgument: Access refused a request. Run ends.
	ErrorInvalidArgument ErrorCode = "invalid_argument"
	// ErrorUnavailable: Access could not be reached or did not answer in time. Run retries.
	ErrorUnavailable ErrorCode = "unavailable"
	// ErrorInternal: Access failed. Run retries.
	ErrorInternal ErrorCode = "internal"
)

func (c ErrorCode) Error() string { return string(c) }

// Error is an error of the Processor or of a call of the Processor Runtime API.
type Error struct {
	Code ErrorCode
	// Op is the operation that failed: new, run, open_session, heartbeat, read, handle, commit or
	// close_session.
	Op string
	// Partition, Checkpoint and HotFloor locate an ErrorDataGap: the checkpoint of the partition
	// is below its hot floor.
	Partition  int
	Checkpoint int64
	HotFloor   int64
	// Err is the cause.
	Err error
}

func (e *Error) Error() string {
	message := "observation processor " + e.Op + ": " + string(e.Code)
	if e.Code == ErrorDataGap {
		message += fmt.Sprintf(" in partition %d: the checkpoint %d is below the hot floor %d", e.Partition, e.Checkpoint, e.HotFloor)
	}
	if e.Err != nil {
		message += ": " + e.Err.Error()
	}
	return message
}

func (e *Error) Unwrap() error { return e.Err }

// Is reports whether target is the Code of e.
func (e *Error) Is(target error) bool { return target == e.Code }

// rpcError returns the Error of a call of op that failed with err. Its code is that of the
// ErrorDetail of the status or, without a known one, as after a transport failure, follows the
// status code.
func rpcError(op string, err error) *Error {
	e := &Error{Op: op, Err: err}
	answered := status.Convert(err)
	for _, detail := range answered.Details() {
		if detail, ok := detail.(*wire.ErrorDetail); ok {
			e.Code = detailCodes[detail.GetCode()]
			if gap := detail.GetDataGap(); gap != nil {
				e.Partition, e.Checkpoint, e.HotFloor = int(gap.GetPartition()), int64(gap.GetCheckpoint()), int64(gap.GetHotFloor())
			}
		}
	}
	if e.Code != "" {
		return e
	}
	switch answered.Code() {
	case codes.Unauthenticated:
		e.Code = ErrorUnauthenticated
	case codes.PermissionDenied:
		e.Code = ErrorPermissionDenied
	case codes.InvalidArgument:
		e.Code = ErrorInvalidArgument
	case codes.NotFound:
		e.Code = ErrorProcessorNotFound
	case codes.AlreadyExists:
		e.Code = ErrorOwnerActive
	case codes.OutOfRange:
		e.Code = ErrorOffsetOutOfRange
	case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled, codes.ResourceExhausted, codes.Aborted:
		e.Code = ErrorUnavailable
	default:
		e.Code = ErrorInternal
	}
	return e
}

// detailCodes maps the codes of ErrorDetail to ErrorCodes.
var detailCodes = map[string]ErrorCode{
	"unauthenticated":     ErrorUnauthenticated,
	"permission_denied":   ErrorPermissionDenied,
	"invalid_argument":    ErrorInvalidArgument,
	"not_found":           ErrorProcessorNotFound,
	"owner_active":        ErrorOwnerActive,
	"session_fenced":      ErrorSessionFenced,
	"processor_paused":    ErrorProcessorPaused,
	"data_gap":            ErrorDataGap,
	"offset_out_of_range": ErrorOffsetOutOfRange,
	"internal":            ErrorInternal,
}
