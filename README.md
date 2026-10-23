# WiseIoT Observation Processor Go SDK

`github.com/wiseiot-cloud/observationprocessor` is the Go module that runs WiseIoT Access
Observation Processors. An Observation Processor is a durable subscription of the raw observations
that match a filter, with a checkpoint in each of the 16 feed partitions. Operators create and manage
processors over the Access REST API (`/acs/v1/observation-processors`); an application runs one
with this SDK against the Processor Runtime gRPC API of `wiz-acs-core`.

| Import | Purpose |
|---|---|
| `observationprocessor/runtime` (package `processor`) | `Processor`, `Handler`, `Inbox`, `FailureSink`, retries, credentials, errors, `Snapshot` and `Observer` |
| `observationprocessor/testkit` | `MemoryInbox` and `MemoryFailureSink` for tests, and `SQLInbox`, an Inbox in a PostgreSQL table, for reference |

The generated wire client stays private under `internal/wire`; the public types never expose it.

```sh
go get github.com/wiseiot-cloud/observationprocessor@latest
```

## Quick start

```go
p, err := processor.New(processor.Config{
    Endpoint:    "access.example.com:443", // the Processor Runtime API, over TLS
    ProcessorID: processorID,
    Credential:  processor.FileToken("/run/secrets/access-api-token"),
})
if err != nil {
    return err
}
handler := processor.HandlerFunc(func(ctx context.Context, o processor.Observation) error {
    var reading struct {
        Temperature float64 `json:"temperature"`
    }
    if err := o.DecodeJSON(&reading); err != nil {
        return processor.Permanent(err) // recorded by the FailureSink, then skipped
    }
    return store(ctx, o.ID, o.EffectiveDeviceID(), reading.Temperature)
})
go func() {
    <-terminating // for example SIGTERM
    _ = p.Shutdown(context.Background())
}()
return p.Run(context.Background(), handler)
```

An `Observation` carries the fields of the Processor Runtime `Observation`. `EffectiveDeviceID()` is
the resolved source device, or the principal when the source is unresolved; `Label` is the opaque
label that the resolved source device had when Access accepted the observation, empty without one,
and `Tags` its key-value tags then, nil without tags. Neither changes when the device is relabeled
or retagged later; the `Filter` selects on labels but never on tags.

`Run` returns `nil` after `Shutdown`, `ctx.Err()` when its context ends, and otherwise the `*Error`
that retrying cannot fix. `Run` may be called once per `Processor`.

## Example application

[`examples/printer`](examples/printer/main.go) is a complete application: it runs a processor and
prints each observation as one JSON line on standard output. Create the processor over REST, keep
an API token in a file and run it against the Processor Runtime API; `-insecure` reaches a loopback
endpoint without TLS, such as the one of a local `wiz-acs-core`:

```sh
go run github.com/wiseiot-cloud/observationprocessor/examples/printer@latest -endpoint 127.0.0.1:8084 -insecure -processor-id <processorId> -token-file <file>
```

Ctrl+C or SIGTERM shuts it down. It exits with 1 when the processor stops for good, for example at
a data gap. The Access end-to-end journey (`tests/access/e2e`) runs it.

## Guarantees

- Every partition runs a loop of its own: it reads the next page, waiting up to `LongPollTimeout`
  (default 20 s) while the partition has nothing new, calls the Handler for the page and commits it.
  An idle partition never delays a busy one, a partition never has two pages in flight, and memory
  holds at most one page per partition.
- Within a partition, the Handler sees the observations in offset order, one call at a time. Up to
  `MaxConcurrentPartitions` partitions (1 to 16, default 1) call the Handler at the same time; the
  others wait for a free slot with their page, while reads, commits and retry delays never take one.
- A page (up to `PageSize` observations, default 100) is committed only after every observation of
  it has resolved: the Handler returned `nil`, or `Permanent` and the FailureSink recorded it. Pages
  that match nothing still move the checkpoint.
- Delivery is at least once. A Handler error, a crash, a lost commit answer or a new session hands
  the same observations to the Handler again.

## Idempotency with an Inbox

Make every effect idempotent. `processor.Idempotent(inbox, handler)` calls the Handler through an
`Inbox` keyed by the observation ID; a good Inbox records the key in the same database transaction
as the effects of the Handler:

```go
inbox, err := testkit.NewSQLInbox(db, "observation_inbox")
if err != nil {
    return err
}
handler := processor.Idempotent(inbox, processor.HandlerFunc(
    func(ctx context.Context, o processor.Observation) error {
        tx, _ := testkit.Transaction(ctx)
        _, err := tx.ExecContext(ctx, `INSERT INTO readings (observation_id, payload) VALUES ($1, $2)`, o.ID, o.Payload)
        return err
    }))
```

`SQLInbox` is a reference: own the schema, retention and isolation of the Inbox in the application.
An Inbox cannot make a call of an external API exactly once; use the idempotency key of that API.

## Errors, retries and the Failure Sink

- `nil` resolves the observation.
- Any other error stops the page; the whole page is read again after the delay of `RetryPolicy`
  (`BoundedRetryPolicy` by default: 100 ms doubling to 10 s, with jitter), without a commit.
  `processor.RetryAfter(err, delay)` sets the least delay. A panic counts as an error, and
  `HandlerTimeout` (default 30 s) ends the context of a call.
- `processor.Permanent(err)` resolves the observation once `FailureSink.RecordFailure` succeeded; if
  it fails, the page is read again. The record omits the payload unless `IncludePayloadInFailures`.
  Without a FailureSink, `Run` ends with `ErrorInvalidConfig`; `RequireFailureSink` makes `New`
  refuse such a Config instead.

A transient failure of Access (`ErrorUnavailable`, `ErrorInternal`) is retried: a Read reads the page
again, a Commit commits the same offset again, a heartbeat waits for the next one, and `OpenSession`
backs off. `Run` ends with `ErrorUnauthenticated`, `ErrorPermissionDenied` (the API token lacks the
`processor` scope), `ErrorProcessorNotFound`, `ErrorInvalidArgument`, `ErrorOffsetOutOfRange` or
`ErrorDataGap`. Test codes with `errors.Is(err, processor.ErrorDataGap)` and read the details with
`errors.As(err, &e)` for an `e *processor.Error`.

## Ownership and standby instances

One session at a time owns a processor, with an owner epoch that grows with every session. Run a
second instance as a hot standby: its `OpenSession` answers `owner_active`, and it retries with the
`RetryPolicy` until the owner leaves. `Shutdown` stops new pages, ends the contexts of the Handler
calls in progress, waits for them and closes the session, which frees the processor at once; after
a crash the session expires 30 s after its last heartbeat (every 10 s). `Shutdown` waits at most
`ShutdownTimeout` (default 30 s) or its context, but `Run` itself never opens a session or returns
before every Handler call has returned.

## Pause, filter changes and resets

Pausing a processor, changing its filter or resetting its checkpoints over REST fences the session:
`Run` opens a new one at once, with the new filter and checkpoints. While a processor is paused, the
session only heartbeats; it reads again once a heartbeat tells that the processor resumed.

## Data gaps

Access deletes hot raw data only after archiving it. When a checkpoint falls below the hot floor of
its partition, `Run` stops, after its Handler calls returned, with an `ErrorDataGap` that names the
partition, the checkpoint and the hot floor. Access never skips the gap by itself: an operator
decides, resets the checkpoints over REST
(`POST /acs/v1/observation-processors/{processorId}/checkpoint-reset`), and restarts the
application.

## Credentials and transport

Every call presents an Access API token that holds the `processor` scope, as
`authorization: Bearer <token>`; a token for a processor needs no other scope. `Credential` provides
it before every call: `StaticToken`, `FileToken` (read for every call, so replacing the file rotates
the token) or a `CredentialProviderFunc`. A failing provider ends `Run` with `ErrorUnauthenticated`.
The providers never show the token when formatted; keep tokens out of URLs and logs.

`Endpoint` is `host:port` and uses TLS, verified with the system roots unless `TLSConfig` says
otherwise. `AllowInsecure` dials without TLS and is accepted only for a loopback endpoint.

## Observability

- `Processor.Snapshot()` returns the state (`starting` also while waiting as a standby, `running`,
  `paused`, `degraded` while retrying, `stopping`, `stopped`), the owner epoch, the partitions in
  Handler calls (at most `MaxConcurrentPartitions`) and the Handler calls in progress, the lag (the
  sum over the partitions), the time of the last commit, and the code and time of the last error.
  It is cheap and safe for concurrent use, and returns a copy.
- `Snapshot.Filter` is the filter as the `OpenSession` of the current or last session answered it
  (claimed protocols, protocol definitions, principal devices, gateway groups, resolved source
  devices, protocol statuses and labels); it is the zero `Filter` before the first session, which
  `OwnerEpoch` 0 tells.
- `Snapshot.Partitions` has one `PartitionSnapshot` per partition of the current or last session, in
  partition order: `HighWatermark` and `LastReadAt` of its latest Read, `Lag` (that high watermark
  minus how far the partition got, a page counting until it commits), and `Checkpoint` and
  `LastCommitAt` of the last commit by this instance in the session. A Read does not tell the
  checkpoint, so `Checkpoint` means something only once `LastCommitAt` is set. Every new session
  starts the partitions over from zero values, since it may follow a filter change or a checkpoint
  reset.
- `Config.Observer` receives events (state changes, sessions opened and closed, handled
  observations, commits, retries, Failure Sink results) to build metrics from. It is called
  synchronously and concurrently.
- `Config.Logger` is a `*slog.Logger`; nil logs nothing. The SDK logs neither tokens nor payloads.

The lag and the per-partition values reflect only what this instance saw in its latest Read and
commit, and hold still while the processor is paused. The authoritative status, partition
checkpoints and lags are over REST at `GET /acs/v1/observation-processors/{processorId}/status`.

## Development

| Command | Purpose |
|---|---|
| `make test`, `make test-race` | the tests, without and with the race detector (which needs cgo) |
| `make lint` | `gofmt`, `go vet` and `go mod tidy -diff` |
| `make generate` | regenerate `internal/wire` from `proto`; the result must show no diff |
| `make sync-contracts ACCESS_ROOT=<dir>` | copy the Processor Runtime contract from a WiseIoT Access checkout |

`proto/wiseiot/access/processor/v1/processor_runtime.proto` is a copy of
`contracts/wiseiot/access/processor/v1/processor_runtime.proto` in WiseIoT Access, which owns the
contract. When it changes there, sync it, run `make generate`, adapt the SDK and tag a release
`vX.Y.Z`; Access then pins that release with
`go get -tool github.com/wiseiot-cloud/observationprocessor/examples/printer@vX.Y.Z`.
