// Command printer runs an Observation Processor with the Go SDK and prints each of its observations
// as one JSON line on standard output; its log goes to standard error. Create the processor over
// REST first (POST /acs/v1/observation-processors) and keep an Access API token in a file:
//
//	go run ./examples/printer -endpoint access.example.com:443 -processor-id <id> -token-file <file>
//
// -insecure reaches a loopback endpoint, such as the Processor Runtime API of a local wiz-acs-core,
// without TLS. Ctrl+C or SIGTERM shuts the processor down, which frees it at once for another
// instance. Printer exits with 0 after a shutdown and with 1 when the processor stops for good,
// for example at a data gap, which an operator resolves by resetting the checkpoints over REST.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	processor "github.com/wiseiot-cloud/observationprocessor/runtime"
)

// line is the JSON line of an observation.
type line struct {
	ObservationID     string                   `json:"observationId"`
	Partition         int                      `json:"partition"`
	Offset            int64                    `json:"offset"`
	PrincipalDeviceID string                   `json:"principalDeviceId"`
	GatewayGroupID    string                   `json:"gatewayGroupId,omitempty"`
	DeliveryID        string                   `json:"deliveryId"`
	EntryIndex        int                      `json:"entryIndex"`
	ClaimedProtocol   string                   `json:"claimedProtocol"`
	ProtocolStatus    processor.ProtocolStatus `json:"protocolStatus"`
	// DeviceID is the device that the observation is about.
	DeviceID string `json:"deviceId"`
	// Tags are the tags that the device had when Access accepted the observation.
	Tags       map[string]string `json:"tags,omitempty"`
	ObservedAt string            `json:"observedAt"`
	ReceivedAt time.Time         `json:"receivedAt"`
	// Payload is the JSON value that the device sent.
	Payload json.RawMessage `json:"payload"`
}

func main() {
	endpoint := flag.String("endpoint", "localhost:8084", "host:port of the Processor Runtime API")
	insecure := flag.Bool("insecure", false, "reach a loopback endpoint without TLS")
	processorID := flag.String("processor-id", "", "ID of the processor to run (required)")
	tokenFile := flag.String("token-file", "", "file that holds the Access API token (required)")
	instanceID := flag.String("instance-id", "", "name of this instance in the processor status (default: the host name)")
	flag.Parse()
	if *processorID == "" || *tokenFile == "" || flag.NArg() > 0 {
		flag.Usage()
		os.Exit(2)
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	p, err := processor.New(processor.Config{
		Endpoint:      *endpoint,
		AllowInsecure: *insecure,
		ProcessorID:   *processorID,
		Credential:    processor.FileToken(*tokenFile),
		InstanceID:    *instanceID,
		Logger:        logger,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	signals, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-signals.Done()
		_ = p.Shutdown(context.Background())
	}()

	// With the default MaxConcurrentPartitions of 1 the Handler runs one call at a time, so it
	// writes to standard output without a lock. A page is committed only after its lines are
	// written; after a crash, the observations of uncommitted pages are printed again.
	output := json.NewEncoder(os.Stdout)
	output.SetEscapeHTML(false)
	err = p.Run(context.Background(), processor.HandlerFunc(func(_ context.Context, o processor.Observation) error {
		return output.Encode(line{
			ObservationID:     o.ID,
			Partition:         o.Partition,
			Offset:            o.Offset,
			PrincipalDeviceID: o.PrincipalDeviceID,
			GatewayGroupID:    o.GatewayGroupID,
			DeliveryID:        o.DeliveryID,
			EntryIndex:        o.EntryIndex,
			ClaimedProtocol:   o.ClaimedProtocol,
			ProtocolStatus:    o.ProtocolStatus,
			DeviceID:          o.EffectiveDeviceID(),
			Tags:              o.Tags,
			ObservedAt:        o.ObservedAt,
			ReceivedAt:        o.ReceivedAt,
			Payload:           o.Payload,
		})
	}))
	if err != nil {
		// Run logged why it stopped.
		os.Exit(1)
	}
}
