package processor_test

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	processor "github.com/wiseiot-cloud/observationprocessor/runtime"
	"github.com/wiseiot-cloud/observationprocessor/testkit"
)

func Example() {
	p, err := processor.New(processor.Config{
		Endpoint:                "access.example.com:443",
		ProcessorID:             os.Getenv("PROCESSOR_ID"),
		Credential:              processor.FileToken("/run/secrets/access-api-token"),
		MaxConcurrentPartitions: 4,
		Logger:                  slog.Default(),
	})
	if err != nil {
		log.Fatal(err)
	}
	// Use an Inbox in the database of the application, in the transaction of its effects.
	inbox := testkit.NewMemoryInbox()
	handler := processor.Idempotent(inbox, processor.HandlerFunc(func(ctx context.Context, o processor.Observation) error {
		var reading struct {
			Temperature float64 `json:"temperature"`
		}
		if err := o.DecodeJSON(&reading); err != nil {
			return processor.Permanent(err) // needs a FailureSink
		}
		log.Printf("%s measured %.1f °C at %s", o.EffectiveDeviceID(), reading.Temperature, o.ObservedAt)
		return nil
	}))

	// Shut down on SIGTERM: the processor is free at once for a standby instance.
	signals, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	go func() {
		<-signals.Done()
		_ = p.Shutdown(context.Background())
	}()
	if err := p.Run(context.Background(), handler); err != nil {
		log.Fatal(err) // for example a data gap, after which an operator resets the checkpoints
	}
}
