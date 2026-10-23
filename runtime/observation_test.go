package processor_test

import (
	"context"
	"errors"
	"testing"

	processor "github.com/wiseiot-cloud/observationprocessor/runtime"
	"github.com/wiseiot-cloud/observationprocessor/testkit"
)

func TestObservationHelpers(t *testing.T) {
	t.Parallel()
	o := processor.Observation{PrincipalDeviceID: "gateway", Payload: []byte(`{"temperature":21.5,"unit":"C"}`)}
	if got := o.EffectiveDeviceID(); got != "gateway" {
		t.Errorf("EffectiveDeviceID without a resolved source = %q", got)
	}
	o.ResolvedSourceDeviceID = "child"
	if got := o.EffectiveDeviceID(); got != "child" {
		t.Errorf("EffectiveDeviceID = %q", got)
	}
	var reading struct {
		Temperature float64 `json:"temperature"`
	}
	if err := o.DecodeJSON(&reading); err != nil || reading.Temperature != 21.5 {
		t.Errorf("DecodeJSON = %+v, %v", reading, err)
	}
	if err := o.DecodeJSONStrict(&reading); err == nil {
		t.Error("DecodeJSONStrict accepted the member unit")
	}
}

func TestIdempotentHandlesAnObservationOnce(t *testing.T) {
	t.Parallel()
	inbox := testkit.NewMemoryInbox()
	calls := 0
	fail := true
	handler := processor.Idempotent(inbox, processor.HandlerFunc(func(context.Context, processor.Observation) error {
		calls++
		if fail {
			return errors.New("rolled back")
		}
		return nil
	}))
	observation := processor.Observation{ID: "observation-1"}
	if err := handler.Handle(context.Background(), observation); err == nil || inbox.Contains("observation-1") {
		t.Errorf("a failed operation = %v, recorded %t", err, inbox.Contains("observation-1"))
	}
	fail = false
	for range 2 {
		if err := handler.Handle(context.Background(), observation); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 2 || !inbox.Contains("observation-1") {
		t.Errorf("%d calls, recorded %t; want 2 and true", calls, inbox.Contains("observation-1"))
	}
}
