package testkit

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

func TestMemoryInboxCommitsOnlySuccessfulOperationOnce(t *testing.T) {
	t.Parallel()
	inbox := NewMemoryInbox()
	var calls atomic.Int32
	if err := inbox.ProcessOnce(context.Background(), "observation", func(context.Context) error {
		calls.Add(1)
		return errors.New("rollback")
	}); err == nil {
		t.Fatal("failed operation was accepted")
	}
	for range 2 {
		if err := inbox.ProcessOnce(context.Background(), "observation", func(context.Context) error {
			calls.Add(1)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 2 || !inbox.Contains("observation") {
		t.Fatalf("calls = %d, contains = %t", calls.Load(), inbox.Contains("observation"))
	}
}
