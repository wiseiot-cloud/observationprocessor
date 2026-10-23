// Package testkit holds an Inbox and a FailureSink in memory, for tests of applications, and an
// Inbox in a SQL table, for reference.
package testkit

import (
	"bytes"
	"context"
	"sync"

	processor "github.com/wiseiot-cloud/observationprocessor/runtime"
)

// MemoryInbox is a processor.Inbox in memory.
type MemoryInbox struct {
	mu        sync.Mutex
	processed map[string]bool
}

func NewMemoryInbox() *MemoryInbox {
	return &MemoryInbox{processed: map[string]bool{}}
}

// ProcessOnce runs operation unless key was processed, and records key when operation succeeds.
// It runs one operation at a time.
func (i *MemoryInbox) ProcessOnce(ctx context.Context, key string, operation func(context.Context) error) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.processed[key] {
		return nil
	}
	if err := operation(ctx); err != nil {
		return err
	}
	i.processed[key] = true
	return nil
}

// Contains reports whether key was processed.
func (i *MemoryInbox) Contains(key string) bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.processed[key]
}

// MemoryFailureSink is a processor.FailureSink in memory. RecordFailure fails with Err when it is
// set.
type MemoryFailureSink struct {
	mu      sync.Mutex
	records []processor.FailureRecord
	Err     error
}

func (s *MemoryFailureSink) RecordFailure(_ context.Context, record processor.FailureRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Err != nil {
		return s.Err
	}
	record.Observation.Payload = bytes.Clone(record.Observation.Payload)
	s.records = append(s.records, record)
	return nil
}

// Records returns the recorded failures in order.
func (s *MemoryFailureSink) Records() []processor.FailureRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]processor.FailureRecord(nil), s.records...)
}
