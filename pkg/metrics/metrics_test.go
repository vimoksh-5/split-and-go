package metrics_test

import (
	"errors"
	"testing"
	"time"

	"github.com/vimoksh-5/split-and-go/pkg/metrics"
)

func TestMemoryCollector(t *testing.T) {
	c := metrics.NewMemoryCollector()

	c.RecordChunkSent("sess-1", 0, 1024, 5*time.Millisecond)
	c.RecordChunkSent("sess-1", 1, 2048, 8*time.Millisecond)
	c.RecordChunkReceived("sess-1", 0, 1024)
	c.RecordRetry("sess-1", 1, errors.New("err"))
	c.RecordChecksumError("sess-1", 1)
	c.RecordStreamComplete("sess-1", 2, 3072, 20*time.Millisecond)

	s := c.Snapshot()
	if s.TotalChunksSent != 2 {
		t.Errorf("TotalChunksSent: got %d, want 2", s.TotalChunksSent)
	}
	if s.TotalBytesSent != 3072 {
		t.Errorf("TotalBytesSent: got %d, want 3072", s.TotalBytesSent)
	}
	if s.TotalChunksReceived != 1 {
		t.Errorf("TotalChunksReceived: got %d, want 1", s.TotalChunksReceived)
	}
	if s.TotalBytesReceived != 1024 {
		t.Errorf("TotalBytesReceived: got %d, want 1024", s.TotalBytesReceived)
	}
	if s.RetryCount != 1 {
		t.Errorf("RetryCount: got %d, want 1", s.RetryCount)
	}
	if s.ChecksumErrors != 1 {
		t.Errorf("ChecksumErrors: got %d, want 1", s.ChecksumErrors)
	}
	if s.CompletedStreams != 1 {
		t.Errorf("CompletedStreams: got %d, want 1", s.CompletedStreams)
	}
}
