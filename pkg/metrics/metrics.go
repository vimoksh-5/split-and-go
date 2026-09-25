package metrics

import (
	"sync"
	"sync/atomic"
	"time"
)

// Progress contains the current state of a stream transfer.
type Progress struct {
	SessionID       string
	ChunksProcessed int64
	TotalChunks     int64 // -1 if unknown
	BytesProcessed  int64
	TotalBytes      int64 // -1 if unknown
	Percent         float64
	Elapsed         time.Duration
}

// Collector defines observability hooks for split-and-go operations.
type Collector interface {
	RecordChunkSent(sessionID string, sequence int64, bytes int, duration time.Duration)
	RecordChunkReceived(sessionID string, sequence int64, bytes int)
	RecordChecksumError(sessionID string, sequence int64)
	RecordRetry(sessionID string, attempt int, err error)
	RecordStreamComplete(sessionID string, totalChunks int64, totalBytes int64, duration time.Duration)
}

// Snapshot holds a point-in-time view of streaming statistics.
type Snapshot struct {
	TotalChunksSent     int64
	TotalChunksReceived int64
	TotalBytesSent      int64
	TotalBytesReceived  int64
	ChecksumErrors      int64
	RetryCount          int64
	CompletedStreams    int64
}

// MemoryCollector is an atomic, thread-safe in-memory metrics collector.
type MemoryCollector struct {
	chunksSent     int64
	chunksReceived int64
	bytesSent      int64
	bytesReceived  int64
	checksumErrors int64
	retries        int64
	completed      int64

	mu        sync.RWMutex
	latencies []time.Duration
}

// NewMemoryCollector creates a new MemoryCollector.
func NewMemoryCollector() *MemoryCollector {
	return &MemoryCollector{}
}

func (m *MemoryCollector) RecordChunkSent(sessionID string, sequence int64, bytes int, duration time.Duration) {
	atomic.AddInt64(&m.chunksSent, 1)
	atomic.AddInt64(&m.bytesSent, int64(bytes))
}

func (m *MemoryCollector) RecordChunkReceived(sessionID string, sequence int64, bytes int) {
	atomic.AddInt64(&m.chunksReceived, 1)
	atomic.AddInt64(&m.bytesReceived, int64(bytes))
}

func (m *MemoryCollector) RecordChecksumError(sessionID string, sequence int64) {
	atomic.AddInt64(&m.checksumErrors, 1)
}

func (m *MemoryCollector) RecordRetry(sessionID string, attempt int, err error) {
	atomic.AddInt64(&m.retries, 1)
}

func (m *MemoryCollector) RecordStreamComplete(sessionID string, totalChunks int64, totalBytes int64, duration time.Duration) {
	atomic.AddInt64(&m.completed, 1)
}

// Snapshot returns a copy of the current metrics.
func (m *MemoryCollector) Snapshot() Snapshot {
	return Snapshot{
		TotalChunksSent:     atomic.LoadInt64(&m.chunksSent),
		TotalChunksReceived: atomic.LoadInt64(&m.chunksReceived),
		TotalBytesSent:      atomic.LoadInt64(&m.bytesSent),
		TotalBytesReceived:  atomic.LoadInt64(&m.bytesReceived),
		ChecksumErrors:      atomic.LoadInt64(&m.checksumErrors),
		RetryCount:          atomic.LoadInt64(&m.retries),
		CompletedStreams:    atomic.LoadInt64(&m.completed),
	}
}

// NoopCollector is a zero-cost dummy collector.
type NoopCollector struct{}

func (n NoopCollector) RecordChunkSent(sessionID string, sequence int64, bytes int, duration time.Duration) {}
func (n NoopCollector) RecordChunkReceived(sessionID string, sequence int64, bytes int)                     {}
func (n NoopCollector) RecordChecksumError(sessionID string, sequence int64)                               {}
func (n NoopCollector) RecordRetry(sessionID string, attempt int, err error)                               {}
func (n NoopCollector) RecordStreamComplete(sessionID string, totalChunks int64, totalBytes int64, duration time.Duration) {
}
