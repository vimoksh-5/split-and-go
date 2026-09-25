// Package splitandgo provides a high-throughput, enterprise-resilient streaming and chunking
// engine for Go services handling large payloads across HTTP, REST, and gRPC.
//
// Key Features:
//   - Zero-overhead streaming: Process multi-gigabyte files or millions of structured records with bounded RAM.
//   - Tiered buffer pooling: Minimizes GC pressure under extreme concurrent load.
//   - Checksum integrity: Automatic hardware-accelerated CRC32 or SHA256 verification per chunk.
//   - Sliding window reassembly: Seamlessly handles out-of-order chunk arrivals.
//   - Dual-mode streaming: Arbitrary raw byte streams (files, blobs, proto) & typed record micro-batching (JSON/NDJSON).
//   - Transports: Native adapters for HTTP (chunked transfer, SSE, binary frames) and gRPC streaming.
//   - Resilience: Exponential backoff with jitter and telemetry metrics hooks.
package splitandgo

import (
	"context"
	"io"

	"github.com/vimoksh-5/split-and-go/pkg/assembler"
	"github.com/vimoksh-5/split-and-go/pkg/checksum"
	"github.com/vimoksh-5/split-and-go/pkg/compression"
	"github.com/vimoksh-5/split-and-go/pkg/core"
	"github.com/vimoksh-5/split-and-go/pkg/metrics"
	"github.com/vimoksh-5/split-and-go/pkg/pool"
	"github.com/vimoksh-5/split-and-go/pkg/record"
	"github.com/vimoksh-5/split-and-go/pkg/retry"
	"github.com/vimoksh-5/split-and-go/pkg/splitter"
)

// Type aliases for seamless root package access
type (
	Chunk        = core.Chunk
	ChecksumType = core.ChecksumType
	ChunkFlags   = core.ChunkFlags
	Splitter     = splitter.Splitter
	Assembler    = assembler.Assembler
	BufferPool   = pool.BufferPool
	RetryPolicy  = retry.Policy
	Compressor   = compression.Compressor
	Collector    = metrics.Collector
)

// Checksum constants
const (
	ChecksumNone   = core.ChecksumNone
	ChecksumCRC32  = core.ChecksumCRC32
	ChecksumSHA256 = core.ChecksumSHA256
)

// Common errors
var (
	ErrChecksumMismatch    = core.ErrChecksumMismatch
	ErrSequenceMismatch    = core.ErrSequenceMismatch
	ErrCorruptedChunk      = core.ErrCorruptedChunk
	ErrStreamIncomplete    = core.ErrStreamIncomplete
	ErrReorderBufferFull   = assembler.ErrReorderBufferFull
)

// Splitter Option aliases
var (
	WithChunkSize    = splitter.WithChunkSize
	WithSessionID    = splitter.WithSessionID
	WithTotalBytes   = splitter.WithTotalBytes
	WithChecksumType = splitter.WithChecksumType
	WithCompressor   = splitter.WithCompressor
	WithBufferPool   = splitter.WithBufferPool
	WithMetrics      = splitter.WithMetrics
	WithMetadata     = splitter.WithMetadata
)

// Assembler Option aliases
var (
	WithExpectedSessionID = assembler.WithExpectedSessionID
	WithVerifyChecksums   = assembler.WithVerifyChecksums
	WithMaxReorderBuffer  = assembler.WithMaxReorderBuffer
	WithDecompressor      = assembler.WithDecompressor
)

// Default instances
var (
	DefaultPool   = pool.DefaultPool
	DefaultPolicy = retry.DefaultPolicy
)

// NewSplitter creates a new stream Splitter.
func NewSplitter(r io.Reader, opts ...splitter.Option) *Splitter {
	return splitter.New(r, opts...)
}

// NewAssembler creates a new chunk Assembler writing to w.
func NewAssembler(w io.Writer, opts ...assembler.Option) *Assembler {
	return assembler.New(w, opts...)
}

// AsReader converts a chunk channel into a readable io.ReadCloser.
func AsReader(ctx context.Context, chunkCh <-chan *core.Chunk, opts ...assembler.Option) io.ReadCloser {
	return assembler.AsReader(ctx, chunkCh, opts...)
}

// NewRecordStreamer creates a typed struct/record streamer for micro-batched streaming.
func NewRecordStreamer[T any](opts ...record.Option) *record.Streamer[T] {
	return record.NewStreamer[T](opts...)
}

// NewRecordReceiver creates a receiver for decoding typed records from chunks.
func NewRecordReceiver[T any]() *record.Receiver[T] {
	return record.NewReceiver[T]()
}

// NewCRC32Hasher returns a CRC32 Castagnoli hasher.
func NewCRC32Hasher() checksum.Hasher {
	return checksum.NewCRC32()
}

// NewGzipCompressor returns a pooled Gzip compressor.
func NewGzipCompressor(level ...int) compression.Compressor {
	return compression.NewGzip(level...)
}

// NewMemoryMetrics returns an in-memory metrics tracker.
func NewMemoryMetrics() *metrics.MemoryCollector {
	return metrics.NewMemoryCollector()
}

// NewPatternReader creates a virtual stream reader for generating MB to GB payloads with 0 RAM.
func NewPatternReader(pattern []byte, total int64) *core.PatternReader {
	return core.NewPatternReader(pattern, total)
}
