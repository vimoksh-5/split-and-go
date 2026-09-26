// Package splitandgo provides a high-throughput, enterprise-resilient streaming and chunking
// engine for Go services handling large payloads across HTTP, REST, and gRPC.
//
// Key Features:
//   - Zero-overhead streaming: Process multi-gigabyte files or millions of structured records with bounded RAM.
//   - Dual-Mode: Raw HTTP chunked streaming (any browser/curl) AND Framed binary streaming (enterprise microservices).
//   - Tiered buffer pooling: Minimizes GC pressure under extreme concurrent load.
//   - Checksum integrity: Automatic hardware-accelerated CRC32 or SHA256 verification per chunk.
//   - Sliding window reassembly: Seamlessly handles out-of-order chunk arrivals.
//   - Transports: Native adapters for HTTP (chunked transfer, SSE, binary frames) and gRPC streaming.
//   - Direct-to-Disk Persistence: Overlapped socket streaming directly to NVMe SSDs with zero RAM bloat.
package splitandgo

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/vimoksh-5/split-and-go/pkg/assembler"
	"github.com/vimoksh-5/split-and-go/pkg/checksum"
	"github.com/vimoksh-5/split-and-go/pkg/compression"
	"github.com/vimoksh-5/split-and-go/pkg/core"
	"github.com/vimoksh-5/split-and-go/pkg/metrics"
	"github.com/vimoksh-5/split-and-go/pkg/pool"
	"github.com/vimoksh-5/split-and-go/pkg/record"
	"github.com/vimoksh-5/split-and-go/pkg/retry"
	"github.com/vimoksh-5/split-and-go/pkg/splitter"
	splitHttp "github.com/vimoksh-5/split-and-go/pkg/transport/http"
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
	RawOption    = splitHttp.RawOption
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

// Raw Stream Option aliases
var (
	WithRawChunkSize     = splitHttp.WithRawChunkSize
	WithRawContentType   = splitHttp.WithRawContentType
	WithRawPool          = splitHttp.WithRawPool
	WithRawMaxUploadSize = splitHttp.WithRawMaxUploadSize
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

// ============================================================================
// Plug-and-Play High-Level Facade Helpers (Raw & Framed Modes)
// ============================================================================

// ServeRawStream streams raw bytes directly from src to an http.ResponseWriter using
// standard Transfer-Encoding: chunked, tiered sync.Pool buffers, automatic socket flushing,
// and hardware Castagnoli CRC32 computed in HTTP trailers.
// Compatible with all standard browsers, curl, <video>, and fetch().
func ServeRawStream(w http.ResponseWriter, r *http.Request, src io.Reader, opts ...splitHttp.RawOption) (int64, uint32, error) {
	return splitHttp.StreamRawResponse(w, r, src, opts...)
}

// ServeRawFile streams a file on disk directly to an http.ResponseWriter with zero RAM accumulation.
func ServeRawFile(w http.ResponseWriter, r *http.Request, filePath string, opts ...splitHttp.RawOption) (int64, uint32, error) {
	return splitHttp.StreamRawFile(w, r, filePath, opts...)
}

// ReceiveRaw streams an incoming HTTP upload body directly to an io.Writer with constant RAM.
func ReceiveRaw(r *http.Request, dst io.Writer, opts ...splitHttp.RawOption) (int64, uint32, error) {
	return splitHttp.ReceiveRawRequest(r, dst, opts...)
}

// ReceiveRawToFile streams an incoming HTTP upload directly to destPath on disk with constant flat RAM.
func ReceiveRawToFile(r *http.Request, destPath string, opts ...splitHttp.RawOption) (int64, uint32, error) {
	return splitHttp.ReceiveRawToFile(r, destPath, opts...)
}

// ServeFramedStream streams src using Split-and-Go binary envelope framing (0x534701).
// Best for microservice-to-microservice high-throughput pipelines.
func ServeFramedStream(w http.ResponseWriter, r *http.Request, src io.Reader, opts ...splitter.Option) error {
	return splitHttp.StreamResponse(w, r, src, opts...)
}

// ReceiveFramedRequest receives a framed Split-and-Go request body into dst.
func ReceiveFramedRequest(w http.ResponseWriter, r *http.Request, dst io.Writer, opts ...assembler.Option) error {
	return splitHttp.ReceiveRequest(w, r, dst, opts...)
}

// FileServerHandler creates a drop-in http.Handler that streams files from rootDir at line-rate.
// Seamlessly compatible with standard net/http, Gin, Chi, Echo, and Fiber.
func FileServerHandler(rootDir string, opts ...splitHttp.RawOption) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cleanPath := filepath.Clean(r.URL.Path)
		filePath := filepath.Join(rootDir, cleanPath)
		info, err := os.Stat(filePath)
		if err != nil || info.IsDir() {
			http.NotFound(w, r)
			return
		}
		_, _, _ = splitHttp.StreamRawFile(w, r, filePath, opts...)
	})
}

// UploadHandler creates a drop-in http.Handler that streams incoming uploads directly to destDir on disk.
func UploadHandler(destDir string, opts ...splitHttp.RawOption) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost && r.Method != http.MethodPut {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		filename := r.URL.Query().Get("filename")
		if filename == "" {
			filename = fmt.Sprintf("upload_%d.bin", time.Now().UnixNano())
		}
		destPath := filepath.Join(destDir, filepath.Base(filename))
		written, crc, err := splitHttp.ReceiveRawToFile(r, destPath, opts...)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"status":"success","bytes":%d,"crc32":"0x%08X","path":%q}`, written, crc, destPath)
	})
}
