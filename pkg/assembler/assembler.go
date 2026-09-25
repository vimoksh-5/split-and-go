package assembler

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/vimoksh/split-and-go/pkg/checksum"
	"github.com/vimoksh/split-and-go/pkg/compression"
	"github.com/vimoksh/split-and-go/pkg/core"
	"github.com/vimoksh/split-and-go/pkg/metrics"
)

var (
	ErrReorderBufferFull = errors.New("splitandgo: reorder buffer capacity exceeded")
	ErrStreamCompleted   = errors.New("splitandgo: stream is already completed")
)

// Default settings
const (
	DefaultMaxReorderBuffer = 128
)

// Config configures Assembler behavior.
type Config struct {
	SessionID        string
	VerifyChecksums  bool
	MaxReorderBuffer int
	Decompressor     compression.Compressor
	Metrics          metrics.Collector
	VerifyCumulative bool
}

// Option configures Assembler options.
type Option func(*Config)

// WithExpectedSessionID validates that all chunks match this session ID.
func WithExpectedSessionID(id string) Option {
	return func(c *Config) {
		c.SessionID = id
	}
}

// WithVerifyChecksums toggles checksum verification (default: true).
func WithVerifyChecksums(verify bool) Option {
	return func(c *Config) {
		c.VerifyChecksums = verify
	}
}

// WithMaxReorderBuffer sets max out-of-order chunks to buffer (default: 128).
func WithMaxReorderBuffer(limit int) Option {
	return func(c *Config) {
		if limit > 0 {
			c.MaxReorderBuffer = limit
		}
	}
}

// WithDecompressor specifies the decompressor for compressed chunks.
func WithDecompressor(dec compression.Compressor) Option {
	return func(c *Config) {
		c.Decompressor = dec
	}
}

// WithMetrics attaches a metrics collector.
func WithMetrics(col metrics.Collector) Option {
	return func(c *Config) {
		c.Metrics = col
	}
}

// Assembler reassembles streamed chunks into an io.Writer in correct sequence.
type Assembler struct {
	writer           io.Writer
	cfg              Config
	mu               sync.Mutex
	nextSequence     int64
	totalBytes       int64
	isCompleted      bool
	pendingChunks    map[int64]*core.Chunk
	hashers          map[core.ChecksumType]checksum.Hasher
	cumulativeHasher *checksum.CumulativeStreamHasher
	streamStarted    time.Time
}

// New creates a new chunk Assembler writing to w.
func New(w io.Writer, opts ...Option) *Assembler {
	cfg := Config{
		VerifyChecksums:  true,
		MaxReorderBuffer: DefaultMaxReorderBuffer,
		Decompressor:     compression.NewGzip(),
		Metrics:          metrics.NoopCollector{},
	}

	for _, opt := range opts {
		opt(&cfg)
	}

	if cfg.Metrics == nil {
		cfg.Metrics = metrics.NoopCollector{}
	}

	return &Assembler{
		writer:        w,
		cfg:           cfg,
		pendingChunks: make(map[int64]*core.Chunk),
		hashers: map[core.ChecksumType]checksum.Hasher{
			core.ChecksumNone:   checksum.NoneHasher{},
			core.ChecksumCRC32:  checksum.NewCRC32(),
			core.ChecksumSHA256: checksum.NewSHA256(),
		},
		cumulativeHasher: checksum.NewCumulativeStreamHasher(),
		streamStarted:    time.Now(),
	}
}

// WriteChunk processes and writes an incoming chunk.
func (a *Assembler) WriteChunk(chunk *core.Chunk) error {
	if chunk == nil {
		return errors.New("splitandgo: cannot write nil chunk")
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if a.isCompleted {
		return ErrStreamCompleted
	}

	// Validate SessionID if configured
	if a.cfg.SessionID != "" && chunk.SessionID != "" && chunk.SessionID != a.cfg.SessionID {
		return fmt.Errorf("%w: got %s, expected %s", core.ErrSessionMismatch, chunk.SessionID, a.cfg.SessionID)
	}

	// Verify chunk checksum
	if a.cfg.VerifyChecksums && chunk.ChecksumType != core.ChecksumNone {
		hasher, ok := a.hashers[chunk.ChecksumType]
		if !ok {
			hasher = checksum.GetDefaultHasher(chunk.ChecksumType)
			a.hashers[chunk.ChecksumType] = hasher
		}

		if !hasher.Verify(chunk) {
			a.cfg.Metrics.RecordChecksumError(chunk.SessionID, chunk.Sequence)
			return core.ErrChecksumMismatch
		}
	}

	// If chunk is in the past (duplicate or already written), ignore safely
	if chunk.Sequence < a.nextSequence {
		return nil
	}

	// If chunk is out of order (future sequence), buffer it
	if chunk.Sequence > a.nextSequence {
		if len(a.pendingChunks) >= a.cfg.MaxReorderBuffer {
			return ErrReorderBufferFull
		}
		a.pendingChunks[chunk.Sequence] = chunk.Clone()
		return nil
	}

	// In-order chunk: write it now
	if err := a.flushChunk(chunk); err != nil {
		return err
	}

	// Check if buffered future chunks can now be written sequentially
	for {
		next, exists := a.pendingChunks[a.nextSequence]
		if !exists {
			break
		}
		delete(a.pendingChunks, a.nextSequence)
		if err := a.flushChunk(next); err != nil {
			return err
		}
	}

	return nil
}

// flushChunk decompresses, updates cumulative hash, and writes payload to destination writer.
func (a *Assembler) flushChunk(chunk *core.Chunk) error {
	payload := chunk.Data

	// Handle decompression if flagged
	if chunk.IsCompressed() {
		if a.cfg.Decompressor == nil {
			return errors.New("splitandgo: chunk is compressed but no decompressor configured")
		}
		decompressed, err := a.cfg.Decompressor.Decompress(payload)
		if err != nil {
			return fmt.Errorf("%w: %v", compression.ErrDecompressionFailed, err)
		}
		payload = decompressed
	}

	// Write payload
	if len(payload) > 0 {
		n, err := a.writer.Write(payload)
		if err != nil {
			return err
		}
		a.totalBytes += int64(n)
		a.cumulativeHasher.Update(payload)
	}

	a.cfg.Metrics.RecordChunkReceived(chunk.SessionID, chunk.Sequence, len(payload))
	a.nextSequence++

	if chunk.IsLast() {
		a.isCompleted = true
		a.cfg.Metrics.RecordStreamComplete(chunk.SessionID, a.nextSequence, a.totalBytes, time.Since(a.streamStarted))
	}

	return nil
}

// IsCompleted returns true if the final chunk has been assembled.
func (a *Assembler) IsCompleted() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.isCompleted
}

// TotalBytes returns the total bytes reassembled so far.
func (a *Assembler) TotalBytes() int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.totalBytes
}

// CumulativeHash returns the SHA256 checksum of the entire reassembled stream.
func (a *Assembler) CumulativeHash() []byte {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.cumulativeHasher.Sum()
}

// AsReader converts chunk delivery into an io.ReadCloser.
// Chunks sent to the returned channel will be streamed out through the reader.
func AsReader(ctx context.Context, chunkCh <-chan *core.Chunk, opts ...Option) io.ReadCloser {
	pr, pw := io.Pipe()
	assembler := New(pw, opts...)

	go func() {
		defer pw.Close()

		for {
			select {
			case <-ctx.Done():
				pw.CloseWithError(ctx.Err())
				return
			case chunk, ok := <-chunkCh:
				if !ok {
					// Channel closed
					if !assembler.IsCompleted() {
						pw.CloseWithError(core.ErrStreamIncomplete)
					}
					return
				}

				if err := assembler.WriteChunk(chunk); err != nil {
					pw.CloseWithError(err)
					return
				}

				if chunk.IsLast() {
					return
				}
			}
		}
	}()

	return pr
}
