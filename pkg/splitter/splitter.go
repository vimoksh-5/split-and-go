package splitter

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/google/uuid"
	"github.com/vimoksh/split-and-go/pkg/checksum"
	"github.com/vimoksh/split-and-go/pkg/compression"
	"github.com/vimoksh/split-and-go/pkg/core"
	"github.com/vimoksh/split-and-go/pkg/metrics"
	"github.com/vimoksh/split-and-go/pkg/pool"
)

// Default settings
const (
	DefaultChunkSize     = 64 * 1024 // 64 KB
	DefaultChannelBuffer = 16
)

// Config configures Splitter behavior.
type Config struct {
	ChunkSize     int
	SessionID     string
	TotalBytes    int64 // -1 if unknown
	TotalChunks   int64 // -1 if unknown
	ChecksumType  core.ChecksumType
	Compressor    compression.Compressor
	Pool          pool.BufferPool
	Metrics       metrics.Collector
	ChannelBuffer int
	Metadata      map[string]string
}

// Option is a functional option for configuring Splitter.
type Option func(*Config)

// WithChunkSize sets the byte size of each chunk.
func WithChunkSize(size int) Option {
	return func(c *Config) {
		if size > 0 {
			c.ChunkSize = size
		}
	}
}

// WithSessionID sets the stream session ID.
func WithSessionID(id string) Option {
	return func(c *Config) {
		c.SessionID = id
	}
}

// WithTotalBytes specifies the total payload byte length if known.
func WithTotalBytes(total int64) Option {
	return func(c *Config) {
		c.TotalBytes = total
		if c.ChunkSize > 0 && total >= 0 {
			c.TotalChunks = (total + int64(c.ChunkSize) - 1) / int64(c.ChunkSize)
		}
	}
}

// WithChecksumType configures the checksum algorithm for chunks.
func WithChecksumType(t core.ChecksumType) Option {
	return func(c *Config) {
		c.ChecksumType = t
	}
}

// WithCompressor sets chunk compression (e.g. gzip).
func WithCompressor(comp compression.Compressor) Option {
	return func(c *Config) {
		c.Compressor = comp
	}
}

// WithBufferPool sets the memory pool for zero-allocation buffer reuse.
func WithBufferPool(p pool.BufferPool) Option {
	return func(c *Config) {
		c.Pool = p
	}
}

// WithMetrics attaches a metrics collector.
func WithMetrics(col metrics.Collector) Option {
	return func(c *Config) {
		c.Metrics = col
	}
}

// WithChannelBuffer configures async stream channel buffer depth.
func WithChannelBuffer(depth int) Option {
	return func(c *Config) {
		if depth > 0 {
			c.ChannelBuffer = depth
		}
	}
}

// WithMetadata adds a key-value header to all chunks.
func WithMetadata(key, value string) Option {
	return func(c *Config) {
		if c.Metadata == nil {
			c.Metadata = make(map[string]string)
		}
		c.Metadata[key] = value
	}
}

// WithMetadataMap sets initial metadata map.
func WithMetadataMap(m map[string]string) Option {
	return func(c *Config) {
		c.Metadata = m
	}
}

// Splitter splits an io.Reader stream into discrete, validated chunks.
type Splitter struct {
	reader        io.Reader
	cfg           Config
	hasher        checksum.Hasher
	sequence      int64
	offset        int64
	currBuf       []byte
	currLen       int
	nextBuf       []byte
	nextLen       int
	initialized   bool
	isDone        bool
	readErr       error
	streamStarted time.Time
}

// New creates a new Splitter for reading and chunking data.
func New(r io.Reader, opts ...Option) *Splitter {
	cfg := Config{
		ChunkSize:     DefaultChunkSize,
		SessionID:     uuid.NewString(),
		TotalBytes:    -1,
		TotalChunks:   -1,
		ChecksumType:  core.ChecksumCRC32,
		Compressor:    compression.NoneCompressor{},
		Pool:          pool.DefaultPool,
		Metrics:       metrics.NoopCollector{},
		ChannelBuffer: DefaultChannelBuffer,
		Metadata:      make(map[string]string),
	}

	for _, opt := range opts {
		opt(&cfg)
	}

	if cfg.Compressor == nil {
		cfg.Compressor = compression.NoneCompressor{}
	}
	if cfg.Pool == nil {
		cfg.Pool = pool.DefaultPool
	}
	if cfg.Metrics == nil {
		cfg.Metrics = metrics.NoopCollector{}
	}

	return &Splitter{
		reader:        r,
		cfg:           cfg,
		hasher:        checksum.GetDefaultHasher(cfg.ChecksumType),
		streamStarted: time.Now(),
	}
}

// SessionID returns the unique session ID of this stream.
func (s *Splitter) SessionID() string {
	return s.cfg.SessionID
}

// TotalChunks returns the total chunks if known, or -1.
func (s *Splitter) TotalChunks() int64 {
	return s.cfg.TotalChunks
}

// initLookahead primes the lookahead buffers.
func (s *Splitter) initLookahead() error {
	s.currBuf = s.cfg.Pool.Get(s.cfg.ChunkSize)
	n1, err1 := io.ReadFull(s.reader, s.currBuf)
	s.currLen = n1

	if err1 != nil && !errors.Is(err1, io.EOF) && !errors.Is(err1, io.ErrUnexpectedEOF) {
		return err1
	}

	if n1 == 0 && (errors.Is(err1, io.EOF) || err1 == nil) {
		// Empty stream
		s.readErr = io.EOF
		return nil
	}

	// Read next chunk for lookahead
	s.nextBuf = s.cfg.Pool.Get(s.cfg.ChunkSize)
	n2, err2 := io.ReadFull(s.reader, s.nextBuf)
	s.nextLen = n2

	if err2 != nil && !errors.Is(err2, io.EOF) && !errors.Is(err2, io.ErrUnexpectedEOF) {
		s.readErr = err2
	} else if errors.Is(err2, io.EOF) {
		s.readErr = io.EOF
	}

	return nil
}

// Next returns the next chunk from the stream. Returns io.EOF when stream is exhausted.
func (s *Splitter) Next() (*core.Chunk, error) {
	if s.isDone {
		return nil, io.EOF
	}

	start := time.Now()

	if !s.initialized {
		s.initialized = true
		if err := s.initLookahead(); err != nil {
			return nil, err
		}
	}

	// Empty stream case
	if s.currLen == 0 {
		s.isDone = true
		s.cfg.Pool.Put(s.currBuf)
		if s.nextBuf != nil {
			s.cfg.Pool.Put(s.nextBuf)
		}
		// Return a final empty chunk
		chunk := &core.Chunk{
			SessionID:    s.cfg.SessionID,
			Sequence:     0,
			Offset:       0,
			TotalChunks:  1,
			TotalBytes:   0,
			Data:         []byte{},
			ChecksumType: s.cfg.ChecksumType,
			Metadata:     s.cfg.Metadata,
		}
		chunk.SetLast(true)
		c32, cBytes := s.hasher.Compute(chunk.Data)
		chunk.Checksum = c32
		chunk.ChecksumBytes = cBytes
		return chunk, nil
	}

	// Determine if current chunk is the last one
	isLast := (s.nextLen == 0)

	// Copy data out of current buffer
	payload := make([]byte, s.currLen)
	copy(payload, s.currBuf[:s.currLen])

	var flags core.ChunkFlags
	if isLast {
		flags |= core.FlagIsLast
	}

	// Optional compression
	if s.cfg.Compressor != nil && s.cfg.Compressor.Algorithm() != compression.None {
		compressed, err := s.cfg.Compressor.Compress(payload)
		if err == nil && len(compressed) < len(payload) {
			payload = compressed
			flags |= core.FlagIsCompressed
		}
	}

	// Compute checksum on the final payload bytes
	c32, cBytes := s.hasher.Compute(payload)

	chunk := &core.Chunk{
		SessionID:     s.cfg.SessionID,
		Sequence:      s.sequence,
		Offset:        s.offset,
		TotalChunks:   s.cfg.TotalChunks,
		TotalBytes:    s.cfg.TotalBytes,
		Data:          payload,
		Checksum:      c32,
		ChecksumBytes: cBytes,
		ChecksumType:  s.cfg.ChecksumType,
		Flags:         flags,
		Metadata:      s.cfg.Metadata,
	}

	s.sequence++
	s.offset += int64(s.currLen)

	s.cfg.Metrics.RecordChunkSent(s.cfg.SessionID, chunk.Sequence, len(chunk.Data), time.Since(start))

	if isLast {
		s.isDone = true
		s.cfg.Pool.Put(s.currBuf)
		if s.nextBuf != nil {
			s.cfg.Pool.Put(s.nextBuf)
		}
		s.cfg.Metrics.RecordStreamComplete(s.cfg.SessionID, s.sequence, s.offset, time.Since(s.streamStarted))
	} else {
		// Slide window: reuse currBuf by reading next chunk into it
		temp := s.currBuf
		s.currBuf = s.nextBuf
		s.currLen = s.nextLen

		if s.readErr == nil {
			n, err := io.ReadFull(s.reader, temp)
			s.nextBuf = temp
			s.nextLen = n
			if err != nil {
				if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
					s.readErr = io.EOF
				} else {
					s.readErr = err
				}
			}
		} else {
			s.nextBuf = nil
			s.nextLen = 0
		}
	}

	return chunk, nil
}

// Stream launches an asynchronous streaming pipeline yielding chunks over a Go channel.
func (s *Splitter) Stream(ctx context.Context) (<-chan *core.Chunk, <-chan error) {
	chunkCh := make(chan *core.Chunk, s.cfg.ChannelBuffer)
	errCh := make(chan error, 1)

	go func() {
		defer close(chunkCh)
		defer close(errCh)

		for {
			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			default:
				chunk, err := s.Next()
				if err != nil {
					if !errors.Is(err, io.EOF) {
						errCh <- err
					}
					return
				}

				select {
				case <-ctx.Done():
					errCh <- ctx.Err()
					return
				case chunkCh <- chunk:
				}

				if chunk.IsLast() {
					return
				}
			}
		}
	}()

	return chunkCh, errCh
}

// All reads and returns all chunks as a slice.
func (s *Splitter) All() ([]*core.Chunk, error) {
	var chunks []*core.Chunk
	for {
		chunk, err := s.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		chunks = append(chunks, chunk)
		if chunk.IsLast() {
			break
		}
	}
	return chunks, nil
}
