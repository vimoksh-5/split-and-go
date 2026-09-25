package record

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/google/uuid"
	"github.com/vimoksh-5/split-and-go/pkg/assembler"
	"github.com/vimoksh-5/split-and-go/pkg/checksum"
	"github.com/vimoksh-5/split-and-go/pkg/core"
	"github.com/vimoksh-5/split-and-go/pkg/metrics"
)

// Format defines serialization format for structured records.
type Format string

const (
	FormatNDJSON Format = "ndjson" // Newline-delimited JSON (stream friendly)
	FormatJSON   Format = "json"   // JSON array
)

// Config configures RecordStreamer.
type Config struct {
	BatchSize     int
	MaxBatchBytes int
	Format        Format
	SessionID     string
	ChecksumType  core.ChecksumType
	Metrics       metrics.Collector
	Metadata      map[string]string
}

// Option configures RecordStreamer.
type Option func(*Config)

// WithBatchSize sets maximum items per chunk (default: 100).
func WithBatchSize(n int) Option {
	return func(c *Config) {
		if n > 0 {
			c.BatchSize = n
		}
	}
}

// WithMaxBatchBytes sets maximum byte size per chunk before flushing (default: 64KB).
func WithMaxBatchBytes(bytes int) Option {
	return func(c *Config) {
		if bytes > 0 {
			c.MaxBatchBytes = bytes
		}
	}
}

// WithFormat sets the record serialization format (default: FormatNDJSON).
func WithFormat(f Format) Option {
	return func(c *Config) {
		c.Format = f
	}
}

// WithSessionID sets the session ID.
func WithSessionID(id string) Option {
	return func(c *Config) {
		c.SessionID = id
	}
}

// Streamer splits a stream or slice of typed records [T] into Chunk envelopes.
type Streamer[T any] struct {
	cfg      Config
	hasher   checksum.Hasher
	sequence int64
	offset   int64
}

// NewStreamer creates a new typed record streamer.
func NewStreamer[T any](opts ...Option) *Streamer[T] {
	cfg := Config{
		BatchSize:     100,
		MaxBatchBytes: 64 * 1024,
		Format:        FormatNDJSON,
		SessionID:     uuid.NewString(),
		ChecksumType:  core.ChecksumCRC32,
		Metrics:       metrics.NoopCollector{},
		Metadata:      make(map[string]string),
	}

	for _, opt := range opts {
		opt(&cfg)
	}

	return &Streamer[T]{
		cfg:    cfg,
		hasher: checksum.GetDefaultHasher(cfg.ChecksumType),
	}
}

// StreamFromChannel consumes items from itemCh and emits *core.Chunk into chunkCh.
func (s *Streamer[T]) StreamFromChannel(ctx context.Context, itemCh <-chan T) (<-chan *core.Chunk, <-chan error) {
	chunkCh := make(chan *core.Chunk, 16)
	errCh := make(chan error, 1)

	go func() {
		defer close(chunkCh)
		defer close(errCh)

		var batch []T
		var currentBytes int

		flushBatch := func(isLast bool) error {
			if len(batch) == 0 && !isLast {
				return nil
			}

			chunk, err := s.encodeBatch(batch, isLast)
			if err != nil {
				return err
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case chunkCh <- chunk:
			}

			batch = batch[:0]
			currentBytes = 0
			return nil
		}

		for {
			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			case item, ok := <-itemCh:
				if !ok {
					// Channel exhausted, flush final batch
					if err := flushBatch(true); err != nil {
						errCh <- err
					}
					return
				}

				batch = append(batch, item)
				// Estimate serialized size roughly
				currentBytes += 128

				if len(batch) >= s.cfg.BatchSize || currentBytes >= s.cfg.MaxBatchBytes {
					if err := flushBatch(false); err != nil {
						errCh <- err
						return
					}
				}
			}
		}
	}()

	return chunkCh, errCh
}

// StreamSlice partitions a slice of items into discrete chunks.
func (s *Streamer[T]) StreamSlice(items []T) ([]*core.Chunk, error) {
	if len(items) == 0 {
		chunk, err := s.encodeBatch(nil, true)
		if err != nil {
			return nil, err
		}
		return []*core.Chunk{chunk}, nil
	}

	var chunks []*core.Chunk
	total := len(items)

	for i := 0; i < total; i += s.cfg.BatchSize {
		end := i + s.cfg.BatchSize
		isLast := false
		if end >= total {
			end = total
			isLast = true
		}

		batch := items[i:end]
		chunk, err := s.encodeBatch(batch, isLast)
		if err != nil {
			return nil, err
		}
		chunks = append(chunks, chunk)
	}

	return chunks, nil
}

func (s *Streamer[T]) encodeBatch(batch []T, isLast bool) (*core.Chunk, error) {
	var buf bytes.Buffer

	if s.cfg.Format == FormatNDJSON {
		for _, item := range batch {
			data, err := json.Marshal(item)
			if err != nil {
				return nil, fmt.Errorf("splitandgo: record marshal error: %w", err)
			}
			buf.Write(data)
			buf.WriteByte('\n')
		}
	} else {
		data, err := json.Marshal(batch)
		if err != nil {
			return nil, fmt.Errorf("splitandgo: batch marshal error: %w", err)
		}
		buf.Write(data)
	}

	payload := buf.Bytes()
	c32, cBytes := s.hasher.Compute(payload)

	var flags core.ChunkFlags
	if isLast {
		flags |= core.FlagIsLast
	}

	meta := make(map[string]string)
	for k, v := range s.cfg.Metadata {
		meta[k] = v
	}
	meta["format"] = string(s.cfg.Format)
	meta["record_count"] = strconv.Itoa(len(batch))

	chunk := &core.Chunk{
		SessionID:     s.cfg.SessionID,
		Sequence:      s.sequence,
		Offset:        s.offset,
		TotalChunks:   -1,
		TotalBytes:    -1,
		Data:          payload,
		Checksum:      c32,
		ChecksumBytes: cBytes,
		ChecksumType:  s.cfg.ChecksumType,
		Flags:         flags,
		Metadata:      meta,
	}

	s.sequence++
	s.offset += int64(len(payload))

	return chunk, nil
}

// Receiver reassembles chunks and decodes them into typed items [T].
type Receiver[T any] struct {
	assembler *assembler.Assembler
}

// NewReceiver creates a new typed record receiver.
func NewReceiver[T any]() *Receiver[T] {
	return &Receiver[T]{}
}

// DecodeChunk extracts records of type T from a chunk.
func (r *Receiver[T]) DecodeChunk(chunk *core.Chunk) ([]T, error) {
	if chunk == nil || len(chunk.Data) == 0 {
		return nil, nil
	}

	format := FormatNDJSON
	if chunk.Metadata != nil && chunk.Metadata["format"] != "" {
		format = Format(chunk.Metadata["format"])
	}

	if format == FormatNDJSON {
		var items []T
		lines := bytes.Split(chunk.Data, []byte("\n"))
		for _, line := range lines {
			line = bytes.TrimSpace(line)
			if len(line) == 0 {
				continue
			}
			var item T
			if err := json.Unmarshal(line, &item); err != nil {
				return nil, fmt.Errorf("splitandgo: record unmarshal error: %w", err)
			}
			items = append(items, item)
		}
		return items, nil
	}

	// JSON Array format
	var items []T
	if err := json.Unmarshal(chunk.Data, &items); err != nil {
		return nil, fmt.Errorf("splitandgo: array unmarshal error: %w", err)
	}
	return items, nil
}

// ConsumeChannel receives chunks from chunkCh and sends decoded items [T] to itemCh.
func (r *Receiver[T]) ConsumeChannel(ctx context.Context, chunkCh <-chan *core.Chunk) (<-chan T, <-chan error) {
	itemCh := make(chan T, 64)
	errCh := make(chan error, 1)

	go func() {
		defer close(itemCh)
		defer close(errCh)

		for {
			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			case chunk, ok := <-chunkCh:
				if !ok {
					return
				}

				items, err := r.DecodeChunk(chunk)
				if err != nil {
					errCh <- err
					return
				}

				for _, it := range items {
					select {
					case <-ctx.Done():
						errCh <- ctx.Err()
						return
					case itemCh <- it:
					}
				}

				if chunk.IsLast() {
					return
				}
			}
		}
	}()

	return itemCh, errCh
}
