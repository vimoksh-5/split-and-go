package splitter_test

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/vimoksh/split-and-go/pkg/compression"
	"github.com/vimoksh/split-and-go/pkg/core"
	"github.com/vimoksh/split-and-go/pkg/metrics"
	"github.com/vimoksh/split-and-go/pkg/splitter"
)

func TestSplitterExactChunks(t *testing.T) {
	// 3 exact chunks of 100 bytes = 300 bytes
	data := bytes.Repeat([]byte("A"), 300)
	s := splitter.New(bytes.NewReader(data),
		splitter.WithChunkSize(100),
		splitter.WithSessionID("test-exact"),
	)

	chunks, err := s.All()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(chunks) != 3 {
		t.Fatalf("expected 3 chunks, got %d", len(chunks))
	}

	for i, c := range chunks {
		if c.Sequence != int64(i) {
			t.Errorf("chunk %d has sequence %d", i, c.Sequence)
		}
		if len(c.Data) != 100 {
			t.Errorf("chunk %d has length %d, want 100", i, len(c.Data))
		}
		if i == 2 {
			if !c.IsLast() {
				t.Errorf("expected chunk 2 to be marked IsLast")
			}
		} else {
			if c.IsLast() {
				t.Errorf("chunk %d should not be marked IsLast", i)
			}
		}
	}
}

func TestSplitterPartialLastChunk(t *testing.T) {
	// 250 bytes with chunk size 100 -> 100, 100, 50
	data := bytes.Repeat([]byte("B"), 250)
	s := splitter.New(bytes.NewReader(data),
		splitter.WithChunkSize(100),
	)

	chunks, err := s.All()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(chunks) != 3 {
		t.Fatalf("expected 3 chunks, got %d", len(chunks))
	}

	if len(chunks[0].Data) != 100 || len(chunks[1].Data) != 100 || len(chunks[2].Data) != 50 {
		t.Fatalf("chunk lengths incorrect: %d, %d, %d", len(chunks[0].Data), len(chunks[1].Data), len(chunks[2].Data))
	}

	if !chunks[2].IsLast() {
		t.Fatalf("last chunk not marked IsLast")
	}
}

func TestSplitterEmptyInput(t *testing.T) {
	s := splitter.New(bytes.NewReader([]byte{}))
	chunk, err := s.Next()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !chunk.IsLast() {
		t.Fatalf("expected empty chunk to be marked IsLast")
	}
	if len(chunk.Data) != 0 {
		t.Fatalf("expected 0 bytes data")
	}

	// Next call should return io.EOF
	_, err2 := s.Next()
	if err2 != io.EOF {
		t.Fatalf("expected io.EOF, got %v", err2)
	}
}

func TestSplitterAsyncStream(t *testing.T) {
	data := bytes.Repeat([]byte("C"), 500)
	collector := metrics.NewMemoryCollector()

	s := splitter.New(bytes.NewReader(data),
		splitter.WithChunkSize(100),
		splitter.WithMetrics(collector),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	chunkCh, errCh := s.Stream(ctx)
	received := 0

	for chunk := range chunkCh {
		received++
		if chunk.Sequence == 4 && !chunk.IsLast() {
			t.Errorf("chunk 4 should be marked IsLast")
		}
	}

	if err := <-errCh; err != nil {
		t.Fatalf("stream error: %v", err)
	}

	if received != 5 {
		t.Fatalf("expected 5 chunks, got %d", received)
	}

	snap := collector.Snapshot()
	if snap.TotalChunksSent != 5 {
		t.Fatalf("metrics sent chunks: %d, want 5", snap.TotalChunksSent)
	}
}

func TestSplitterWithCompression(t *testing.T) {
	// Highly compressible data
	data := bytes.Repeat([]byte("enterprise split and go compression test "), 100)
	s := splitter.New(bytes.NewReader(data),
		splitter.WithChunkSize(1024),
		splitter.WithCompressor(compression.NewGzip()),
	)

	chunks, err := s.All()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify at least the first chunk was compressed
	if !chunks[0].IsCompressed() {
		t.Errorf("chunk 0 expected to be compressed")
	}
	if (chunks[0].Flags & core.FlagIsCompressed) == 0 {
		t.Errorf("flag FlagIsCompressed not set on chunk 0")
	}
}
