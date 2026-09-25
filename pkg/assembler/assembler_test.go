package assembler_test

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/vimoksh-5/split-and-go/pkg/assembler"
	"github.com/vimoksh-5/split-and-go/pkg/compression"
	"github.com/vimoksh-5/split-and-go/pkg/core"
	"github.com/vimoksh-5/split-and-go/pkg/splitter"
)

func TestAssemblerInOrder(t *testing.T) {
	orig := []byte("Lorem ipsum dolor sit amet, consectetur adipiscing elit. Praesent scelerisque.")
	s := splitter.New(bytes.NewReader(orig), splitter.WithChunkSize(16))

	chunks, err := s.All()
	if err != nil {
		t.Fatalf("splitter failed: %v", err)
	}

	var dst bytes.Buffer
	asm := assembler.New(&dst)

	for _, c := range chunks {
		if err := asm.WriteChunk(c); err != nil {
			t.Fatalf("WriteChunk failed: %v", err)
		}
	}

	if !asm.IsCompleted() {
		t.Fatalf("expected assembler to be completed")
	}

	if !bytes.Equal(dst.Bytes(), orig) {
		t.Fatalf("assembled data mismatch: got %q, want %q", dst.String(), string(orig))
	}
}

func TestAssemblerOutOfOrderReordering(t *testing.T) {
	orig := []byte("0123456789ABCDEFabcdef")
	s := splitter.New(bytes.NewReader(orig), splitter.WithChunkSize(5))

	chunks, err := s.All()
	if err != nil {
		t.Fatalf("splitter failed: %v", err)
	}

	if len(chunks) < 4 {
		t.Fatalf("expected at least 4 chunks, got %d", len(chunks))
	}

	var dst bytes.Buffer
	asm := assembler.New(&dst)

	// Scramble order: send chunk 2, 0, 3, 1, then rest
	scrambled := []*core.Chunk{chunks[2], chunks[0], chunks[3], chunks[1]}
	for i := 4; i < len(chunks); i++ {
		scrambled = append(scrambled, chunks[i])
	}

	for _, c := range scrambled {
		if err := asm.WriteChunk(c); err != nil {
			t.Fatalf("WriteChunk failed on chunk %d: %v", c.Sequence, err)
		}
	}

	if !asm.IsCompleted() {
		t.Fatalf("assembler should be completed")
	}

	if !bytes.Equal(dst.Bytes(), orig) {
		t.Fatalf("assembled data mismatch with out-of-order delivery: got %s, want %s", dst.String(), string(orig))
	}
}

func TestAssemblerChecksumTampering(t *testing.T) {
	orig := []byte("data that will be tampered with in transit")
	s := splitter.New(bytes.NewReader(orig), splitter.WithChunkSize(10))

	chunks, err := s.All()
	if err != nil {
		t.Fatalf("splitter failed: %v", err)
	}

	// Corrupt chunk 1
	chunks[1].Data[0] ^= 0xFF

	var dst bytes.Buffer
	asm := assembler.New(&dst)

	if err := asm.WriteChunk(chunks[0]); err != nil {
		t.Fatalf("chunk 0 failed: %v", err)
	}

	err = asm.WriteChunk(chunks[1])
	if err != core.ErrChecksumMismatch {
		t.Fatalf("expected ErrChecksumMismatch, got %v", err)
	}
}

func TestAssemblerCompressed(t *testing.T) {
	orig := bytes.Repeat([]byte("high volume repetitive data "), 50)
	s := splitter.New(bytes.NewReader(orig),
		splitter.WithChunkSize(256),
		splitter.WithCompressor(compression.NewGzip()),
	)

	chunks, err := s.All()
	if err != nil {
		t.Fatalf("splitter failed: %v", err)
	}

	var dst bytes.Buffer
	asm := assembler.New(&dst, assembler.WithDecompressor(compression.NewGzip()))

	for _, c := range chunks {
		if err := asm.WriteChunk(c); err != nil {
			t.Fatalf("write chunk %d failed: %v", c.Sequence, err)
		}
	}

	if !bytes.Equal(dst.Bytes(), orig) {
		t.Fatalf("decompressed assembled data mismatch")
	}
}

func TestAssemblerAsReader(t *testing.T) {
	orig := []byte("streaming directly through io.Reader with zero buffering")
	s := splitter.New(bytes.NewReader(orig), splitter.WithChunkSize(8))

	chunkCh := make(chan *core.Chunk, 10)
	go func() {
		defer close(chunkCh)
		for {
			c, err := s.Next()
			if err != nil {
				return
			}
			chunkCh <- c
			if c.IsLast() {
				return
			}
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	reader := assembler.AsReader(ctx, chunkCh)
	defer reader.Close()

	readAll, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if !bytes.Equal(readAll, orig) {
		t.Fatalf("AsReader mismatch: got %s, want %s", readAll, orig)
	}
}
