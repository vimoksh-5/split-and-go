package splitandgo_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/vimoksh/split-and-go"
	"github.com/vimoksh/split-and-go/pkg/record"
)

// BenchmarkThroughput64K measures end-to-end splitting and reassembling throughput with 64KB chunks
func BenchmarkThroughput64K(b *testing.B) {
	payloadSize := 10 * 1024 * 1024 // 10 MB
	raw := bytes.Repeat([]byte("X"), payloadSize)

	b.SetBytes(int64(payloadSize))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		src := bytes.NewReader(raw)
		s := splitandgo.NewSplitter(src,
			splitandgo.WithChunkSize(64*1024),
			splitandgo.WithChecksumType(splitandgo.ChecksumCRC32),
		)

		asm := splitandgo.NewAssembler(io.Discard, splitandgo.WithVerifyChecksums(true))

		for {
			chunk, err := s.Next()
			if err != nil {
				break
			}
			if err := asm.WriteChunk(chunk); err != nil {
				b.Fatalf("WriteChunk error: %v", err)
			}
			if chunk.IsLast() {
				break
			}
		}
	}
}

// BenchmarkRecordStreaming measures throughput for micro-batched structured records
func BenchmarkRecordStreaming(b *testing.B) {
	type BenchItem struct {
		ID   int64  `json:"id"`
		Name string `json:"name"`
		Val  int    `json:"val"`
	}

	items := make([]BenchItem, 10000)
	for i := 0; i < len(items); i++ {
		items[i] = BenchItem{ID: int64(i), Name: "item_benchmark", Val: i * 2}
	}

	streamer := splitandgo.NewRecordStreamer[BenchItem](
		record.WithBatchSize(500),
		record.WithFormat(record.FormatNDJSON),
	)
	receiver := splitandgo.NewRecordReceiver[BenchItem]()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		chunks, err := streamer.StreamSlice(items)
		if err != nil {
			b.Fatalf("StreamSlice error: %v", err)
		}

		for _, chunk := range chunks {
			_, err := receiver.DecodeChunk(chunk)
			if err != nil {
				b.Fatalf("DecodeChunk error: %v", err)
			}
		}
	}
}
