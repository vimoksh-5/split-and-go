package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/vimoksh-5/split-and-go"
	"github.com/vimoksh-5/split-and-go/pkg/metrics"
)

func main() {
	fmt.Println("=== Example 1: High-Performance Byte Streaming with Split-and-Go ===")

	// Simulate a 5 MB payload of text data
	chunkSize := 256 * 1024 // 256 KB chunks
	data := strings.Repeat("Split-and-Go is blazing fast and enterprise ready! ", 100000)
	src := strings.NewReader(data)
	totalBytes := int64(src.Len())

	fmt.Printf("Total payload size: %.2f MB\n", float64(totalBytes)/(1024*1024))
	fmt.Printf("Chunk size: %d KB\n\n", chunkSize/1024)

	metricsCollector := splitandgo.NewMemoryMetrics()

	// 1. Initialize the Splitter
	s := splitandgo.NewSplitter(src,
		splitandgo.WithChunkSize(chunkSize),
		splitandgo.WithTotalBytes(totalBytes),
		splitandgo.WithChecksumType(splitandgo.ChecksumCRC32),
		splitandgo.WithMetrics(metricsCollector),
		splitandgo.WithMetadata("filename", "large_dataset.txt"),
	)

	// 2. Initialize Assembler writing to destination
	var dst bytes.Buffer
	asm := splitandgo.NewAssembler(&dst,
		splitandgo.WithVerifyChecksums(true),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	chunkCh, errCh := s.Stream(ctx)
	startTime := time.Now()

	// 3. Process the stream
	for chunk := range chunkCh {
		fmt.Printf("[Stream] Emitted Chunk #%d | Size: %d KB | Offset: %d | Last: %t | CRC32: %d\n",
			chunk.Sequence, chunk.PayloadSize()/1024, chunk.Offset, chunk.IsLast(), chunk.Checksum)

		// Simulate writing chunk on consumer side
		if err := asm.WriteChunk(chunk); err != nil {
			log.Fatalf("Reassembly error: %v", err)
		}
	}

	if err := <-errCh; err != nil && err != io.EOF {
		log.Fatalf("Stream error: %v", err)
	}

	duration := time.Since(startTime)
	snap := metricsCollector.Snapshot()

	fmt.Printf("\n Transfer Completed in %v\n", duration)
	fmt.Printf("Reassembled Size: %.2f MB\n", float64(asm.TotalBytes())/(1024*1024))
	fmt.Printf("Integrity Verified: %t\n", bytes.Equal(dst.Bytes(), []byte(data)))
	fmt.Printf("Metrics -> Chunks Sent: %d, Bytes Sent: %d, Checksum Errors: %d\n",
		snap.TotalChunksSent, snap.TotalBytesSent, snap.ChecksumErrors)
	_ = metrics.Progress{}
}
