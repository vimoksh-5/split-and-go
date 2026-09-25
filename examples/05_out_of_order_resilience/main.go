package main

import (
	"bytes"
	"fmt"
	"log"
	"strings"

	"github.com/vimoksh/split-and-go"
	"github.com/vimoksh/split-and-go/pkg/core"
)

func main() {
	fmt.Println("=== Example 5: Sliding-Window Out-of-Order Reordering & Checksum Resilience ===")

	origText := strings.Repeat("Enterprise networks experience out-of-order packets and packet loss! ", 50)
	src := strings.NewReader(origText)

	// Split into 10 chunks
	s := splitandgo.NewSplitter(src,
		splitandgo.WithChunkSize(len(origText)/10),
		splitandgo.WithChecksumType(splitandgo.ChecksumCRC32),
	)

	chunks, err := s.All()
	if err != nil {
		log.Fatalf("Splitter failed: %v", err)
	}

	fmt.Printf("Original stream split into %d chunks.\n", len(chunks))

	// Scramble the chunk order intentionally
	// Real-world scenario: chunks [4, 1, 0, 3, 2, ...] arrive out of order
	scrambledIndices := []int{2, 0, 4, 1, 3}
	var scrambledOrder []*core.Chunk
	for _, idx := range scrambledIndices {
		if idx < len(chunks) {
			scrambledOrder = append(scrambledOrder, chunks[idx])
		}
	}
	// Append remaining
	for i := 5; i < len(chunks); i++ {
		scrambledOrder = append(scrambledOrder, chunks[i])
	}

	fmt.Print("Arrival sequence of chunk IDs: ")
	for _, c := range scrambledOrder {
		fmt.Printf("#%d ", c.Sequence)
	}
	fmt.Println()

	var assembled bytes.Buffer
	asm := splitandgo.NewAssembler(&assembled,
		splitandgo.WithVerifyChecksums(true),
		splitandgo.WithMaxReorderBuffer(64),
	)

	for _, chunk := range scrambledOrder {
		fmt.Printf(" -> Ingesting chunk #%d (Payload: %d bytes)... \n", chunk.Sequence, chunk.PayloadSize())
		if err := asm.WriteChunk(chunk); err != nil {
			log.Fatalf("Assembly error: %v", err)
		}
	}

	if !asm.IsCompleted() {
		log.Fatalf("Assembler did not complete stream")
	}

	match := bytes.Equal(assembled.Bytes(), []byte(origText))
	fmt.Printf("\n Out-of-order reordering successfully reconstructed stream: %t!\n", match)
	fmt.Printf("Total reassembled size: %d bytes\n", assembled.Len())

	// Demonstrate checksum tampering detection
	fmt.Println("\n--- Tampering Demonstration ---")
	corruptChunk := chunks[0].Clone()
	corruptChunk.Data[0] ^= 0xFF // Flip bits in data
	corruptChunk.Sequence = 0

	tamperAsm := splitandgo.NewAssembler(new(bytes.Buffer))
	tamperErr := tamperAsm.WriteChunk(corruptChunk)
	if tamperErr == splitandgo.ErrChecksumMismatch {
		fmt.Println(" CRC32 Checksum Mismatch successfully caught! Tampered data rejected.")
	} else {
		log.Fatalf("Failed to detect corrupted chunk: %v", tamperErr)
	}
}
