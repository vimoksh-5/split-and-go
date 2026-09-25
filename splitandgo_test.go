package splitandgo_test

import (
	"bytes"
	"testing"

	"github.com/vimoksh/split-and-go"
)

func TestRootFacadeStreaming(t *testing.T) {
	orig := bytes.Repeat([]byte("test facade high level split-and-go streaming "), 10)

	s := splitandgo.NewSplitter(bytes.NewReader(orig),
		splitandgo.WithChunkSize(64),
		splitandgo.WithChecksumType(splitandgo.ChecksumCRC32),
	)

	var dst bytes.Buffer
	asm := splitandgo.NewAssembler(&dst, splitandgo.WithVerifyChecksums(true))

	for {
		chunk, err := s.Next()
		if err != nil {
			break
		}
		if err := asm.WriteChunk(chunk); err != nil {
			t.Fatalf("assembler failed: %v", err)
		}
		if chunk.IsLast() {
			break
		}
	}

	if !bytes.Equal(dst.Bytes(), orig) {
		t.Fatalf("facade assembled payload mismatch")
	}
}
