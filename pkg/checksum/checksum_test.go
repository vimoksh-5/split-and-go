package checksum_test

import (
	"bytes"
	"testing"

	"github.com/vimoksh-5/split-and-go/pkg/checksum"
	"github.com/vimoksh-5/split-and-go/pkg/core"
)

func TestCRC32Hasher(t *testing.T) {
	hasher := checksum.NewCRC32()
	payload := []byte("high performance streaming with split-and-go")

	c32, cBytes := hasher.Compute(payload)
	if c32 == 0 {
		t.Fatalf("expected non-zero checksum")
	}

	chunk := &core.Chunk{
		Data:          payload,
		Checksum:      c32,
		ChecksumBytes: cBytes,
		ChecksumType:  core.ChecksumCRC32,
	}

	if !hasher.Verify(chunk) {
		t.Fatalf("checksum verification failed")
	}

	// Corrupt payload
	chunk.Data = []byte("corrupted payload")
	if hasher.Verify(chunk) {
		t.Fatalf("expected verification to fail on corrupted chunk")
	}
}

func TestSHA256Hasher(t *testing.T) {
	hasher := checksum.NewSHA256()
	payload := []byte("sha256 chunk integrity check")

	c32, cBytes := hasher.Compute(payload)
	chunk := &core.Chunk{
		Data:          payload,
		Checksum:      c32,
		ChecksumBytes: cBytes,
		ChecksumType:  core.ChecksumSHA256,
	}

	if !hasher.Verify(chunk) {
		t.Fatalf("SHA256 verification failed")
	}

	// Corrupt checksum bytes
	chunk.ChecksumBytes[0] ^= 0xFF
	if hasher.Verify(chunk) {
		t.Fatalf("expected verification to fail with tampered checksum bytes")
	}
}

func TestCumulativeStreamHasher(t *testing.T) {
	ch := checksum.NewCumulativeStreamHasher()
	ch.Update([]byte("part1"))
	ch.Update([]byte("part2"))
	ch.Update([]byte("part3"))

	sum1 := ch.Sum()

	// Verify equal to hashing "part1part2part3"
	standalone := checksum.NewSHA256()
	_, full := standalone.Compute([]byte("part1part2part3"))

	if !bytes.Equal(sum1, full) {
		t.Fatalf("cumulative hash mismatch: got %x, want %x", sum1, full)
	}
}
