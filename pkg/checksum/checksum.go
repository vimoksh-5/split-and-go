package checksum

import (
	"crypto/sha256"
	"encoding/binary"
	"hash"
	"hash/crc32"
	"sync"

	"github.com/vimoksh/split-and-go/pkg/core"
)

// Castagnoli table for high-performance CRC32C (standard in cloud storage & enterprise networks)
var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

// Hasher defines the interface for calculating and verifying chunk checksums.
type Hasher interface {
	Type() core.ChecksumType
	Compute(data []byte) (uint32, []byte)
	Verify(chunk *core.Chunk) bool
}

// CRC32Hasher calculates 32-bit CRC32 checksums (Castagnoli or IEEE).
type CRC32Hasher struct {
	table *crc32.Table
}

// NewCRC32 creates a CRC32 hasher using the Castagnoli polynomial (recommended).
func NewCRC32() *CRC32Hasher {
	return &CRC32Hasher{table: castagnoliTable}
}

// NewCRC32IEEE creates a CRC32 hasher using the IEEE polynomial.
func NewCRC32IEEE() *CRC32Hasher {
	return &CRC32Hasher{table: crc32.IEEETable}
}

func (h *CRC32Hasher) Type() core.ChecksumType {
	return core.ChecksumCRC32
}

func (h *CRC32Hasher) Compute(data []byte) (uint32, []byte) {
	c := crc32.Checksum(data, h.table)
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], c)
	return c, b[:]
}

func (h *CRC32Hasher) Verify(chunk *core.Chunk) bool {
	if chunk == nil {
		return false
	}
	expected := crc32.Checksum(chunk.Data, h.table)
	return chunk.Checksum == expected
}

// SHA256Hasher calculates cryptographic SHA-256 checksums.
type SHA256Hasher struct {
	pool sync.Pool
}

// NewSHA256 creates a SHA256 hasher using pooled hashers.
func NewSHA256() *SHA256Hasher {
	return &SHA256Hasher{
		pool: sync.Pool{
			New: func() any {
				return sha256.New()
			},
		},
	}
}

func (h *SHA256Hasher) Type() core.ChecksumType {
	return core.ChecksumSHA256
}

func (h *SHA256Hasher) Compute(data []byte) (uint32, []byte) {
	hasher := h.pool.Get().(hash.Hash)
	defer func() {
		hasher.Reset()
		h.pool.Put(hasher)
	}()

	hasher.Write(data)
	sum := hasher.Sum(nil)
	// Lower 32 bits into uint32 for fast header access
	c32 := binary.BigEndian.Uint32(sum[len(sum)-4:])
	return c32, sum
}

func (h *SHA256Hasher) Verify(chunk *core.Chunk) bool {
	if chunk == nil {
		return false
	}
	_, full := h.Compute(chunk.Data)
	if len(chunk.ChecksumBytes) != len(full) {
		return false
	}
	for i := range full {
		if chunk.ChecksumBytes[i] != full[i] {
			return false
		}
	}
	return true
}

// NoneHasher is a no-op hasher when checksums are disabled for maximum raw throughput.
type NoneHasher struct{}

func (n NoneHasher) Type() core.ChecksumType {
	return core.ChecksumNone
}

func (n NoneHasher) Compute(data []byte) (uint32, []byte) {
	return 0, nil
}

func (n NoneHasher) Verify(chunk *core.Chunk) bool {
	return true
}

// CumulativeStreamHasher verifies the integrity of an entire multi-chunk stream.
type CumulativeStreamHasher struct {
	mu     sync.Mutex
	hasher hash.Hash
}

// NewCumulativeStreamHasher initializes a cumulative SHA256 stream hasher.
func NewCumulativeStreamHasher() *CumulativeStreamHasher {
	return &CumulativeStreamHasher{
		hasher: sha256.New(),
	}
}

// Update feeds a chunk's data to the running stream hash.
func (c *CumulativeStreamHasher) Update(data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.hasher.Write(data)
}

// Sum returns the cumulative stream hash bytes.
func (c *CumulativeStreamHasher) Sum() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.hasher.Sum(nil)
}

// Reset clears the cumulative state for a new stream.
func (c *CumulativeStreamHasher) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.hasher.Reset()
}

// GetDefaultHasher returns the standard Hasher for a given core.ChecksumType.
func GetDefaultHasher(t core.ChecksumType) Hasher {
	switch t {
	case core.ChecksumCRC32:
		return NewCRC32()
	case core.ChecksumSHA256:
		return NewSHA256()
	default:
		return NoneHasher{}
	}
}
