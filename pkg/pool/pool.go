package pool

import (
	"math/bits"
	"sync"
)

// Default size classes: 4KB, 16KB, 64KB, 256KB, 1MB, 4MB, 16MB
var defaultSizeClasses = []int{
	4 * 1024,
	16 * 1024,
	64 * 1024,
	256 * 1024,
	1024 * 1024,
	4 * 1024 * 1024,
	16 * 1024 * 1024,
}

// BufferPool provides reusable byte buffers to minimize GC allocations during chunking.
type BufferPool interface {
	// Get returns a byte slice with at least the requested capacity.
	Get(size int) []byte
	// Put returns a byte slice to the pool.
	Put(buf []byte)
}

// TieredPool is a tiered sync.Pool implementation that groups buffers into size classes.
type TieredPool struct {
	pools []sync.Pool
	sizes []int
}

// NewTieredPool creates a new TieredPool with custom or default size classes.
func NewTieredPool(sizes ...int) *TieredPool {
	if len(sizes) == 0 {
		sizes = defaultSizeClasses
	}

	tp := &TieredPool{
		pools: make([]sync.Pool, len(sizes)),
		sizes: sizes,
	}

	for i, size := range sizes {
		allocSize := size
		tp.pools[i].New = func() any {
			b := make([]byte, allocSize)
			return &b
		}
	}

	return tp
}

// DefaultPool is the global tiered buffer pool instance.
var DefaultPool = NewTieredPool()

// Get retrieves a buffer with at least the specified size.
// The returned slice has len = size, cap >= size.
func (p *TieredPool) Get(size int) []byte {
	if size <= 0 {
		return nil
	}

	// Find the smallest size class that fits
	idx := p.findPoolIndex(size)
	if idx == -1 {
		// Requested size is larger than our largest size class; allocate directly.
		return make([]byte, size)
	}

	ptr := p.pools[idx].Get().(*[]byte)
	buf := *ptr
	return buf[:size]
}

// Put returns the buffer to the appropriate pool based on its capacity.
func (p *TieredPool) Put(buf []byte) {
	c := cap(buf)
	if c == 0 {
		return
	}

	// Find matching size class
	for i := len(p.sizes) - 1; i >= 0; i-- {
		if c >= p.sizes[i] {
			// Zero out reference slices if needed or reuse as-is
			p.pools[i].Put(&buf)
			return
		}
	}
	// If it doesn't match any pool class, let GC collect it.
}

func (p *TieredPool) findPoolIndex(size int) int {
	for i, s := range p.sizes {
		if size <= s {
			return i
		}
	}
	return -1
}

// NextPowerOfTwo returns the next power of two greater than or equal to n.
func NextPowerOfTwo(n int) int {
	if n <= 1 {
		return 1
	}
	return 1 << (bits.Len(uint(n - 1)))
}
