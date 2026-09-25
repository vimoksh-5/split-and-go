package pool_test

import (
	"testing"

	"github.com/vimoksh-5/split-and-go/pkg/pool"
)

func TestTieredPoolGetAndPut(t *testing.T) {
	p := pool.NewTieredPool(1024, 4096, 16384)

	// Small buffer from first pool
	buf1 := p.Get(500)
	if len(buf1) != 500 {
		t.Fatalf("expected len 500, got %d", len(buf1))
	}
	if cap(buf1) < 1024 {
		t.Fatalf("expected cap >= 1024, got %d", cap(buf1))
	}
	p.Put(buf1)

	// Larger buffer from second pool
	buf2 := p.Get(2000)
	if len(buf2) != 2000 {
		t.Fatalf("expected len 2000, got %d", len(buf2))
	}
	if cap(buf2) < 4096 {
		t.Fatalf("expected cap >= 4096, got %d", cap(buf2))
	}
	p.Put(buf2)

	// Buffer larger than max size class (direct allocation)
	buf3 := p.Get(32000)
	if len(buf3) != 32000 {
		t.Fatalf("expected len 32000, got %d", len(buf3))
	}
	p.Put(buf3)
}

func BenchmarkTieredPool(b *testing.B) {
	p := pool.DefaultPool
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buf := p.Get(64 * 1024)
		buf[0] = byte(i)
		p.Put(buf)
	}
}
