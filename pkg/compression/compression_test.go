package compression_test

import (
	"bytes"
	"testing"

	"github.com/vimoksh/split-and-go/pkg/compression"
)

func TestGzipCompressionRoundtrip(t *testing.T) {
	c := compression.NewGzip()
	raw := bytes.Repeat([]byte("Enterprise Split and Go streaming library compression test payload! "), 50)

	compressed, err := c.Compress(raw)
	if err != nil {
		t.Fatalf("Compress failed: %v", err)
	}

	if len(compressed) >= len(raw) {
		t.Fatalf("Expected compressed size (%d) < raw size (%d)", len(compressed), len(raw))
	}

	decompressed, err := c.Decompress(compressed)
	if err != nil {
		t.Fatalf("Decompress failed: %v", err)
	}

	if !bytes.Equal(decompressed, raw) {
		t.Fatalf("Decompressed data did not match original")
	}
}

func TestNoneCompressor(t *testing.T) {
	c := compression.NoneCompressor{}
	raw := []byte("plain data")

	comp, err := c.Compress(raw)
	if err != nil || !bytes.Equal(comp, raw) {
		t.Fatalf("NoneCompressor failed")
	}

	decomp, err := c.Decompress(comp)
	if err != nil || !bytes.Equal(decomp, raw) {
		t.Fatalf("NoneCompressor decompress failed")
	}
}
