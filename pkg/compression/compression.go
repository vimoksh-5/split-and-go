package compression

import (
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"sync"
)

var (
	ErrUnknownCompression = errors.New("splitandgo: unknown compression algorithm")
	ErrDecompressionFailed = errors.New("splitandgo: decompression failed")
)

// Algorithm identifies the compression codec.
type Algorithm string

const (
	None Algorithm = "none"
	Gzip Algorithm = "gzip"
)

// Compressor defines the interface for compressing and decompressing chunk payloads.
type Compressor interface {
	Algorithm() Algorithm
	Compress(data []byte) ([]byte, error)
	Decompress(data []byte) ([]byte, error)
}

// GzipCompressor provides pooled gzip compression and decompression.
type GzipCompressor struct {
	level      int
	writerPool sync.Pool
	readerPool sync.Pool
}

// NewGzip creates a GzipCompressor with default or specified compression level.
func NewGzip(level ...int) *GzipCompressor {
	lvl := gzip.DefaultCompression
	if len(level) > 0 {
		lvl = level[0]
	}

	gc := &GzipCompressor{
		level: lvl,
	}

	gc.writerPool = sync.Pool{
		New: func() any {
			w, _ := gzip.NewWriterLevel(io.Discard, gc.level)
			return w
		},
	}

	gc.readerPool = sync.Pool{
		New: func() any {
			return new(gzip.Reader)
		},
	}

	return gc
}

func (g *GzipCompressor) Algorithm() Algorithm {
	return Gzip
}

func (g *GzipCompressor) Compress(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return data, nil
	}

	var buf bytes.Buffer
	w := g.writerPool.Get().(*gzip.Writer)
	w.Reset(&buf)

	if _, err := w.Write(data); err != nil {
		g.writerPool.Put(w)
		return nil, err
	}

	if err := w.Close(); err != nil {
		g.writerPool.Put(w)
		return nil, err
	}

	g.writerPool.Put(w)
	return buf.Bytes(), nil
}

func (g *GzipCompressor) Decompress(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return data, nil
	}

	r := g.readerPool.Get().(*gzip.Reader)
	if err := r.Reset(bytes.NewReader(data)); err != nil {
		g.readerPool.Put(r)
		return nil, err
	}

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		r.Close()
		g.readerPool.Put(r)
		return nil, ErrDecompressionFailed
	}

	if err := r.Close(); err != nil {
		g.readerPool.Put(r)
		return nil, err
	}

	g.readerPool.Put(r)
	return buf.Bytes(), nil
}

// NoneCompressor represents uncompressed data.
type NoneCompressor struct{}

func (n NoneCompressor) Algorithm() Algorithm {
	return None
}

func (n NoneCompressor) Compress(data []byte) ([]byte, error) {
	return data, nil
}

func (n NoneCompressor) Decompress(data []byte) ([]byte, error) {
	return data, nil
}
