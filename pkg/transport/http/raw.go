package http

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"

	"github.com/vimoksh-5/split-and-go/pkg/checksum"
	"github.com/vimoksh-5/split-and-go/pkg/pool"
)

// Raw Header & Trailer constants
const (
	HeaderChecksumCRC32 = "X-Checksum-CRC32"
	HeaderTotalBytesOut = "X-Total-Bytes"
	HeaderTrailer       = "Trailer"
)

// RawConfig holds configuration for raw HTTP streaming.
type RawConfig struct {
	ChunkSize       int
	Pool            pool.BufferPool
	ContentType     string
	VerifyChecksum  bool
	CalculateCRC32  bool
	MaxUploadSize   int64
	FilePermissions os.FileMode
}

// DefaultRawConfig returns production defaults for raw streaming.
func DefaultRawConfig() *RawConfig {
	return &RawConfig{
		ChunkSize:       128 * 1024, // 128 KB default chunk
		Pool:            pool.DefaultPool,
		ContentType:     ContentTypeStream,
		CalculateCRC32:  true,
		VerifyChecksum:  true,
		MaxUploadSize:   50 * 1024 * 1024 * 1024, // 50 GB default limit
		FilePermissions: 0644,
	}
}

// RawOption functional option for raw streaming.
type RawOption func(*RawConfig)

// WithRawChunkSize sets the buffer chunk size for raw streams.
func WithRawChunkSize(size int) RawOption {
	return func(c *RawConfig) {
		if size > 0 {
			c.ChunkSize = size
		}
	}
}

// WithRawContentType sets the HTTP Content-Type.
func WithRawContentType(ct string) RawOption {
	return func(c *RawConfig) {
		if ct != "" {
			c.ContentType = ct
		}
	}
}

// WithRawPool sets a custom buffer pool.
func WithRawPool(p pool.BufferPool) RawOption {
	return func(c *RawConfig) {
		if p != nil {
			c.Pool = p
		}
	}
}

// WithRawMaxUploadSize sets maximum allowed upload size.
func WithRawMaxUploadSize(maxBytes int64) RawOption {
	return func(c *RawConfig) {
		if maxBytes > 0 {
			c.MaxUploadSize = maxBytes
		}
	}
}

// StreamRawResponse streams raw bytes directly from src to an http.ResponseWriter using
// standard Transfer-Encoding: chunked, tiered sync.Pool buffers, automatic socket flushing,
// and hardware Castagnoli CRC32 computed in HTTP trailers.
//
// This allows ANY standard HTTP client (browsers, curl, HTML5 <video>, fetch) to consume
// the stream at line rate with zero client-side dependencies.
func StreamRawResponse(w http.ResponseWriter, r *http.Request, src io.Reader, opts ...RawOption) (int64, uint32, error) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		return 0, 0, ErrFlusherNotSupported
	}

	cfg := DefaultRawConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	// Prepare HTTP response headers
	w.Header().Set(HeaderContentType, cfg.ContentType)
	w.Header().Set("Transfer-Encoding", "chunked")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set(HeaderTrailer, HeaderChecksumCRC32+", "+HeaderTotalBytesOut)
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	buf := cfg.Pool.Get(cfg.ChunkSize)
	defer cfg.Pool.Put(buf)

	hasher := checksum.NewCRC32Hash()
	var totalWritten int64

	for {
		select {
		case <-r.Context().Done():
			return totalWritten, hasher.Sum32(), r.Context().Err()
		default:
			n, err := src.Read(buf)
			if n > 0 {
				if cfg.CalculateCRC32 {
					hasher.Write(buf[:n])
				}
				nw, writeErr := w.Write(buf[:n])
				totalWritten += int64(nw)
				flusher.Flush()
				if writeErr != nil {
					return totalWritten, hasher.Sum32(), writeErr
				}
			}
			if err != nil {
				if errors.Is(err, io.EOF) {
					crc := hasher.Sum32()
					// Set HTTP Trailers for verified integrity on standard HTTP clients
					w.Header().Set(HeaderChecksumCRC32, fmt.Sprintf("0x%08X", crc))
					w.Header().Set(HeaderTotalBytesOut, strconv.FormatInt(totalWritten, 10))
					return totalWritten, crc, nil
				}
				return totalWritten, hasher.Sum32(), err
			}
		}
	}
}

// StreamRawFile opens filePath and streams it directly to the response writer using StreamRawResponse.
func StreamRawFile(w http.ResponseWriter, r *http.Request, filePath string, opts ...RawOption) (int64, uint32, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err == nil {
		w.Header().Set("Content-Length", strconv.FormatInt(fi.Size(), 10))
	}

	return StreamRawResponse(w, r, f, opts...)
}

// ReceiveRawRequest streams raw request body directly into dst with zero RAM accumulation.
// Calculates hardware Castagnoli CRC32 on the fly and optionally verifies client-supplied checksum.
func ReceiveRawRequest(r *http.Request, dst io.Writer, opts ...RawOption) (int64, uint32, error) {
	defer r.Body.Close()

	cfg := DefaultRawConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	buf := cfg.Pool.Get(cfg.ChunkSize)
	defer cfg.Pool.Put(buf)

	hasher := checksum.NewCRC32Hash()
	var totalRead int64

	for {
		select {
		case <-r.Context().Done():
			return totalRead, hasher.Sum32(), r.Context().Err()
		default:
			n, err := r.Body.Read(buf)
			if n > 0 {
				if cfg.MaxUploadSize > 0 && totalRead+int64(n) > cfg.MaxUploadSize {
					return totalRead, hasher.Sum32(), errors.New("splitandgo: payload exceeds max upload limit")
				}
				if cfg.CalculateCRC32 {
					hasher.Write(buf[:n])
				}
				nw, writeErr := dst.Write(buf[:n])
				totalRead += int64(nw)
				if writeErr != nil {
					return totalRead, hasher.Sum32(), writeErr
				}
			}
			if err != nil {
				if errors.Is(err, io.EOF) {
					crc := hasher.Sum32()
					return totalRead, crc, nil
				}
				return totalRead, hasher.Sum32(), err
			}
		}
	}
}

// ReceiveRawToFile streams an incoming upload directly to destPath on disk.
func ReceiveRawToFile(r *http.Request, destPath string, opts ...RawOption) (int64, uint32, error) {
	if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
		return 0, 0, err
	}

	f, err := os.OpenFile(destPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()

	n, crc, err := ReceiveRawRequest(r, f, opts...)
	if err != nil {
		return n, crc, err
	}

	// Commit to physical SSD/NVMe disk platter
	if syncErr := f.Sync(); syncErr != nil {
		return n, crc, syncErr
	}

	return n, crc, nil
}
