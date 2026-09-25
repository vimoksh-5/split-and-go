package http

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"

	"github.com/vimoksh-5/split-and-go/pkg/assembler"
	"github.com/vimoksh-5/split-and-go/pkg/core"
	"github.com/vimoksh-5/split-and-go/pkg/retry"
	"github.com/vimoksh-5/split-and-go/pkg/splitter"
)

// HTTP Header constants used by Split-and-Go
const (
	HeaderSessionID     = "X-SplitAndGo-Session-ID"
	HeaderSequence      = "X-SplitAndGo-Sequence"
	HeaderTotalChunks   = "X-SplitAndGo-Total-Chunks"
	HeaderTotalBytes    = "X-SplitAndGo-Total-Bytes"
	HeaderChecksum      = "X-SplitAndGo-Checksum"
	HeaderChecksumType  = "X-SplitAndGo-Checksum-Type"
	HeaderIsLast        = "X-SplitAndGo-Is-Last"
	HeaderIsCompressed  = "X-SplitAndGo-Is-Compressed"
	HeaderContentType   = "Content-Type"
	ContentTypeBinary   = "application/vnd.splitandgo.chunk"
	ContentTypeStream   = "application/octet-stream"
	ContentTypeNDJSON   = "application/x-ndjson"
)

var (
	ErrFlusherNotSupported = errors.New("splitandgo: http.ResponseWriter does not support http.Flusher")
)

// Client wraps an *http.Client with Split-and-Go streaming capabilities.
type Client struct {
	httpClient *http.Client
	retryPolicy retry.Policy
}

// NewClient creates a new Split-and-Go HTTP client.
func NewClient(httpClient *http.Client, retryPolicy ...retry.Policy) *Client {
	if httpClient == nil {
		httpClient = &http.Client{
			Timeout: 0, // Streaming requests should not have short standard timeouts
		}
	}

	rp := retry.DefaultPolicy()
	if len(retryPolicy) > 0 {
		rp = retryPolicy[0]
	}

	return &Client{
		httpClient:  httpClient,
		retryPolicy: rp,
	}
}

// StreamPost streams an io.Reader as binary framed chunks to the target URL.
func (c *Client) StreamPost(ctx context.Context, url string, r io.Reader, opts ...splitter.Option) (*http.Response, error) {
	// Create piped streaming body
	pr, pw := io.Pipe()
	s := splitter.New(r, opts...)

	go func() {
		defer pw.Close()
		for {
			chunk, err := s.Next()
			if err != nil {
				if !errors.Is(err, io.EOF) {
					pw.CloseWithError(err)
				}
				return
			}

			if err := chunk.EncodeBinary(pw); err != nil {
				pw.CloseWithError(err)
				return
			}

			if chunk.IsLast() {
				return
			}
		}
	}()

	var resp *http.Response
	err := c.retryPolicy.Execute(ctx, func(reqCtx context.Context) error {
		req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, url, pr)
		if err != nil {
			return err
		}

		req.Header.Set(HeaderContentType, ContentTypeBinary)
		req.Header.Set(HeaderSessionID, s.SessionID())

		var httpErr error
		resp, httpErr = c.httpClient.Do(req)
		return httpErr
	})

	return resp, err
}

// ReadStreamResponse reads a binary framed chunked response and reassembles it into w.
func (c *Client) ReadStreamResponse(ctx context.Context, resp *http.Response, w io.Writer, opts ...assembler.Option) error {
	defer resp.Body.Close()

	asm := assembler.New(w, opts...)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			chunk, err := core.DecodeBinary(resp.Body)
			if err != nil {
				if errors.Is(err, io.EOF) {
					if !asm.IsCompleted() {
						return core.ErrStreamIncomplete
					}
					return nil
				}
				return err
			}

			if err := asm.WriteChunk(chunk); err != nil {
				return err
			}

			if chunk.IsLast() {
				return nil
			}
		}
	}
}

// StreamResponse streams an io.Reader to an http.ResponseWriter using chunked transfer and flushing.
func StreamResponse(w http.ResponseWriter, r *http.Request, src io.Reader, opts ...splitter.Option) error {
	flusher, ok := w.(http.Flusher)
	if !ok {
		return ErrFlusherNotSupported
	}

	s := splitter.New(src, opts...)

	w.Header().Set(HeaderContentType, ContentTypeBinary)
	w.Header().Set(HeaderSessionID, s.SessionID())
	w.Header().Set("Transfer-Encoding", "chunked")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	for {
		select {
		case <-r.Context().Done():
			return r.Context().Err()
		default:
			chunk, err := s.Next()
			if err != nil {
				if errors.Is(err, io.EOF) {
					return nil
				}
				return err
			}

			if err := chunk.EncodeBinary(w); err != nil {
				return err
			}

			flusher.Flush()

			if chunk.IsLast() {
				return nil
			}
		}
	}
}

// ReceiveRequest reads chunks from an incoming HTTP request body and writes the payload to dst.
func ReceiveRequest(w http.ResponseWriter, r *http.Request, dst io.Writer, opts ...assembler.Option) error {
	defer r.Body.Close()

	asm := assembler.New(dst, opts...)

	for {
		select {
		case <-r.Context().Done():
			return r.Context().Err()
		default:
			chunk, err := core.DecodeBinary(r.Body)
			if err != nil {
				if errors.Is(err, io.EOF) {
					if !asm.IsCompleted() {
						return core.ErrStreamIncomplete
					}
					return nil
				}
				return err
			}

			if err := asm.WriteChunk(chunk); err != nil {
				return err
			}

			if chunk.IsLast() {
				return nil
			}
		}
	}
}

// SendChunkAsHeaders encodes a single chunk with HTTP headers for REST chunked endpoints.
func PopulateChunkHeaders(header http.Header, chunk *core.Chunk) {
	header.Set(HeaderSessionID, chunk.SessionID)
	header.Set(HeaderSequence, strconv.FormatInt(chunk.Sequence, 10))
	header.Set(HeaderTotalChunks, strconv.FormatInt(chunk.TotalChunks, 10))
	header.Set(HeaderTotalBytes, strconv.FormatInt(chunk.TotalBytes, 10))
	header.Set(HeaderChecksum, strconv.FormatUint(uint64(chunk.Checksum), 10))
	header.Set(HeaderChecksumType, chunk.ChecksumType.String())
	header.Set(HeaderIsLast, strconv.FormatBool(chunk.IsLast()))
	header.Set(HeaderIsCompressed, strconv.FormatBool(chunk.IsCompressed()))
}
