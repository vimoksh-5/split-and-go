package http_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	splitHttp "github.com/vimoksh-5/split-and-go/pkg/transport/http"
	"github.com/vimoksh-5/split-and-go/pkg/splitter"
)

func TestHTTPStreamUploadRoundtrip(t *testing.T) {
	origPayload := bytes.Repeat([]byte("Split-and-Go HTTP chunk streaming upload test payload! "), 100)
	var serverReceived bytes.Buffer

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := splitHttp.ReceiveRequest(w, r, &serverReceived); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := splitHttp.NewClient(server.Client())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resp, err := client.StreamPost(ctx, server.URL, bytes.NewReader(origPayload), splitter.WithChunkSize(256))
	if err != nil {
		t.Fatalf("StreamPost failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("unexpected status code: %d, body: %s", resp.StatusCode, string(body))
	}

	if !bytes.Equal(serverReceived.Bytes(), origPayload) {
		t.Fatalf("server received payload mismatch")
	}
}

func TestHTTPStreamDownloadRoundtrip(t *testing.T) {
	origPayload := bytes.Repeat([]byte("Split-and-Go HTTP chunk streaming download test payload! "), 100)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := splitHttp.StreamResponse(w, r, bytes.NewReader(origPayload), splitter.WithChunkSize(256)); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}))
	defer server.Close()

	client := splitHttp.NewClient(server.Client())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resp, err := server.Client().Get(server.URL)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	var clientReceived bytes.Buffer
	if err := client.ReadStreamResponse(ctx, resp, &clientReceived); err != nil {
		t.Fatalf("ReadStreamResponse failed: %v", err)
	}

	if !bytes.Equal(clientReceived.Bytes(), origPayload) {
		t.Fatalf("client received payload mismatch")
	}
}
