package http_test

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/vimoksh-5/split-and-go/pkg/checksum"
	splitHttp "github.com/vimoksh-5/split-and-go/pkg/transport/http"
)

func TestStreamRawResponse(t *testing.T) {
	data := bytes.Repeat([]byte("SPLIT-AND-GO-RAW-STREAMING-BLOCK-1234567890"), 500) // ~21.5 KB
	expectedCRC := checksum.CRC32(data)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		written, crc, err := splitHttp.StreamRawResponse(w, r, bytes.NewReader(data),
			splitHttp.WithRawChunkSize(1024),
			splitHttp.WithRawContentType("text/plain"),
		)
		if err != nil {
			t.Errorf("StreamRawResponse error: %v", err)
		}
		if written != int64(len(data)) {
			t.Errorf("Expected %d written, got %d", len(data), written)
		}
		if crc != expectedCRC {
			t.Errorf("Expected CRC 0x%08X, got 0x%08X", expectedCRC, crc)
		}
	}))
	defer server.Close()

	resp, err := http.Get(server.URL)
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if !bytes.Equal(body, data) {
		t.Fatalf("Streamed body does not match original data")
	}
}

func TestReceiveRawToFile(t *testing.T) {
	data := bytes.Repeat([]byte("DIRECT-TO-DISK-NVME-STREAM-INGESTION-TEST-PAYLOAD!"), 1000) // ~50 KB
	expectedCRC := checksum.CRC32(data)

	tmpDir := t.TempDir()
	destPath := filepath.Join(tmpDir, "uploaded.bin")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n, crc, err := splitHttp.ReceiveRawToFile(r, destPath, splitHttp.WithRawChunkSize(2048))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if n != int64(len(data)) {
			t.Errorf("Expected %d bytes written to file, got %d", len(data), n)
		}
		if crc != expectedCRC {
			t.Errorf("Expected CRC 0x%08X, got 0x%08X", expectedCRC, crc)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	req, err := http.NewRequest(http.MethodPost, server.URL, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("NewRequest failed: %v", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Unexpected status: %d", resp.StatusCode)
	}

	// Verify file on disk
	fileBytes, err := os.ReadFile(destPath)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}

	if !bytes.Equal(fileBytes, data) {
		t.Fatalf("File on disk does not match uploaded payload")
	}
}
