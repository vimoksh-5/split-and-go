package splitandgo_test

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/vimoksh-5/split-and-go"
	"github.com/vimoksh-5/split-and-go/pkg/checksum"
)

func TestRootFacadeStreaming(t *testing.T) {
	orig := bytes.Repeat([]byte("test facade high level split-and-go streaming "), 10)

	s := splitandgo.NewSplitter(bytes.NewReader(orig),
		splitandgo.WithChunkSize(64),
		splitandgo.WithChecksumType(splitandgo.ChecksumCRC32),
	)

	var dst bytes.Buffer
	asm := splitandgo.NewAssembler(&dst, splitandgo.WithVerifyChecksums(true))

	for {
		chunk, err := s.Next()
		if err != nil {
			break
		}
		if err := asm.WriteChunk(chunk); err != nil {
			t.Fatalf("assembler failed: %v", err)
		}
		if chunk.IsLast() {
			break
		}
	}

	if !bytes.Equal(dst.Bytes(), orig) {
		t.Fatalf("facade assembled payload mismatch")
	}
}

func TestFacadeRawHTTPStreaming(t *testing.T) {
	data := bytes.Repeat([]byte("SPLIT-AND-GO-RAW-HTTP-FACADE-TEST-1234567890"), 200)

	// Test ServeRawStream
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _, err := splitandgo.ServeRawStream(w, r, bytes.NewReader(data),
			splitandgo.WithRawChunkSize(512),
		)
		if err != nil {
			t.Errorf("ServeRawStream error: %v", err)
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
		t.Fatalf("Streamed body mismatch")
	}
}

func TestFacadeHandlers(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "sample.txt")
	testContent := []byte("Hello, Split-and-Go FileServerHandler and UploadHandler!")
	if err := os.WriteFile(testFile, testContent, 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// 1. Test FileServerHandler
	fsHandler := splitandgo.FileServerHandler(tmpDir)
	req := httptest.NewRequest(http.MethodGet, "/sample.txt", nil)
	rr := httptest.NewRecorder()
	fsHandler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("FileServerHandler returned status %d", rr.Code)
	}
	if !bytes.Equal(rr.Body.Bytes(), testContent) {
		t.Fatalf("FileServerHandler content mismatch")
	}

	// 2. Test UploadHandler
	upDir := t.TempDir()
	upHandler := splitandgo.UploadHandler(upDir)
	uploadContent := bytes.Repeat([]byte("UPLOAD-ME-SAFELY-DIRECT-TO-DISK!"), 100)
	expectedCRC := checksum.CRC32(uploadContent)

	upReq := httptest.NewRequest(http.MethodPost, "/upload?filename=test_up.bin", bytes.NewReader(uploadContent))
	upRR := httptest.NewRecorder()
	upHandler.ServeHTTP(upRR, upReq)

	if upRR.Code != http.StatusOK {
		t.Fatalf("UploadHandler returned status %d: %s", upRR.Code, upRR.Body.String())
	}

	savedFile := filepath.Join(upDir, "test_up.bin")
	diskBytes, err := os.ReadFile(savedFile)
	if err != nil {
		t.Fatalf("ReadFile uploaded file failed: %v", err)
	}
	if !bytes.Equal(diskBytes, uploadContent) {
		t.Fatalf("Uploaded file on disk does not match original")
	}

	diskCRC := checksum.CRC32(diskBytes)
	if diskCRC != expectedCRC {
		t.Fatalf("Checksum mismatch on uploaded file")
	}
}
