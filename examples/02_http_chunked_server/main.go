package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"

	"github.com/vimoksh-5/split-and-go"
	splitHttp "github.com/vimoksh-5/split-and-go/pkg/transport/http"
)

func main() {
	fmt.Println("=== Example 2: HTTP / REST Chunked Streaming with Split-and-Go ===")

	// 1. Setup an HTTP server with Split-and-Go streaming endpoints
	mux := http.NewServeMux()

	// Endpoint 1: Receive chunked upload stream from client
	mux.HandleFunc("/upload", func(w http.ResponseWriter, r *http.Request) {
		var received bytes.Buffer
		if err := splitHttp.ReceiveRequest(w, r, &received); err != nil {
			http.Error(w, fmt.Sprintf("Streaming upload failed: %v", err), http.StatusBadRequest)
			return
		}

		fmt.Printf("[Server] Successfully received and reassembled %d bytes from %s\n",
			received.Len(), r.Header.Get(splitHttp.HeaderSessionID))

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Upload completed successfully"))
	})

	// Endpoint 2: Stream large response back to client using HTTP chunked transfer
	mux.HandleFunc("/download", func(w http.ResponseWriter, r *http.Request) {
		largePayload := strings.Repeat("Streaming response chunk directly to network socket! ", 20000)
		src := strings.NewReader(largePayload)

		fmt.Println("[Server] Streaming chunked response to client...")
		if err := splitHttp.StreamResponse(w, r, src, splitandgo.WithChunkSize(64*1024)); err != nil {
			log.Printf("[Server] Stream response error: %v", err)
		}
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	client := splitHttp.NewClient(server.Client())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 2. Perform chunked stream upload
	fmt.Println("\n[Client] Starting chunked POST upload...")
	uploadData := strings.Repeat("Client payload chunk for enterprise streaming. ", 50000)
	resp, err := client.StreamPost(ctx, server.URL+"/upload", strings.NewReader(uploadData),
		splitandgo.WithChunkSize(128*1024),
		splitandgo.WithChecksumType(splitandgo.ChecksumCRC32),
	)
	if err != nil {
		log.Fatalf("StreamPost failed: %v", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	fmt.Printf("[Client] Server response: %s (Status: %d)\n", string(respBody), resp.StatusCode)

	// 3. Perform chunked stream download
	fmt.Println("\n[Client] Starting chunked GET download...")
	downloadResp, err := server.Client().Get(server.URL + "/download")
	if err != nil {
		log.Fatalf("GET /download failed: %v", err)
	}

	var downloadBuf bytes.Buffer
	if err := client.ReadStreamResponse(ctx, downloadResp, &downloadBuf, splitandgo.WithVerifyChecksums(true)); err != nil {
		log.Fatalf("ReadStreamResponse failed: %v", err)
	}

	fmt.Printf("[Client] Download complete! Reassembled %d bytes seamlessly.\n", downloadBuf.Len())
}
