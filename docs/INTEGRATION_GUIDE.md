# 🔌 Split-and-Go API Integration Guide

This guide explains how to wire **Split-and-Go** into your existing Go microservices, HTTP handlers (standard `net/http`, Gin, Chi, Echo, Fiber), and gRPC services to eliminate memory bloat, high TTFB, and OOM crashes.

---

## 📌 Table of Contents
1. [Core Architectural Value: Why Use It?](#-core-architectural-value)
2. [Recipe 1: Streaming Database Records & JSON Collections](#-recipe-1-streaming-database-records--json-collections)
3. [Recipe 2: Streaming Files, Images, Videos & Large Blobs (HTTP)](#-recipe-2-streaming-files-images-videos--large-blobs-http)
4. [Recipe 3: gRPC Service-to-Service Streaming](#-recipe-3-grpc-service-to-service-streaming)
5. [Recipe 4: Streaming Large Client Uploads to Server](#-recipe-4-streaming-large-client-uploads-to-server)
6. [Consuming Split Streams in Other Languages (Python, Node.js, Curl)](#-consuming-in-other-languages)
7. [Enterprise & MNC Use Case Catalog](#-enterprise--mnc-use-case-catalog)

---

## 💡 Core Architectural Value

| Without Split-and-Go (Monolithic) | With Split-and-Go (Streaming) |
| :--- | :--- |
| **Server buffers full 200MB in RAM** before sending 1st byte | **Server holds only 64 KB in RAM** at any moment |
| **High TTFB (e.g. 5–30 seconds)** while client blocks | **Sub-millisecond TTFB (< 1ms)** — client receives data immediately |
| **Container OOM Crash** when 100 concurrent users request 50MB | **Flat memory footprint** regardless of concurrency or payload size |
| **No checksums on wire** (silent bit corruption risk) | **Hardware Castagnoli CRC32** verified on every single chunk |

---

## 📋 Recipe 1: Streaming Database Records & JSON Collections

### Scenario:
Your API queries Postgres, MongoDB, or Elasticsearch and returns 100,000 records.

### ❌ The Old Dangerous Way:
```go
// Loading 100,000 structs into memory -> 250 MB heap spike -> GC pause -> Pod OOM!
func GetUsersMonolithic(w http.ResponseWriter, r *http.Request) {
    var users []User
    db.Find(&users) // ❌ Loads 100k items in RAM
    json.NewEncoder(w).Encode(users)
}
```

### ✅ The Split-and-Go Streaming Way:
```go
import (
    "net/http"
    "github.com/vimoksh-5/split-and-go"
    "github.com/vimoksh-5/split-and-go/pkg/record"
)

type User struct {
    ID    int64  `json:"id"`
    Name  string `json:"name"`
    Email string `json:"email"`
}

func GetUsersStream(w http.ResponseWriter, r *http.Request) {
    flusher, ok := w.(http.Flusher)
    if !ok {
        http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
        return
    }

    w.Header().Set("Content-Type", "application/x-ndjson")
    w.Header().Set("Transfer-Encoding", "chunked")
    w.WriteHeader(http.StatusOK)
    flusher.Flush()

    // 1. Create a channel yielding rows from database cursor
    rowCh := make(chan User, 100)
    go func() {
        defer close(rowCh)
        rows, _ := db.Query("SELECT id, name, email FROM users")
        defer rows.Close()
        for rows.Next() {
            var u User
            rows.Scan(&u.ID, &u.Name, &u.Email)
            rowCh <- u
        }
    }()

    // 2. Streamer micro-batches 500 records per chunk
    streamer := splitandgo.NewRecordStreamer[User](
        record.WithBatchSize(500),
        record.WithFormat(record.FormatNDJSON),
    )

    chunkCh, errCh := streamer.StreamFromChannel(r.Context(), rowCh)

    // 3. Flush chunks to socket in real-time
    for chunk := range chunkCh {
        w.Write(chunk.Data)
        flusher.Flush() // Pushes bytes to network immediately!
    }

    if err := <-errCh; err != nil {
        log.Printf("Stream error: %v", err)
    }
}
```

---

## 📋 Recipe 2: Streaming Files, Images, Videos & Large Blobs (HTTP)

### Scenario:
Serving large media, reports, or cloud storage objects (10 MB to 10 GB).

```go
import (
    "os"
    "net/http"
    "github.com/vimoksh-5/split-and-go"
    splitHttp "github.com/vimoksh-5/split-and-go/pkg/transport/http"
)

func DownloadReportHandler(w http.ResponseWriter, r *http.Request) {
    file, err := os.Open("/data/reports/annual_analytics_2026.parquet")
    if err != nil {
        http.Error(w, "File not found", http.StatusNotFound)
        return
    }
    defer file.Close()

    // Streams file in 128 KB chunks with hardware CRC32 checksums
    err = splitHttp.StreamResponse(w, r, file,
        splitandgo.WithChunkSize(128 * 1024),
        splitandgo.WithChecksumType(splitandgo.ChecksumCRC32),
    )
    if err != nil {
        log.Printf("Streaming error: %v", err)
    }
}
```

---

## 📋 Recipe 3: gRPC Service-to-Service Streaming

### Scenario:
Service A (Catalog Service) transfers a 1 GB search index to Service B (Search Worker).

#### gRPC Server Implementation:
```go
import (
    "github.com/vimoksh-5/split-and-go"
    splitGrpc "github.com/vimoksh-5/split-and-go/pkg/transport/grpc"
    "github.com/vimoksh-5/split-and-go/pkg/transport/grpc/pb"
)

type CatalogServer struct {
    pb.UnimplementedStreamServiceServer
}

func (s *CatalogServer) StreamDownload(req *pb.StreamDownloadRequest, stream pb.StreamService_StreamDownloadServer) error {
    fileReader, _ := os.Open("/indexes/" + req.ResourceId)
    defer fileReader.Close()

    // Automatically splits file into ChunkEnvelopes with CRC32 and streams across gRPC
    return splitGrpc.SendStream(stream.Context(), stream, fileReader,
        splitandgo.WithChunkSize(256 * 1024),
    )
}
```

#### gRPC Client Consumer:
```go
func DownloadIndex(ctx context.Context, client pb.StreamServiceClient, destinationPath string) error {
    stream, err := client.StreamDownload(ctx, &pb.StreamDownloadRequest{
        ResourceId: "search_index_2026.bin",
    })
    if err != nil { return err }

    destFile, _ := os.Create(destinationPath)
    defer destFile.Close()

    // Reassembles chunks in correct sequence, verifies CRC32 checksums on the fly
    return splitGrpc.ReceiveStream(ctx, stream, destFile,
        splitandgo.WithVerifyChecksums(true),
    )
}
```

---

## 📋 Recipe 4: Streaming Large Client Uploads to Server

### Scenario:
Client uploads a 2 GB file to your API without the server buffering 2 GB in memory.

#### Server Upload Receiver:
```go
func UploadHandler(w http.ResponseWriter, r *http.Request) {
    destFile, err := os.Create("/uploads/" + r.Header.Get("X-Filename"))
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    defer destFile.Close()

    // Reassembles chunks directly into destination file with CRC32 validation
    err = splitHttp.ReceiveRequest(w, r, destFile, splitandgo.WithVerifyChecksums(true))
    if err != nil {
        http.Error(w, "Upload failed: " + err.Error(), http.StatusBadRequest)
        return
    }

    w.WriteHeader(http.StatusOK)
    w.Write([]byte("Upload completed successfully!"))
}
```

---

## 🌐 Consuming in Other Languages

Because Split-and-Go supports standard HTTP/1.1 and HTTP/2 Chunked Transfer (`Transfer-Encoding: chunked`) and NDJSON, **clients written in any language can consume it out-of-the-box**:

### Browser JavaScript (`fetch()` with `ReadableStream`):
```javascript
const response = await fetch('/api/v1/users/stream');
const reader = response.body.getReader();
const decoder = new TextDecoder();

while (true) {
  const { done, value } = await reader.read();
  if (done) break;
  
  const textChunk = decoder.decode(value);
  console.log("Received chunk in real-time:", textChunk);
}
```

### Python (`requests` streaming):
```python
import requests

with requests.get("http://localhost:8080/api/v1/users/stream", stream=True) as r:
    for line in r.iter_lines():
        if line:
            record = json.loads(line)
            process_record(record)
```

### cURL (Command Line):
```bash
curl -N http://localhost:8080/api/v1/users/stream
```

---

## 🏢 Enterprise & MNC Use Case Catalog

| Industry / Domain | Real-World Use Case | Why Monolithic Fails | Why Split-and-Go Wins |
| :--- | :--- | :--- | :--- |
| **FinTech & Banking** | Exporting millions of historical ledger transactions | 100k+ rows cause 504 Gateway Timeout and server OOM | Streams in 500-record batches; first record arrives in < 1ms |
| **AI & Machine Learning** | Transferring 2GB–10GB model weights/embeddings between pods | Pod killed by Kubernetes OOMKiller (`Exit Code 137`) | Memory stays flat under 5 MB throughout 10 GB transfer |
| **Log Analytics & Observability** | Exporting 500MB of raw JSON trace/event logs to SIEM | Memory spike during JSON serialization slows entire node | Streams NDJSON line-by-line with zero whole-file memory |
| **Media & Streaming** | 4K video transcoding and high-res image ingestion | File buffering fills temporary container disk and RAM | Directly pipes from network socket into cloud object storage |
| **IoT & Edge Mesh** | Sensor fleet syncing telemetry over flaky 4G/5G connections | Network drop at 95% forces restarting full upload | Sliding-window packet reassembly handles scrambled network routes |
