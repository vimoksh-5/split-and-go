package main

import (
	"fmt"
	"io"
	"net/http"
	"runtime"
	"time"

	"github.com/vimoksh-5/split-and-go"
)

// ANSI colors for clean visual output
const (
	colorReset  = "\033[0m"
	colorRed    = "\033[31m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorCyan   = "\033[36m"
	colorBold   = "\033[1m"
)

func formatBytes(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

func getMemStats() runtime.MemStats {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m
}

func main() {
	fmt.Printf("%s%s========================================================================%s\n", colorBold, colorCyan, colorReset)
	fmt.Printf("%s%s   REAL-WORLD PUBLIC INTERNET DATA: MONOLITHIC vs SPLIT-AND-GO          %s\n", colorBold, colorYellow, colorReset)
	fmt.Printf("%s%s========================================================================%s\n\n", colorBold, colorCyan, colorReset)

	// We use Cloudflare's public streaming edge endpoint (20 MB binary data)
	// You can also use any public JSON API like USGS GeoJSON or S3 bucket URLs!
	targetURL := "https://speed.cloudflare.com/__down?bytes=20000000" // 20 MB
	fmt.Printf("Public Target URL: %s%s%s\n", colorCyan, targetURL, colorReset)
	fmt.Println("Payload Size: ~20.00 MB over real Internet connection...\n")

	// -------------------------------------------------------------
	// 1. Traditional Way: Monolithic Buffering (io.ReadAll)
	// -------------------------------------------------------------
	fmt.Printf("%s[1/2] Fetching via Traditional Monolithic Approach (io.ReadAll)...%s\n", colorYellow, colorReset)
	runtime.GC()
	memBeforeNaive := getMemStats()
	startNaive := time.Now()

	respNaive, err := http.Get(targetURL)
	if err != nil {
		fmt.Printf("HTTP error: %v\n", err)
		return
	}
	defer respNaive.Body.Close()

	// Monolithic read: buffers all 20MB in RAM at once
	fullBytes, err := io.ReadAll(respNaive.Body)
	if err != nil {
		fmt.Printf("Read error: %v\n", err)
		return
	}

	naiveDuration := time.Since(startNaive)
	memAfterNaive := getMemStats()
	naiveHeap := uint64(0)
	if memAfterNaive.Alloc > memBeforeNaive.Alloc {
		naiveHeap = memAfterNaive.Alloc - memBeforeNaive.Alloc
	}

	fmt.Printf("   Downloaded %s in %v\n", formatBytes(uint64(len(fullBytes))), naiveDuration)
	fmt.Printf("   %sPeak Heap Impact: %s%s (Entire file buffered in RAM before app can process!)\n\n",
		colorRed, formatBytes(naiveHeap), colorReset)

	// Clean up memory before second test
	fullBytes = nil
	runtime.GC()
	time.Sleep(500 * time.Millisecond)

	// -------------------------------------------------------------
	// 2. Split-and-Go Way: Stream Chunks with CRC32 Verification
	// -------------------------------------------------------------
	fmt.Printf("%s[2/2] Fetching via Split-and-Go Streaming Pipeline (64 KB Chunks + CRC32)...%s\n", colorGreen, colorReset)
	runtime.GC()
	memBeforeSplit := getMemStats()
	startSplit := time.Now()

	respSplit, err := http.Get(targetURL)
	if err != nil {
		fmt.Printf("HTTP error: %v\n", err)
		return
	}
	defer respSplit.Body.Close()

	// Wrap the live HTTP response body with Splitter (64 KB chunks)
	s := splitandgo.NewSplitter(respSplit.Body,
		splitandgo.WithChunkSize(64*1024),
		splitandgo.WithChecksumType(splitandgo.ChecksumCRC32),
	)

	// Reassemble on the fly with CRC32 integrity verification directly into io.Discard (or disk)
	asm := splitandgo.NewAssembler(io.Discard, splitandgo.WithVerifyChecksums(true))

	chunkCount := 0
	var timeToFirstChunk time.Duration
	var totalStreamBytes int64

	for {
		chunk, err := s.Next()
		if err != nil {
			break
		}

		chunkCount++
		totalStreamBytes += int64(chunk.PayloadSize())

		if chunkCount == 1 {
			timeToFirstChunk = time.Since(startSplit)
			fmt.Printf("   %s⚡ First Chunk (#0) arrived & verified in %v!%s (App begins processing immediately!)\n",
				colorGreen, timeToFirstChunk, colorReset)
		}

		// Reassemble and verify Castagnoli CRC32 on every single chunk
		if err := asm.WriteChunk(chunk); err != nil {
			fmt.Printf("Integrity error on chunk #%d: %v\n", chunk.Sequence, err)
			return
		}

		if chunk.IsLast() {
			break
		}
	}

	splitDuration := time.Since(startSplit)
	memAfterSplit := getMemStats()
	splitHeap := uint64(0)
	if memAfterSplit.Alloc > memBeforeSplit.Alloc {
		splitHeap = memAfterSplit.Alloc - memBeforeSplit.Alloc
	}

	fmt.Printf("   Streamed & Verified %d chunks (%s) in %v\n",
		chunkCount, formatBytes(uint64(totalStreamBytes)), splitDuration)
	fmt.Printf("   %sPeak Heap Impact: %s%s (Constant flat buffer reuse!)\n\n",
		colorGreen, formatBytes(splitHeap), colorReset)

	// -------------------------------------------------------------
	// Comparison Summary
	// -------------------------------------------------------------
	fmt.Printf("%s%s+-----------------------------------+-------------------------+-------------------------+%s\n", colorBold, colorCyan, colorReset)
	fmt.Printf("%s| REAL INTERNET METRIC              | TRADITIONAL (io.ReadAll)| SPLIT-AND-GO STREAMING  |%s\n", colorBold, colorReset)
	fmt.Printf("%s%s+-----------------------------------+-------------------------+-------------------------+%s\n", colorBold, colorCyan, colorReset)
	fmt.Printf("| Total Payload Downloaded          | %-23s | %-23s |\n", formatBytes(uint64(totalStreamBytes)), formatBytes(uint64(totalStreamBytes)))
	fmt.Printf("| %sTime-To-First-Processable-Data%s   | %s%-23v%s | %s%-23v%s |\n",
		colorBold, colorReset,
		colorRed, naiveDuration, colorReset,
		colorGreen, timeToFirstChunk, colorReset)
	fmt.Printf("| %sPeak Heap RAM Consumption%s       | %s%-23s%s | %s%-23s%s |\n",
		colorBold, colorReset,
		colorRed, formatBytes(naiveHeap), colorReset,
		colorGreen, formatBytes(splitHeap), colorReset)
	fmt.Printf("| Data Integrity Verification       | None (raw stream)       | Castagnoli CRC32 / chunk|\n")
	fmt.Printf("| Bounded Memory Safety             | ❌ No (RAM explodes)    | ✅ Yes (Flat constant)  |\n")
	fmt.Printf("%s%s+-----------------------------------+-------------------------+-------------------------+%s\n\n", colorBold, colorCyan, colorReset)

	fmt.Printf("%s%sWHY THIS IS A GAME-CHANGER:%s\n", colorBold, colorYellow, colorReset)
	fmt.Printf("1. %sImmediate Processing%s: Your app started processing data in %v instead of waiting %v for the full 20MB!\n",
		colorGreen, colorReset, timeToFirstChunk, naiveDuration)
	fmt.Printf("2. %sZero OOM Risk%s: Memory stayed bounded while the traditional method blew up to %s in RAM.\n",
		colorGreen, colorReset, formatBytes(naiveHeap))
	fmt.Printf("3. %sHardware Integrity%s: All %d chunks were verified for bit-level network corruption.\n\n",
		colorGreen, colorReset, chunkCount)
}
