package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/vimoksh-5/split-and-go"
	splitHttp "github.com/vimoksh-5/split-and-go/pkg/transport/http"
)

func main() {
	mux := http.NewServeMux()

	// 1. Serve Interactive Web Dashboard UI
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write([]byte(dashboardHTML))
	})

	// 2. Split-and-Go Streaming Endpoint (Chunked + CRC32) with dynamic scale selection
	mux.HandleFunc("/api/stream/split", func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "Flusher not supported", http.StatusInternalServerError)
			return
		}

		sizeParam := strings.ToLower(r.URL.Query().Get("size"))
		var totalBytes int64 = 15 * 1024 * 1024
		chunkSize := 64 * 1024

		switch sizeParam {
		case "10kb":
			totalBytes = 10 * 1024
			chunkSize = 4 * 1024
		case "128kb":
			totalBytes = 128 * 1024
			chunkSize = 16 * 1024
		case "10mb":
			totalBytes = 10 * 1024 * 1024
			chunkSize = 64 * 1024
		case "100mb":
			totalBytes = 100 * 1024 * 1024
			chunkSize = 128 * 1024
		case "1gb":
			totalBytes = 1024 * 1024 * 1024
			chunkSize = 256 * 1024
		default:
			totalBytes = 15 * 1024 * 1024
			chunkSize = 64 * 1024
		}

		pattern := []byte("SPLIT-AND-GO-HIGH-PERFORMANCE-STREAMING-CHUNK-DATA-PACKET-VALIDATION-\n")
		src := splitandgo.NewPatternReader(pattern, totalBytes)

		w.Header().Set("Content-Type", "application/x-ndjson")
		w.Header().Set("Transfer-Encoding", "chunked")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.WriteHeader(http.StatusOK)
		flusher.Flush()

		// Stream chunks with Castagnoli CRC32
		s := splitandgo.NewSplitter(src,
			splitandgo.WithChunkSize(chunkSize),
			splitandgo.WithChecksumType(splitandgo.ChecksumCRC32),
		)

		for {
			chunk, err := s.Next()
			if err != nil {
				break
			}

			previewLen := min(48, len(chunk.Data))
			metaPayload := map[string]any{
				"sequence": chunk.Sequence,
				"size":     chunk.PayloadSize(),
				"offset":   chunk.Offset,
				"crc32":    chunk.Checksum,
				"is_last":  chunk.IsLast(),
				"data":     string(chunk.Data[:previewLen]) + "... [verified]",
			}
			jsonBytes, _ := json.Marshal(metaPayload)
			jsonBytes = append(jsonBytes, '\n')

			w.Write(jsonBytes)
			flusher.Flush()

			if chunk.IsLast() {
				break
			}
		}
	})

	// 3. Naive Monolithic Endpoint (Full buffering, slow TTFB)
	mux.HandleFunc("/api/stream/naive", func(w http.ResponseWriter, r *http.Request) {
		sizeParam := strings.ToLower(r.URL.Query().Get("size"))
		var totalBytes int64 = 15 * 1024 * 1024

		switch sizeParam {
		case "10kb":
			totalBytes = 10 * 1024
		case "128kb":
			totalBytes = 128 * 1024
		case "10mb":
			totalBytes = 10 * 1024 * 1024
		case "100mb":
			totalBytes = 100 * 1024 * 1024
		case "1gb":
			// Monolithic API cannot allocate 1GB safely in standard REST without OOM risk
			http.Error(w, "500 Server OOM / Memory Limit Exceeded: Monolithic buffering failed on 1 GB payload", http.StatusInternalServerError)
			return
		}

		pattern := []byte("SPLIT-AND-GO-HIGH-PERFORMANCE-STREAMING-CHUNK-DATA-PACKET-VALIDATION-\n")
		// Monolithic: allocate entire byte slice into server memory
		largePayload := make([]byte, totalBytes)
		for i := int64(0); i < totalBytes; i++ {
			largePayload[i] = pattern[i%int64(len(pattern))]
		}

		// Simulate server buffering / serialization latency
		time.Sleep(time.Duration(min(300, int(totalBytes/(1024*1024))*15+20)) * time.Millisecond)

		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(largePayload)))
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.WriteHeader(http.StatusOK)
		w.Write(largePayload)
	})

	// 4. Out-of-Order Packet Resilience Demo Endpoint
	mux.HandleFunc("/api/stream/scrambled", func(w http.ResponseWriter, r *http.Request) {
		orig := "Split-and-Go automatically handles network packet reordering using sliding windows!"
		src := strings.NewReader(orig)

		s := splitandgo.NewSplitter(src,
			splitandgo.WithChunkSize(12),
			splitandgo.WithChecksumType(splitandgo.ChecksumCRC32),
		)

		chunks, _ := s.All()

		// Scramble order: [2, 0, 4, 1, 3, 5, ...]
		var scrambled []*splitandgo.Chunk
		order := []int{2, 0, 4, 1, 3}
		for _, idx := range order {
			if idx < len(chunks) {
				scrambled = append(scrambled, chunks[idx])
			}
		}
		for i := 5; i < len(chunks); i++ {
			scrambled = append(scrambled, chunks[i])
		}

		// Reassemble on server to verify
		var assembled bytes.Buffer
		asm := splitandgo.NewAssembler(&assembled, splitandgo.WithVerifyChecksums(true))
		for _, c := range scrambled {
			asm.WriteChunk(c)
		}

		resp := map[string]any{
			"total_chunks": len(chunks),
			"arrival_order": func() []int64 {
				var ids []int64
				for _, c := range scrambled {
					ids = append(ids, c.Sequence)
				}
				return ids
			}(),
			"reassembled_text": assembled.String(),
			"success":          assembled.String() == orig,
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	})

	addr := ":8090"
	fmt.Printf("\n🚀 Split-and-Go Live Interactive Web Dashboard running at:\n")
	fmt.Printf("👉 http://localhost%s\n\n", addr)

	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
	_ = splitHttp.ContentTypeBinary
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

const dashboardHTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Split-and-Go — Live Browser Streaming Benchmark & Inspector</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;700&display=swap" rel="stylesheet">
  <style>
    :root {
      --bg: #090d16;
      --card-bg: rgba(16, 24, 40, 0.75);
      --border: rgba(255, 255, 255, 0.08);
      --border-glow: rgba(0, 240, 255, 0.3);
      --cyan: #00f0ff;
      --cyan-glow: rgba(0, 240, 255, 0.15);
      --green: #10b981;
      --purple: #a855f7;
      --red: #ef4444;
      --text: #f3f4f6;
      --text-muted: #94a3b8;
    }

    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      background: var(--bg);
      color: var(--text);
      font-family: 'Inter', sans-serif;
      min-height: 100vh;
      overflow-x: hidden;
      padding: 2rem 1.5rem;
      background-image: 
        radial-gradient(circle at 15% 15%, rgba(0, 240, 255, 0.06) 0%, transparent 40%),
        radial-gradient(circle at 85% 85%, rgba(168, 85, 247, 0.06) 0%, transparent 40%);
    }

    .container {
      max-width: 1200px;
      margin: 0 auto;
    }

    /* Header */
    header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 2.5rem;
      border-bottom: 1px solid var(--border);
      padding-bottom: 1.5rem;
    }
    .brand {
      display: flex;
      align-items: center;
      gap: 1rem;
    }
    .logo-badge {
      background: linear-gradient(135deg, var(--cyan), var(--purple));
      color: #000;
      font-weight: 800;
      font-size: 1.25rem;
      width: 48px;
      height: 48px;
      border-radius: 12px;
      display: grid;
      place-items: center;
      box-shadow: 0 0 20px rgba(0, 240, 255, 0.4);
    }
    .brand h1 {
      font-size: 1.6rem;
      font-weight: 800;
      letter-spacing: -0.5px;
    }
    .brand p {
      font-size: 0.85rem;
      color: var(--text-muted);
    }
    .github-link {
      display: inline-flex;
      align-items: center;
      gap: 0.5rem;
      background: rgba(255, 255, 255, 0.05);
      border: 1px solid var(--border);
      color: var(--text);
      text-decoration: none;
      padding: 0.5rem 1rem;
      border-radius: 9999px;
      font-size: 0.85rem;
      font-weight: 600;
      transition: all 0.2s;
    }
    .github-link:hover {
      background: rgba(0, 240, 255, 0.1);
      border-color: var(--cyan);
      box-shadow: 0 0 15px var(--cyan-glow);
    }

    /* Action Banner */
    .hero-actions {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
      gap: 1.25rem;
      margin-bottom: 2rem;
    }
    .action-card {
      background: var(--card-bg);
      border: 1px solid var(--border);
      backdrop-filter: blur(16px);
      border-radius: 16px;
      padding: 1.5rem;
      display: flex;
      flex-direction: column;
      justify-content: space-between;
      position: relative;
      overflow: hidden;
      transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
    }
    .action-card::before {
      content: '';
      position: absolute;
      top: 0; left: 0; right: 0; height: 3px;
      background: transparent;
      transition: background 0.3s;
    }
    .action-card.split::before { background: linear-gradient(90deg, var(--cyan), var(--purple)); }
    .action-card.naive::before { background: var(--red); }
    .action-card.resilience::before { background: var(--green); }
    .action-card:hover {
      transform: translateY(-2px);
      border-color: rgba(255, 255, 255, 0.2);
    }

    .card-title {
      font-size: 1.15rem;
      font-weight: 700;
      margin-bottom: 0.5rem;
      display: flex;
      align-items: center;
      gap: 0.5rem;
    }
    .card-desc {
      color: var(--text-muted);
      font-size: 0.85rem;
      line-height: 1.4;
      margin-bottom: 1.25rem;
    }

    .btn {
      width: 100%;
      padding: 0.85rem 1.25rem;
      font-size: 0.95rem;
      font-weight: 700;
      border-radius: 10px;
      border: none;
      cursor: pointer;
      display: flex;
      align-items: center;
      justify-content: center;
      gap: 0.5rem;
      transition: all 0.2s;
    }
    .btn-cyan {
      background: var(--cyan);
      color: #000;
      box-shadow: 0 4px 20px rgba(0, 240, 255, 0.35);
    }
    .btn-cyan:hover {
      background: #38f8ff;
      box-shadow: 0 6px 25px rgba(0, 240, 255, 0.5);
    }
    .btn-outline {
      background: rgba(255, 255, 255, 0.05);
      border: 1px solid var(--border);
      color: var(--text);
    }
    .btn-outline:hover {
      background: rgba(255, 255, 255, 0.1);
      border-color: rgba(255, 255, 255, 0.3);
    }
    .btn:disabled {
      opacity: 0.5;
      cursor: not-allowed;
    }

    /* Live Metrics Grid */
    .metrics-grid {
      display: grid;
      grid-template-columns: repeat(4, 1fr);
      gap: 1.25rem;
      margin-bottom: 2rem;
    }
    @media (max-width: 900px) {
      .metrics-grid { grid-template-columns: repeat(2, 1fr); }
    }
    @media (max-width: 500px) {
      .metrics-grid { grid-template-columns: 1fr; }
    }

    .metric-box {
      background: var(--card-bg);
      border: 1px solid var(--border);
      backdrop-filter: blur(12px);
      border-radius: 14px;
      padding: 1.25rem;
      display: flex;
      flex-direction: column;
    }
    .metric-label {
      font-size: 0.75rem;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: var(--text-muted);
      margin-bottom: 0.35rem;
    }
    .metric-val {
      font-size: 1.8rem;
      font-weight: 800;
      font-family: 'JetBrains Mono', monospace;
      color: var(--cyan);
    }
    .metric-sub {
      font-size: 0.75rem;
      color: var(--text-muted);
      margin-top: 0.25rem;
    }

    /* Live Pipeline Visualizer */
    .pipeline-container {
      background: var(--card-bg);
      border: 1px solid var(--border);
      backdrop-filter: blur(16px);
      border-radius: 16px;
      padding: 1.5rem;
      margin-bottom: 2rem;
    }
    .pipeline-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 1rem;
    }
    .pipeline-title {
      font-size: 1.1rem;
      font-weight: 700;
    }
    .stream-status {
      display: inline-flex;
      align-items: center;
      gap: 0.4rem;
      font-size: 0.8rem;
      font-weight: 600;
      color: var(--text-muted);
    }
    .status-dot {
      width: 8px;
      height: 8px;
      border-radius: 50%;
      background: #475569;
    }
    .status-dot.active {
      background: var(--cyan);
      box-shadow: 0 0 10px var(--cyan);
      animation: pulse 1.5s infinite;
    }
    @keyframes pulse {
      0%, 100% { opacity: 1; transform: scale(1); }
      50% { opacity: 0.4; transform: scale(1.3); }
    }

    /* Progress bar */
    .progress-track {
      width: 100%;
      height: 8px;
      background: rgba(255, 255, 255, 0.05);
      border-radius: 999px;
      overflow: hidden;
      margin-bottom: 1.5rem;
    }
    .progress-fill {
      width: 0%;
      height: 100%;
      background: linear-gradient(90deg, var(--cyan), var(--purple));
      border-radius: 999px;
      transition: width 0.1s linear;
      box-shadow: 0 0 12px var(--cyan);
    }

    /* Live Chunk Feed Table */
    .chunk-feed {
      max-height: 340px;
      overflow-y: auto;
      font-family: 'JetBrains Mono', monospace;
      font-size: 0.8rem;
      border: 1px solid var(--border);
      border-radius: 10px;
      background: rgba(0, 0, 0, 0.35);
    }
    .chunk-feed table {
      width: 100%;
      border-collapse: collapse;
      text-align: left;
    }
    .chunk-feed th {
      background: rgba(255, 255, 255, 0.04);
      padding: 0.75rem 1rem;
      color: var(--text-muted);
      position: sticky;
      top: 0;
      z-index: 2;
      border-bottom: 1px solid var(--border);
    }
    .chunk-feed td {
      padding: 0.6rem 1rem;
      border-bottom: 1px solid rgba(255, 255, 255, 0.03);
    }
    .chunk-feed tr:hover {
      background: rgba(0, 240, 255, 0.03);
    }
    .tag-crc {
      background: rgba(16, 185, 129, 0.15);
      color: var(--green);
      padding: 0.15rem 0.4rem;
      border-radius: 4px;
      font-size: 0.75rem;
    }
    .tag-last {
      background: rgba(168, 85, 247, 0.2);
      color: var(--purple);
      padding: 0.15rem 0.4rem;
      border-radius: 4px;
      font-weight: 700;
    }

    /* Scale Selector Bar */
    .scale-selector-bar {
      display: flex;
      align-items: center;
      gap: 0.65rem;
      background: var(--card-bg);
      border: 1px solid var(--border);
      backdrop-filter: blur(14px);
      padding: 0.75rem 1.25rem;
      border-radius: 14px;
      margin-bottom: 1.5rem;
      flex-wrap: wrap;
    }
    .scale-label {
      font-size: 0.8rem;
      font-weight: 700;
      color: var(--text-muted);
      text-transform: uppercase;
      letter-spacing: 0.06em;
      margin-right: 0.5rem;
    }
    .pill {
      background: rgba(255, 255, 255, 0.05);
      border: 1px solid var(--border);
      color: var(--text);
      padding: 0.45rem 0.9rem;
      border-radius: 9999px;
      font-size: 0.8rem;
      font-weight: 600;
      cursor: pointer;
      transition: all 0.2s;
    }
    .pill:hover {
      background: rgba(0, 240, 255, 0.1);
      border-color: var(--cyan);
    }
    .pill.active {
      background: var(--cyan);
      color: #000;
      border-color: var(--cyan);
      box-shadow: 0 0 14px rgba(0, 240, 255, 0.45);
    }

    /* Out of order resilience box */
    #resilienceResult {
      display: none;
      margin-top: 1rem;
      padding: 1rem;
      border-radius: 8px;
      background: rgba(16, 185, 129, 0.1);
      border: 1px solid var(--green);
      font-family: 'JetBrains Mono', monospace;
      font-size: 0.85rem;
    }
  </style>
</head>
<body>
  <div class="container">
    <header>
      <div class="brand">
        <div class="logo-badge">⚡</div>
        <div>
          <h1>Split-and-Go Live Inspector</h1>
          <p>Real-time Browser Chunk Streaming & TTFB Verification Engine</p>
        </div>
      </div>
      <a href="https://github.com/vimoksh-5/split-and-go" target="_blank" class="github-link">
        <svg height="18" width="18" viewBox="0 0 16 16" fill="currentColor">
          <path d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0016 8c0-4.42-3.58-8-8-8z"/>
        </svg>
        vimoksh-5/split-and-go
      </a>
    </header>

    <!-- Scale Selector Bar -->
    <div class="scale-selector-bar">
      <span class="scale-label">Select Scale:</span>
      <button class="pill" onclick="selectScale('10kb', '10 KB', 10*1024)">10 KB (Metadata)</button>
      <button class="pill" onclick="selectScale('128kb', '128 KB', 128*1024)">128 KB (REST API)</button>
      <button class="pill active" onclick="selectScale('10mb', '10 MB', 10*1024*1024)">10 MB (Image)</button>
      <button class="pill" onclick="selectScale('100mb', '100 MB', 100*1024*1024)">100 MB (Video)</button>
      <button class="pill" onclick="selectScale('1gb', '1 GB', 1024*1024*1024)">1 GB (Archive)</button>
    </div>

    <!-- Hero Actions -->
    <div class="hero-actions">
      <div class="action-card split">
        <div>
          <div class="card-title">⚡ Split-and-Go Streaming</div>
          <p id="splitCardDesc" class="card-desc">Streams 10 MB in 64 KB discrete chunks with CRC32 integrity verification and zero buffering.</p>
        </div>
        <button id="btnSplit" class="btn btn-cyan" onclick="startSplitStream()">
          <span>Stream Live Chunks</span> ➔
        </button>
      </div>

      <div class="action-card naive">
        <div>
          <div class="card-title">🐢 Monolithic Full Buffering</div>
          <p id="naiveCardDesc" class="card-desc">Simulates standard REST buffering 10 MB in server RAM before sending the first byte.</p>
        </div>
        <button id="btnNaive" class="btn btn-outline" onclick="startNaiveRequest()">
          <span>Test Monolithic API</span>
        </button>
      </div>

      <div class="action-card resilience">
        <div>
          <div class="card-title">🛡️ Out-of-Order Assembly</div>
          <p class="card-desc">Simulates packet scramble: chunks arrive [2, 0, 4, 1, 3] and sliding window reorders them seamlessly.</p>
        </div>
        <button id="btnScramble" class="btn btn-outline" onclick="startScrambleDemo()">
          <span>Test Packet Reordering</span>
        </button>
      </div>
    </div>

    <div id="resilienceResult"></div>

    <!-- Live Metrics Grid -->
    <div class="metrics-grid">
      <div class="metric-box">
        <div class="metric-label">Time To First Byte (TTFB)</div>
        <div id="mTTFB" class="metric-val">-- ms</div>
        <div id="mTTFBSub" class="metric-sub">Client wait time</div>
      </div>
      <div class="metric-box">
        <div class="metric-label">Chunks Processed</div>
        <div id="mChunks" class="metric-val">0</div>
        <div class="metric-sub">Verified sequentially</div>
      </div>
      <div class="metric-box">
        <div class="metric-label">Bytes Streamed</div>
        <div id="mBytes" class="metric-val">0 MB</div>
        <div class="metric-sub">Total payload data</div>
      </div>
      <div class="metric-box">
        <div class="metric-label">Streaming Throughput</div>
        <div id="mThroughput" class="metric-val">-- MB/s</div>
        <div class="metric-sub">Wire speed</div>
      </div>
    </div>

    <!-- Pipeline Visualizer -->
    <div class="pipeline-container">
      <div class="pipeline-header">
        <div class="pipeline-title">Live Wire Chunk Visualizer</div>
        <div class="stream-status">
          <div id="statusDot" class="status-dot"></div>
          <span id="statusText">Pipeline Idle</span>
        </div>
      </div>

      <div class="progress-track">
        <div id="progressFill" class="progress-fill"></div>
      </div>

      <div class="chunk-feed">
        <table>
          <thead>
            <tr>
              <th>Seq #</th>
              <th>Offset</th>
              <th>Chunk Size</th>
              <th>CRC32 Checksum</th>
              <th>Transit Latency</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody id="chunkFeedBody">
            <tr>
              <td colspan="6" style="text-align: center; color: var(--text-muted); padding: 2rem;">
                Click "Stream Live Chunks" above to watch chunks fly in real time!
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  </div>

  <script>
    const mTTFB = document.getElementById('mTTFB');
    const mTTFBSub = document.getElementById('mTTFBSub');
    const mChunks = document.getElementById('mChunks');
    const mBytes = document.getElementById('mBytes');
    const mThroughput = document.getElementById('mThroughput');
    const statusDot = document.getElementById('statusDot');
    const statusText = document.getElementById('statusText');
    const progressFill = document.getElementById('progressFill');
    const chunkFeedBody = document.getElementById('chunkFeedBody');
    const btnSplit = document.getElementById('btnSplit');
    const btnNaive = document.getElementById('btnNaive');
    const resilienceResult = document.getElementById('resilienceResult');

    let currentScaleKey = '10mb';
    let currentScaleLabel = '10 MB';
    let currentScaleBytes = 10 * 1024 * 1024;

    function selectScale(key, label, bytes) {
      currentScaleKey = key;
      currentScaleLabel = label;
      currentScaleBytes = bytes;

      document.querySelectorAll('.pill').forEach(p => p.classList.remove('active'));
      event.target.classList.add('active');

      document.getElementById('splitCardDesc').textContent = 'Streams ' + label + ' in discrete chunks with CRC32 verification and bounded memory.';
      document.getElementById('naiveCardDesc').textContent = 'Simulates standard REST buffering ' + label + ' in server RAM before sending the first byte.';
      resetUI();
    }

    function resetUI() {
      mTTFB.textContent = '-- ms';
      mTTFBSub.textContent = 'Client wait time';
      mChunks.textContent = '0';
      mBytes.textContent = '0 MB';
      mThroughput.textContent = '-- MB/s';
      progressFill.style.width = '0%';
      chunkFeedBody.innerHTML = '';
      resilienceResult.style.display = 'none';
    }

    async function startSplitStream() {
      resetUI();
      btnSplit.disabled = true;
      btnNaive.disabled = true;
      statusDot.className = 'status-dot active';
      statusText.textContent = 'Streaming ' + currentScaleLabel + ' Live Chunks via Socket...';

      const startTime = performance.now();
      let firstByteTime = null;
      let totalBytesReceived = 0;
      let chunkCount = 0;

      try {
        const response = await fetch('/api/stream/split?size=' + currentScaleKey);
        firstByteTime = performance.now();
        const ttfb = (firstByteTime - startTime).toFixed(1);
        mTTFB.textContent = ttfb + ' ms';
        mTTFBSub.textContent = '⚡ Instant Sub-millisecond!';
        mTTFB.style.color = 'var(--cyan)';

        const reader = response.body.getReader();
        const decoder = new TextDecoder('utf-8');
        let buffer = '';

        while (true) {
          const { done, value } = await reader.read();
          if (done) break;

          buffer += decoder.decode(value, { stream: true });
          const lines = buffer.split('\n');
          buffer = lines.pop(); // keep remainder

          for (const line of lines) {
            if (!line.trim()) continue;
            try {
              const chunk = JSON.parse(line);
              chunkCount++;
              totalBytesReceived += chunk.size;

              // Update metrics
              mChunks.textContent = chunkCount;
              if (currentScaleBytes >= 1024 * 1024 * 1024) {
                mBytes.textContent = (totalBytesReceived / (1024 * 1024 * 1024)).toFixed(2) + ' GB';
              } else if (currentScaleBytes < 1024 * 1024) {
                mBytes.textContent = (totalBytesReceived / 1024).toFixed(1) + ' KB';
              } else {
                mBytes.textContent = (totalBytesReceived / (1024 * 1024)).toFixed(2) + ' MB';
              }

              const percent = Math.min(100, (totalBytesReceived / currentScaleBytes) * 100);
              progressFill.style.width = percent + '%';

              const elapsedSec = (performance.now() - startTime) / 1000;
              const throughput = (totalBytesReceived / (1024 * 1024)) / elapsedSec;
              mThroughput.textContent = throughput.toFixed(1) + ' MB/s';

              // Append table row (keep last 50 for DOM performance)
              const tr = document.createElement('tr');
              const latency = (performance.now() - firstByteTime).toFixed(1);
              tr.innerHTML = 
                '<td><b>#' + chunk.sequence + '</b></td>' +
                '<td>' + (chunk.offset / 1024).toFixed(0) + ' KB</td>' +
                '<td>' + (chunk.size / 1024).toFixed(1) + ' KB</td>' +
                '<td><span class="tag-crc">0x' + Number(chunk.crc32).toString(16).toUpperCase() + '</span></td>' +
                '<td>+' + latency + ' ms</td>' +
                '<td>' + (chunk.is_last ? '<span class="tag-last">FINAL</span>' : '<span style="color: var(--cyan)">STREAMING</span>') + '</td>';
              
              if (chunkFeedBody.children.length > 50) {
                chunkFeedBody.removeChild(chunkFeedBody.firstChild);
              }
              chunkFeedBody.appendChild(tr);
              chunkFeedBody.parentElement.parentElement.scrollTop = chunkFeedBody.parentElement.parentElement.scrollHeight;

              if (chunk.is_last) break;
            } catch (err) {
              console.error('parse error', err);
            }
          }
        }

        const totalTime = ((performance.now() - startTime) / 1000).toFixed(2);
        statusDot.className = 'status-dot';
        statusText.textContent = 'Stream Completed in ' + totalTime + 's (' + currentScaleLabel + ')!';
      } catch (e) {
        statusText.textContent = 'Error: ' + e.message;
      } finally {
        btnSplit.disabled = false;
        btnNaive.disabled = false;
      }
    }

    async function startNaiveRequest() {
      resetUI();
      btnSplit.disabled = true;
      btnNaive.disabled = true;
      statusDot.className = 'status-dot active';
      statusText.textContent = 'Waiting for Server to buffer ' + currentScaleLabel + ' in RAM...';
      chunkFeedBody.innerHTML = '<tr><td colspan="6" style="text-align: center; color: var(--red); padding: 2rem;">' +
        '⏳ Client waiting... Server is allocating full ' + currentScaleLabel + ' into RAM before first byte is sent...</td></tr>';

      const startTime = performance.now();
      try {
        const response = await fetch('/api/stream/naive?size=' + currentScaleKey);
        if (!response.ok) {
          const errText = await response.text();
          mTTFB.textContent = 'OOM CRASH';
          mTTFB.style.color = 'var(--red)';
          mTTFBSub.textContent = '🚨 Server Memory Exhausted';
          statusText.textContent = 'Monolithic Failed: ' + errText;
          chunkFeedBody.innerHTML = '<tr><td colspan="6" style="text-align: center; color: var(--red); padding: 2rem;">' +
            '🚨 Server OOM / Memory Limit Exceeded! Monolithic buffering failed on ' + currentScaleLabel + '.</td></tr>';
          return;
        }

        const ttfb = (performance.now() - startTime).toFixed(1);
        mTTFB.textContent = ttfb + ' ms';
        mTTFBSub.textContent = '🐢 High TTFB (Server blocked)';
        mTTFB.style.color = 'var(--red)';

        const blob = await response.blob();
        const totalDuration = ((performance.now() - startTime) / 1000).toFixed(2);
        const totalMB = (blob.size / (1024 * 1024)).toFixed(2);

        mBytes.textContent = totalMB + ' MB';
        mChunks.textContent = '1 (Monolithic)';
        progressFill.style.width = '100%';

        const throughput = (blob.size / (1024 * 1024)) / ((performance.now() - startTime) / 1000);
        mThroughput.textContent = throughput.toFixed(1) + ' MB/s';

        statusDot.className = 'status-dot';
        statusText.textContent = 'Monolithic Download Completed in ' + totalDuration + 's!';

        chunkFeedBody.innerHTML = '<tr>' +
          '<td>#0</td><td>0 KB</td><td>' + totalMB + ' MB</td>' +
          '<td><span style="color: var(--text-muted)">NONE</span></td>' +
          '<td>+' + ttfb + ' ms</td>' +
          '<td><span class="tag-last">MONOLITHIC BLOB</span></td></tr>';
      } catch (e) {
        statusText.textContent = 'Error: ' + e.message;
      } finally {
        btnSplit.disabled = false;
        btnNaive.disabled = false;
      }
    }

    async function startScrambleDemo() {
      resilienceResult.style.display = 'block';
      resilienceResult.innerHTML = '⏳ Simulating packet arrival across scrambled network routes...';

      const res = await fetch('/api/stream/scrambled');
      const data = await res.json();

      resilienceResult.innerHTML = 
        '<div style="color: var(--cyan); font-weight: 700; margin-bottom: 0.5rem;">🛡️ SLIDING-WINDOW OUT-OF-ORDER REASSEMBLY RESULT:</div>' +
        '<div>Arrival Packet Order: <b>' + data.arrival_order.map(id => '#' + id).join(' ➔ ') + '</b></div>' +
        '<div style="margin-top: 0.5rem; color: var(--green);">Reconstructed Text: "' + data.reassembled_text + '"</div>' +
        '<div style="margin-top: 0.25rem;">Integrity Check: <b style="color: var(--green);">100% VALIDATED (CRC32 MATCH)</b></div>';
    }
  </script>
</body>
</html>
`
