package grpc_test

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/vimoksh-5/split-and-go/pkg/core"
	"github.com/vimoksh-5/split-and-go/pkg/splitter"
	splitGrpc "github.com/vimoksh-5/split-and-go/pkg/transport/grpc"
	"github.com/vimoksh-5/split-and-go/pkg/transport/grpc/pb"
)

// mockGRPCStream implements both StreamSender and StreamReceiver in-memory
type mockGRPCStream struct {
	ch     chan *pb.ChunkEnvelope
	closed bool
	mu     sync.Mutex
}

func newMockGRPCStream(buffer int) *mockGRPCStream {
	return &mockGRPCStream{
		ch: make(chan *pb.ChunkEnvelope, buffer),
	}
}

func (m *mockGRPCStream) Send(env *pb.ChunkEnvelope) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return io.ErrClosedPipe
	}
	m.ch <- env
	if env.IsLast {
		m.closed = true
		close(m.ch)
	}
	return nil
}

func (m *mockGRPCStream) Recv() (*pb.ChunkEnvelope, error) {
	env, ok := <-m.ch
	if !ok {
		return nil, io.EOF
	}
	return env, nil
}

func TestProtoConversionRoundtrip(t *testing.T) {
	orig := &core.Chunk{
		SessionID:     "grpc-sess-1",
		Sequence:      10,
		Offset:        2048,
		TotalChunks:   20,
		TotalBytes:    40960,
		Data:          []byte("testing grpc protobuf chunk envelope serialization"),
		Checksum:      987654321,
		ChecksumBytes: []byte{0xDE, 0xAD, 0xBE, 0xEF},
		ChecksumType:  core.ChecksumCRC32,
		Flags:         core.FlagIsLast | core.FlagIsCompressed,
		Metadata: map[string]string{
			"grpc-metadata": "enabled",
		},
	}

	protoMsg := splitGrpc.ChunkToProto(orig)
	back := splitGrpc.ProtoToChunk(protoMsg)

	if back.SessionID != orig.SessionID {
		t.Errorf("SessionID mismatch")
	}
	if back.Sequence != orig.Sequence {
		t.Errorf("Sequence mismatch")
	}
	if !bytes.Equal(back.Data, orig.Data) {
		t.Errorf("Data mismatch")
	}
	if back.Checksum != orig.Checksum {
		t.Errorf("Checksum mismatch")
	}
	if !back.IsLast() {
		t.Errorf("expected IsLast true")
	}
	if !back.IsCompressed() {
		t.Errorf("expected IsCompressed true")
	}
	if back.Metadata["grpc-metadata"] != "enabled" {
		t.Errorf("metadata mismatch")
	}
}

func TestSendAndReceiveStream(t *testing.T) {
	payload := bytes.Repeat([]byte("Enterprise gRPC stream chunking with Split-and-Go! "), 100)
	stream := newMockGRPCStream(16)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	var recvErr error
	var assembled bytes.Buffer

	// Start Receiver in goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		recvErr = splitGrpc.ReceiveStream(ctx, stream, &assembled)
	}()

	// Send from main goroutine
	sendErr := splitGrpc.SendStream(ctx, stream, bytes.NewReader(payload), splitter.WithChunkSize(128))
	if sendErr != nil {
		t.Fatalf("SendStream failed: %v", sendErr)
	}

	wg.Wait()

	if recvErr != nil {
		t.Fatalf("ReceiveStream failed: %v", recvErr)
	}

	if !bytes.Equal(assembled.Bytes(), payload) {
		t.Fatalf("assembled payload mismatch: got %d bytes, want %d bytes", assembled.Len(), len(payload))
	}
}
