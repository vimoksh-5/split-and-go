package core_test

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/vimoksh-5/split-and-go/pkg/core"
)

func TestChunkBinaryRoundtrip(t *testing.T) {
	orig := &core.Chunk{
		SessionID:     "sess-12345-abcde",
		Sequence:      42,
		Offset:        1024 * 42,
		TotalChunks:   100,
		TotalBytes:    1024 * 100,
		Data:          []byte("hello world split-and-go binary chunk test!"),
		Checksum:      123456789,
		ChecksumBytes: []byte{0x01, 0x02, 0x03, 0x04},
		ChecksumType:  core.ChecksumCRC32,
		Flags:         core.FlagIsLast | core.FlagIsCompressed,
		Metadata: map[string]string{
			"content-type": "application/json",
			"trace-id":     "trace-9999",
		},
	}

	var buf bytes.Buffer
	if err := orig.EncodeBinary(&buf); err != nil {
		t.Fatalf("EncodeBinary failed: %v", err)
	}

	decoded, err := core.DecodeBinary(&buf)
	if err != nil {
		t.Fatalf("DecodeBinary failed: %v", err)
	}

	if decoded.SessionID != orig.SessionID {
		t.Errorf("SessionID mismatch: got %v, want %v", decoded.SessionID, orig.SessionID)
	}
	if decoded.Sequence != orig.Sequence {
		t.Errorf("Sequence mismatch: got %v, want %v", decoded.Sequence, orig.Sequence)
	}
	if decoded.Offset != orig.Offset {
		t.Errorf("Offset mismatch: got %v, want %v", decoded.Offset, orig.Offset)
	}
	if decoded.TotalChunks != orig.TotalChunks {
		t.Errorf("TotalChunks mismatch: got %v, want %v", decoded.TotalChunks, orig.TotalChunks)
	}
	if decoded.TotalBytes != orig.TotalBytes {
		t.Errorf("TotalBytes mismatch: got %v, want %v", decoded.TotalBytes, orig.TotalBytes)
	}
	if !bytes.Equal(decoded.Data, orig.Data) {
		t.Errorf("Data mismatch: got %s, want %s", decoded.Data, orig.Data)
	}
	if decoded.Checksum != orig.Checksum {
		t.Errorf("Checksum mismatch: got %v, want %v", decoded.Checksum, orig.Checksum)
	}
	if !bytes.Equal(decoded.ChecksumBytes, orig.ChecksumBytes) {
		t.Errorf("ChecksumBytes mismatch")
	}
	if decoded.ChecksumType != orig.ChecksumType {
		t.Errorf("ChecksumType mismatch")
	}
	if !decoded.IsLast() {
		t.Errorf("expected IsLast to be true")
	}
	if !decoded.IsCompressed() {
		t.Errorf("expected IsCompressed to be true")
	}
	if decoded.Metadata["content-type"] != "application/json" {
		t.Errorf("Metadata mismatch")
	}
}

func TestChunkJSONRoundtrip(t *testing.T) {
	orig := &core.Chunk{
		SessionID: "sess-json",
		Sequence:  1,
		Data:      []byte("json payload"),
		Flags:     core.FlagIsLast,
	}

	data, err := json.Marshal(orig)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	var decoded core.Chunk
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if !decoded.IsLast() {
		t.Errorf("expected IsLast true")
	}
	if string(decoded.Data) != "json payload" {
		t.Errorf("got %s, want json payload", decoded.Data)
	}
}

func TestChunkClone(t *testing.T) {
	orig := &core.Chunk{
		SessionID: "s1",
		Data:      []byte("abc"),
		Metadata:  map[string]string{"k": "v"},
	}

	cloned := orig.Clone()
	cloned.Data[0] = 'z'
	cloned.Metadata["k"] = "modified"

	if orig.Data[0] != 'a' {
		t.Errorf("clone modified original data")
	}
	if orig.Metadata["k"] != "v" {
		t.Errorf("clone modified original metadata")
	}
}
