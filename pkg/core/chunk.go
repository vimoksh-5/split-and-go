package core

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// Magic bytes for Split-and-Go binary framing: 'S', 'G', 0x01 (Version 1)
var MagicHeader = [3]byte{'S', 'G', 0x01}

// Common error definitions
var (
	ErrInvalidMagicHeader   = errors.New("splitandgo: invalid magic header")
	ErrUnsupportedVersion   = errors.New("splitandgo: unsupported protocol version")
	ErrCorruptedChunk       = errors.New("splitandgo: chunk data corrupted or truncated")
	ErrSequenceMismatch     = errors.New("splitandgo: chunk sequence mismatch")
	ErrChecksumMismatch     = errors.New("splitandgo: chunk checksum mismatch")
	ErrStreamAlreadyClosed  = errors.New("splitandgo: stream is already closed")
	ErrBufferTooSmall       = errors.New("splitandgo: buffer too small for chunk")
	ErrSessionMismatch      = errors.New("splitandgo: chunk belongs to a different session")
	ErrStreamIncomplete     = errors.New("splitandgo: stream ended before receiving final chunk")
)

// ChecksumType represents the algorithm used to verify chunk data integrity.
type ChecksumType uint8

const (
	ChecksumNone ChecksumType = iota
	ChecksumCRC32
	ChecksumSHA256
)

func (c ChecksumType) String() string {
	switch c {
	case ChecksumNone:
		return "NONE"
	case ChecksumCRC32:
		return "CRC32"
	case ChecksumSHA256:
		return "SHA256"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", c)
	}
}

// ChunkFlags represents bit flags for chunk control and features.
type ChunkFlags uint8

const (
	FlagIsLast ChunkFlags = 1 << iota
	FlagIsCompressed
)

// Chunk represents an atomic unit of data in a split-and-go stream.
type Chunk struct {
	// SessionID is a unique identifier for the streaming session (e.g., UUID).
	SessionID string `json:"session_id"`

	// Sequence is the 0-based index of this chunk in the stream.
	Sequence int64 `json:"sequence"`

	// Offset is the byte offset of this chunk's data relative to the stream beginning.
	Offset int64 `json:"offset"`

	// TotalChunks is the total number of chunks if known in advance, or -1 if indeterminate.
	TotalChunks int64 `json:"total_chunks"`

	// TotalBytes is the total payload size if known in advance, or -1 if indeterminate.
	TotalBytes int64 `json:"total_bytes"`

	// Data holds the raw payload bytes of this chunk.
	Data []byte `json:"data"`

	// Checksum contains the 32-bit CRC32 checksum (or lower 32-bits if applicable).
	Checksum uint32 `json:"checksum"`

	// ChecksumBytes contains optional full checksum bytes (e.g. 32 bytes for SHA256).
	ChecksumBytes []byte `json:"checksum_bytes,omitempty"`

	// ChecksumType identifies the verification algorithm.
	ChecksumType ChecksumType `json:"checksum_type"`

	// Flags encapsulates control bits like IsLast, IsCompressed.
	Flags ChunkFlags `json:"flags"`

	// Metadata contains arbitrary key-value headers (e.g., Content-Type, Trace-ID).
	Metadata map[string]string `json:"metadata,omitempty"`
}

// IsLast returns true if this chunk is the final one in the stream.
func (c *Chunk) IsLast() bool {
	return (c.Flags & FlagIsLast) != 0
}

// SetLast sets or clears the IsLast flag.
func (c *Chunk) SetLast(last bool) {
	if last {
		c.Flags |= FlagIsLast
	} else {
		c.Flags &= ^FlagIsLast
	}
}

// IsCompressed returns true if the chunk payload has been compressed.
func (c *Chunk) IsCompressed() bool {
	return (c.Flags & FlagIsCompressed) != 0
}

// SetCompressed sets or clears the IsCompressed flag.
func (c *Chunk) SetCompressed(compressed bool) {
	if compressed {
		c.Flags |= FlagIsCompressed
	} else {
		c.Flags &= ^FlagIsCompressed
	}
}

// PayloadSize returns the byte length of the chunk's data payload.
func (c *Chunk) PayloadSize() int {
	return len(c.Data)
}

// Clone creates a deep copy of the Chunk.
func (c *Chunk) Clone() *Chunk {
	if c == nil {
		return nil
	}

	clone := *c
	if len(c.Data) > 0 {
		clone.Data = make([]byte, len(c.Data))
		copy(clone.Data, c.Data)
	}
	if len(c.ChecksumBytes) > 0 {
		clone.ChecksumBytes = make([]byte, len(c.ChecksumBytes))
		copy(clone.ChecksumBytes, c.ChecksumBytes)
	}
	if c.Metadata != nil {
		clone.Metadata = make(map[string]string, len(c.Metadata))
		for k, v := range c.Metadata {
			clone.Metadata[k] = v
		}
	}
	return &clone
}

// EncodeBinary serializes the Chunk into a high-efficiency binary frame.
// Wire Format:
// [3 bytes] Magic Header ('S', 'G', 0x01)
// [2 bytes] SessionID length (N)
// [N bytes] SessionID UTF-8
// [8 bytes] Sequence (int64, big-endian)
// [8 bytes] Offset (int64, big-endian)
// [8 bytes] TotalChunks (int64, big-endian)
// [8 bytes] TotalBytes (int64, big-endian)
// [1 byte]  Flags
// [1 byte]  ChecksumType
// [4 bytes] Checksum (uint32, big-endian)
// [2 bytes] ChecksumBytes length (M)
// [M bytes] ChecksumBytes
// [2 bytes] Metadata count (K)
// For each metadata entry:
//   [2 bytes] Key length
//   [Key bytes]
//   [2 bytes] Value length
//   [Value bytes]
// [4 bytes] Data length (L, uint32, big-endian)
// [L bytes] Data bytes
func (c *Chunk) EncodeBinary(w io.Writer) error {
	// Magic header
	if _, err := w.Write(MagicHeader[:]); err != nil {
		return err
	}

	// Session ID
	sessionBytes := []byte(c.SessionID)
	if err := binary.Write(w, binary.BigEndian, uint16(len(sessionBytes))); err != nil {
		return err
	}
	if len(sessionBytes) > 0 {
		if _, err := w.Write(sessionBytes); err != nil {
			return err
		}
	}

	// Numerics
	if err := binary.Write(w, binary.BigEndian, c.Sequence); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, c.Offset); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, c.TotalChunks); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, c.TotalBytes); err != nil {
		return err
	}

	// Flags and Checksum
	if err := binary.Write(w, binary.BigEndian, uint8(c.Flags)); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, uint8(c.ChecksumType)); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, c.Checksum); err != nil {
		return err
	}

	// ChecksumBytes
	if err := binary.Write(w, binary.BigEndian, uint16(len(c.ChecksumBytes))); err != nil {
		return err
	}
	if len(c.ChecksumBytes) > 0 {
		if _, err := w.Write(c.ChecksumBytes); err != nil {
			return err
		}
	}

	// Metadata
	metaCount := uint16(len(c.Metadata))
	if err := binary.Write(w, binary.BigEndian, metaCount); err != nil {
		return err
	}
	for k, v := range c.Metadata {
		kBytes := []byte(k)
		if err := binary.Write(w, binary.BigEndian, uint16(len(kBytes))); err != nil {
			return err
		}
		if _, err := w.Write(kBytes); err != nil {
			return err
		}

		vBytes := []byte(v)
		if err := binary.Write(w, binary.BigEndian, uint16(len(vBytes))); err != nil {
			return err
		}
		if _, err := w.Write(vBytes); err != nil {
			return err
		}
	}

	// Payload data
	payloadLen := uint32(len(c.Data))
	if err := binary.Write(w, binary.BigEndian, payloadLen); err != nil {
		return err
	}
	if payloadLen > 0 {
		if _, err := w.Write(c.Data); err != nil {
			return err
		}
	}

	return nil
}

// DecodeBinary parses a Chunk from an io.Reader containing a binary frame.
func DecodeBinary(r io.Reader) (*Chunk, error) {
	var magic [3]byte
	if _, err := io.ReadFull(r, magic[:]); err != nil {
		return nil, err
	}
	if magic[0] != MagicHeader[0] || magic[1] != MagicHeader[1] {
		return nil, ErrInvalidMagicHeader
	}
	if magic[2] != MagicHeader[2] {
		return nil, ErrUnsupportedVersion
	}

	c := &Chunk{}

	// Session ID
	var sessionLen uint16
	if err := binary.Read(r, binary.BigEndian, &sessionLen); err != nil {
		return nil, err
	}
	if sessionLen > 0 {
		sessionBytes := make([]byte, sessionLen)
		if _, err := io.ReadFull(r, sessionBytes); err != nil {
			return nil, err
		}
		c.SessionID = string(sessionBytes)
	}

	// Numerics
	if err := binary.Read(r, binary.BigEndian, &c.Sequence); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &c.Offset); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &c.TotalChunks); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &c.TotalBytes); err != nil {
		return nil, err
	}

	// Flags and Checksum
	var flags uint8
	if err := binary.Read(r, binary.BigEndian, &flags); err != nil {
		return nil, err
	}
	c.Flags = ChunkFlags(flags)

	var cType uint8
	if err := binary.Read(r, binary.BigEndian, &cType); err != nil {
		return nil, err
	}
	c.ChecksumType = ChecksumType(cType)

	if err := binary.Read(r, binary.BigEndian, &c.Checksum); err != nil {
		return nil, err
	}

	// ChecksumBytes
	var cBytesLen uint16
	if err := binary.Read(r, binary.BigEndian, &cBytesLen); err != nil {
		return nil, err
	}
	if cBytesLen > 0 {
		c.ChecksumBytes = make([]byte, cBytesLen)
		if _, err := io.ReadFull(r, c.ChecksumBytes); err != nil {
			return nil, err
		}
	}

	// Metadata
	var metaCount uint16
	if err := binary.Read(r, binary.BigEndian, &metaCount); err != nil {
		return nil, err
	}
	if metaCount > 0 {
		c.Metadata = make(map[string]string, metaCount)
		for i := uint16(0); i < metaCount; i++ {
			var kLen uint16
			if err := binary.Read(r, binary.BigEndian, &kLen); err != nil {
				return nil, err
			}
			kBytes := make([]byte, kLen)
			if _, err := io.ReadFull(r, kBytes); err != nil {
				return nil, err
			}

			var vLen uint16
			if err := binary.Read(r, binary.BigEndian, &vLen); err != nil {
				return nil, err
			}
			vBytes := make([]byte, vLen)
			if _, err := io.ReadFull(r, vBytes); err != nil {
				return nil, err
			}

			c.Metadata[string(kBytes)] = string(vBytes)
		}
	}

	// Payload data
	var dataLen uint32
	if err := binary.Read(r, binary.BigEndian, &dataLen); err != nil {
		return nil, err
	}
	if dataLen > 0 {
		c.Data = make([]byte, dataLen)
		if _, err := io.ReadFull(r, c.Data); err != nil {
			return nil, err
		}
	}

	return c, nil
}

// MarshalJSON provides custom or default JSON marshaling.
func (c *Chunk) MarshalJSON() ([]byte, error) {
	type Alias Chunk
	return json.Marshal(&struct {
		*Alias
		Flags uint8 `json:"flags"`
	}{
		Alias: (*Alias)(c),
		Flags: uint8(c.Flags),
	})
}

// UnmarshalJSON provides custom JSON unmarshaling.
func (c *Chunk) UnmarshalJSON(data []byte) error {
	type Alias Chunk
	aux := &struct {
		*Alias
		Flags uint8 `json:"flags"`
	}{
		Alias: (*Alias)(c),
	}
	if err := json.Unmarshal(data, aux); err != nil {
		return err
	}
	c.Flags = ChunkFlags(aux.Flags)
	return nil
}

// ToBytes convenience method for binary serialization into a byte slice.
func (c *Chunk) ToBytes() ([]byte, error) {
	var buf bytes.Buffer
	if err := c.EncodeBinary(&buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// FromBytes convenience function for binary deserialization from a byte slice.
func FromBytes(data []byte) (*Chunk, error) {
	return DecodeBinary(bytes.NewReader(data))
}
