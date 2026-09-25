package grpc

import (
	"context"
	"errors"
	"io"

	"github.com/vimoksh-5/split-and-go/pkg/assembler"
	"github.com/vimoksh-5/split-and-go/pkg/core"
	"github.com/vimoksh-5/split-and-go/pkg/splitter"
	"github.com/vimoksh-5/split-and-go/pkg/transport/grpc/pb"
)

// StreamSender sends protobuf ChunkEnvelopes over a gRPC stream.
type StreamSender interface {
	Send(*pb.ChunkEnvelope) error
}

// StreamReceiver receives protobuf ChunkEnvelopes from a gRPC stream.
type StreamReceiver interface {
	Recv() (*pb.ChunkEnvelope, error)
}

// ChunkToProto converts a core.Chunk to a protobuf ChunkEnvelope.
func ChunkToProto(c *core.Chunk) *pb.ChunkEnvelope {
	if c == nil {
		return nil
	}

	var pType pb.ChecksumType
	switch c.ChecksumType {
	case core.ChecksumCRC32:
		pType = pb.ChecksumType_CHECKSUM_TYPE_CRC32
	case core.ChecksumSHA256:
		pType = pb.ChecksumType_CHECKSUM_TYPE_SHA256
	default:
		pType = pb.ChecksumType_CHECKSUM_TYPE_NONE
	}

	return &pb.ChunkEnvelope{
		SessionId:     c.SessionID,
		Sequence:      c.Sequence,
		Offset:        c.Offset,
		TotalChunks:   c.TotalChunks,
		TotalBytes:    c.TotalBytes,
		Data:          c.Data,
		Checksum:      c.Checksum,
		ChecksumBytes: c.ChecksumBytes,
		ChecksumType:  pType,
		IsLast:        c.IsLast(),
		IsCompressed:  c.IsCompressed(),
		Metadata:      c.Metadata,
	}
}

// ProtoToChunk converts a protobuf ChunkEnvelope to a core.Chunk.
func ProtoToChunk(p *pb.ChunkEnvelope) *core.Chunk {
	if p == nil {
		return nil
	}

	var cType core.ChecksumType
	switch p.ChecksumType {
	case pb.ChecksumType_CHECKSUM_TYPE_CRC32:
		cType = core.ChecksumCRC32
	case pb.ChecksumType_CHECKSUM_TYPE_SHA256:
		cType = core.ChecksumSHA256
	default:
		cType = core.ChecksumNone
	}

	var flags core.ChunkFlags
	if p.IsLast {
		flags |= core.FlagIsLast
	}
	if p.IsCompressed {
		flags |= core.FlagIsCompressed
	}

	return &core.Chunk{
		SessionID:     p.SessionId,
		Sequence:      p.Sequence,
		Offset:        p.Offset,
		TotalChunks:   p.TotalChunks,
		TotalBytes:    p.TotalBytes,
		Data:          p.Data,
		Checksum:      p.Checksum,
		ChecksumBytes: p.ChecksumBytes,
		ChecksumType:  cType,
		Flags:         flags,
		Metadata:      p.Metadata,
	}
}

// SendStream splits data from src and streams chunks over a gRPC StreamSender.
func SendStream(ctx context.Context, sender StreamSender, src io.Reader, opts ...splitter.Option) error {
	s := splitter.New(src, opts...)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			chunk, err := s.Next()
			if err != nil {
				if errors.Is(err, io.EOF) {
					return nil
				}
				return err
			}

			protoChunk := ChunkToProto(chunk)
			if err := sender.Send(protoChunk); err != nil {
				return err
			}

			if chunk.IsLast() {
				return nil
			}
		}
	}
}

// ReceiveStream reads chunk envelopes from a gRPC StreamReceiver and reassembles them into dst.
func ReceiveStream(ctx context.Context, receiver StreamReceiver, dst io.Writer, opts ...assembler.Option) error {
	asm := assembler.New(dst, opts...)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			protoChunk, err := receiver.Recv()
			if err != nil {
				if errors.Is(err, io.EOF) {
					if !asm.IsCompleted() {
						return core.ErrStreamIncomplete
					}
					return nil
				}
				return err
			}

			chunk := ProtoToChunk(protoChunk)
			if err := asm.WriteChunk(chunk); err != nil {
				return err
			}

			if chunk.IsLast() {
				return nil
			}
		}
	}
}
