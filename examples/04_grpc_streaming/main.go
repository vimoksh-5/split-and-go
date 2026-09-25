package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/vimoksh-5/split-and-go"
	splitGrpc "github.com/vimoksh-5/split-and-go/pkg/transport/grpc"
	"github.com/vimoksh-5/split-and-go/pkg/transport/grpc/pb"
)

const bufSize = 1024 * 1024

// streamServer implements pb.StreamServiceServer
type streamServer struct {
	pb.UnimplementedStreamServiceServer
}

func (s *streamServer) StreamUpload(stream pb.StreamService_StreamUploadServer) error {
	var buf bytes.Buffer
	asm := splitandgo.NewAssembler(&buf, splitandgo.WithVerifyChecksums(true))

	for {
		protoChunk, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		chunk := splitGrpc.ProtoToChunk(protoChunk)
		if err := asm.WriteChunk(chunk); err != nil {
			return err
		}

		if chunk.IsLast() {
			break
		}
	}

	fmt.Printf("[gRPC Server] Successfully reassembled %d bytes from client stream!\n", buf.Len())

	return stream.SendAndClose(&pb.StreamAck{
		Success:        true,
		BytesReceived:  int64(buf.Len()),
		ChunksReceived: 10,
	})
}

func (s *streamServer) StreamDownload(req *pb.StreamDownloadRequest, stream pb.StreamService_StreamDownloadServer) error {
	fmt.Printf("[gRPC Server] Serving download for resource: %s\n", req.ResourceId)
	data := strings.Repeat("gRPC server streaming chunk envelope payload! ", 10000)
	src := strings.NewReader(data)

	return splitGrpc.SendStream(stream.Context(), stream, src, splitandgo.WithChunkSize(64*1024))
}

func main() {
	fmt.Println("=== Example 4: gRPC Chunk Streaming with Protobuf Contracts ===")

	// 1. Create in-memory gRPC listener
	lis := bufconn.Listen(bufSize)
	s := grpc.NewServer()
	pb.RegisterStreamServiceServer(s, &streamServer{})

	go func() {
		if err := s.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			log.Fatalf("Server exited with error: %v", err)
		}
	}()
	defer s.Stop()

	// 2. Connect client using bufconn
	dialer := func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("Failed to dial bufnet: %v", err)
	}
	defer conn.Close()

	client := pb.NewStreamServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 3. Test Client Streaming (StreamUpload)
	fmt.Println("\n[gRPC Client] Uploading large payload via client stream...")
	uploadStream, err := client.StreamUpload(ctx)
	if err != nil {
		log.Fatalf("StreamUpload failed: %v", err)
	}

	uploadData := strings.Repeat("Large file binary stream chunk for gRPC transfer. ", 20000)
	err = splitGrpc.SendStream(ctx, uploadStream, strings.NewReader(uploadData), splitandgo.WithChunkSize(128*1024))
	if err != nil {
		log.Fatalf("SendStream failed: %v", err)
	}

	ack, err := uploadStream.CloseAndRecv()
	if err != nil {
		log.Fatalf("CloseAndRecv failed: %v", err)
	}
	fmt.Printf("[gRPC Client] Server ACK received: Success=%t, Bytes=%d\n", ack.Success, ack.BytesReceived)

	// 4. Test Server Streaming (StreamDownload)
	fmt.Println("\n[gRPC Client] Downloading payload via server stream...")
	downloadStream, err := client.StreamDownload(ctx, &pb.StreamDownloadRequest{
		ResourceId:         "dataset-large-2026.parquet",
		PreferredChunkSize: 64 * 1024,
	})
	if err != nil {
		log.Fatalf("StreamDownload failed: %v", err)
	}

	var downloadDst bytes.Buffer
	err = splitGrpc.ReceiveStream(ctx, downloadStream, &downloadDst)
	if err != nil {
		log.Fatalf("ReceiveStream failed: %v", err)
	}

	fmt.Printf("[gRPC Client] Download complete! Reassembled %d bytes seamlessly.\n", downloadDst.Len())
}
