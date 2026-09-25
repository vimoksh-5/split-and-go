.PHONY: all build test test-race bench proto examples clean tidy lint

all: test

build:
	go build ./...

test:
	go test -v ./...

test-race:
	go test -race -v ./...

bench:
	go test -bench=. -benchmem ./...

compare:
	go run cmd/bench/main.go

disk:
	go run examples/06_public_api_real_world/main.go 1gb --disk

demo:
	go run cmd/webdemo/main.go

proto:
	@which protoc > /dev/null || (echo "protoc not found. Please install protoc." && exit 1)
	export PATH=$$PATH:$$(go env GOPATH)/bin && \
	protoc --proto_path=proto \
		--go_out=pkg/transport/grpc/pb --go_opt=module=github.com/vimoksh-5/split-and-go/pkg/transport/grpc/pb \
		--go-grpc_out=pkg/transport/grpc/pb --go-grpc_opt=module=github.com/vimoksh-5/split-and-go/pkg/transport/grpc/pb \
		proto/splitandgo/v1/chunk.proto

examples:
	@echo "Running Example 1: Byte Streaming..."
	go run examples/01_byte_streaming/main.go
	@echo "\nRunning Example 2: HTTP Chunked..."
	go run examples/02_http_chunked_server/main.go
	@echo "\nRunning Example 3: Record Streaming..."
	go run examples/03_record_streaming/main.go
	@echo "\nRunning Example 4: gRPC Streaming..."
	go run examples/04_grpc_streaming/main.go
	@echo "\nRunning Example 5: Out of Order Resilience..."
	go run examples/05_out_of_order_resilience/main.go
	@echo "\nRunning Example 6: Real-World Public Internet Stream..."
	go run examples/06_public_api_real_world/main.go

tidy:
	go mod tidy

clean:
	go clean
