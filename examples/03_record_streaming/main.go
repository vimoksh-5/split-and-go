package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/vimoksh-5/split-and-go"
	"github.com/vimoksh-5/split-and-go/pkg/record"
)

type Transaction struct {
	ID        int64     `json:"id"`
	AccountID string    `json:"account_id"`
	Amount    float64   `json:"amount"`
	Currency  string    `json:"currency"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

func main() {
	fmt.Println("=== Example 3: Structured Record Streaming (NDJSON Micro-batching) ===")

	totalRecords := 50000
	batchSize := 2500

	fmt.Printf("Streaming %d typed database transactions in batches of %d...\n\n", totalRecords, batchSize)

	// Streamer configured with NDJSON format
	streamer := splitandgo.NewRecordStreamer[Transaction](
		record.WithBatchSize(batchSize),
		record.WithFormat(record.FormatNDJSON),
	)

	// Channel representing live DB query rows
	rowCh := make(chan Transaction, 1000)
	go func() {
		defer close(rowCh)
		for i := 1; i <= totalRecords; i++ {
			rowCh <- Transaction{
				ID:        int64(i),
				AccountID: fmt.Sprintf("ACC-%06d", i%500),
				Amount:    10.50 + float64(i%100),
				Currency:  "USD",
				Status:    "COMPLETED",
				CreatedAt: time.Now(),
			}
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Convert record channel into chunk stream
	chunkCh, errCh := streamer.StreamFromChannel(ctx, rowCh)

	// Consumer side: decode chunks back into typed records
	receiver := splitandgo.NewRecordReceiver[Transaction]()
	consumedCh, recvErrCh := receiver.ConsumeChannel(ctx, chunkCh)

	count := 0
	totalAmount := 0.0
	start := time.Now()

	for tx := range consumedCh {
		count++
		totalAmount += tx.Amount
		if count%10000 == 0 {
			fmt.Printf("  -> Processed %d / %d records so far...\n", count, totalRecords)
		}
	}

	if err := <-errCh; err != nil {
		log.Fatalf("Streamer error: %v", err)
	}
	if err := <-recvErrCh; err != nil {
		log.Fatalf("Receiver error: %v", err)
	}

	duration := time.Since(start)
	fmt.Printf("\n All %d records processed in %v! (%.0f records/sec)\n",
		count, duration, float64(count)/duration.Seconds())
	fmt.Printf("Total transacted value: $%.2f\n", totalAmount)
}
