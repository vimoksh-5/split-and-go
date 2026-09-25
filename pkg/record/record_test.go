package record_test

import (
	"context"
	"testing"
	"time"

	"github.com/vimoksh-5/split-and-go/pkg/record"
)

type UserEvent struct {
	ID        int64  `json:"id"`
	UserID    string `json:"user_id"`
	EventType string `json:"event_type"`
	Timestamp int64  `json:"timestamp"`
}

func TestRecordStreamSlice(t *testing.T) {
	streamer := record.NewStreamer[UserEvent](
		record.WithBatchSize(25),
		record.WithFormat(record.FormatNDJSON),
	)

	// Create 100 sample events
	events := make([]UserEvent, 100)
	for i := 0; i < 100; i++ {
		events[i] = UserEvent{
			ID:        int64(i),
			UserID:    "user_abc",
			EventType: "login",
			Timestamp: time.Now().Unix(),
		}
	}

	chunks, err := streamer.StreamSlice(events)
	if err != nil {
		t.Fatalf("StreamSlice failed: %v", err)
	}

	// 100 items / 25 batch size = 4 chunks
	if len(chunks) != 4 {
		t.Fatalf("expected 4 chunks, got %d", len(chunks))
	}

	receiver := record.NewReceiver[UserEvent]()
	var received []UserEvent

	for _, c := range chunks {
		items, err := receiver.DecodeChunk(c)
		if err != nil {
			t.Fatalf("DecodeChunk failed: %v", err)
		}
		received = append(received, items...)
	}

	if len(received) != 100 {
		t.Fatalf("expected 100 received events, got %d", len(received))
	}

	for i := 0; i < 100; i++ {
		if received[i].ID != int64(i) {
			t.Errorf("expected item %d ID to be %d, got %d", i, i, received[i].ID)
		}
	}
}

func TestRecordStreamChannel(t *testing.T) {
	streamer := record.NewStreamer[UserEvent](
		record.WithBatchSize(10),
		record.WithFormat(record.FormatNDJSON),
	)

	itemCh := make(chan UserEvent, 50)
	go func() {
		defer close(itemCh)
		for i := 0; i < 45; i++ {
			itemCh <- UserEvent{
				ID:        int64(i),
				UserID:    "user_live",
				EventType: "click",
			}
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	chunkCh, errCh := streamer.StreamFromChannel(ctx, itemCh)

	receiver := record.NewReceiver[UserEvent]()
	consumedCh, recvErrCh := receiver.ConsumeChannel(ctx, chunkCh)

	var result []UserEvent
	for it := range consumedCh {
		result = append(result, it)
	}

	if err := <-errCh; err != nil {
		t.Fatalf("streamer error: %v", err)
	}
	if err := <-recvErrCh; err != nil {
		t.Fatalf("receiver error: %v", err)
	}

	if len(result) != 45 {
		t.Fatalf("expected 45 consumed items, got %d", len(result))
	}
}

func TestRecordFormatJSON(t *testing.T) {
	streamer := record.NewStreamer[UserEvent](
		record.WithBatchSize(10),
		record.WithFormat(record.FormatJSON),
	)

	items := []UserEvent{
		{ID: 1, UserID: "u1"},
		{ID: 2, UserID: "u2"},
	}

	chunks, err := streamer.StreamSlice(items)
	if err != nil {
		t.Fatalf("StreamSlice failed: %v", err)
	}

	receiver := record.NewReceiver[UserEvent]()
	decoded, err := receiver.DecodeChunk(chunks[0])
	if err != nil {
		t.Fatalf("DecodeChunk failed: %v", err)
	}

	if len(decoded) != 2 {
		t.Fatalf("expected 2 items, got %d", len(decoded))
	}
}
