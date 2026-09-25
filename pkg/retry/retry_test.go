package retry_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/vimoksh-5/split-and-go/pkg/retry"
)

func TestRetrySuccessOnFirstAttempt(t *testing.T) {
	policy := retry.DefaultPolicy()
	calls := 0

	err := policy.Execute(context.Background(), func(ctx context.Context) error {
		calls++
		return nil
	})

	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}
}

func TestRetrySuccessAfterTransientErrors(t *testing.T) {
	policy := retry.Policy{
		MaxAttempts:  4,
		InitialDelay: 5 * time.Millisecond,
		Multiplier:   1.5,
		Jitter:       false,
	}

	calls := 0
	retriesNotified := 0
	policy.OnRetry = func(attempt int, delay time.Duration, err error) {
		retriesNotified++
	}

	targetErr := errors.New("temporary network glitch")
	err := policy.Execute(context.Background(), func(ctx context.Context) error {
		calls++
		if calls < 3 {
			return targetErr
		}
		return nil
	})

	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
	if retriesNotified != 2 {
		t.Fatalf("expected 2 retry notifications, got %d", retriesNotified)
	}
}

func TestRetryExhaustion(t *testing.T) {
	policy := retry.Policy{
		MaxAttempts:  3,
		InitialDelay: 1 * time.Millisecond,
		Multiplier:   2.0,
	}

	expectedErr := errors.New("permanent failure")
	calls := 0
	err := policy.Execute(context.Background(), func(ctx context.Context) error {
		calls++
		return expectedErr
	})

	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected %v, got %v", expectedErr, err)
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
}

func TestRetryContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	policy := retry.Policy{
		MaxAttempts:  5,
		InitialDelay: 100 * time.Millisecond,
		Multiplier:   2.0,
	}

	calls := 0
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	err := policy.Execute(ctx, func(ctx context.Context) error {
		calls++
		return errors.New("error that causes sleep")
	})

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}
