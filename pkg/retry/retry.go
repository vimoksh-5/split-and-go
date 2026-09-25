package retry

import (
	"context"
	"math/rand"
	"time"
)

// Policy defines the backoff and retry behavior for transient network failures.
type Policy struct {
	MaxAttempts   int           // Maximum number of attempts (including the first attempt)
	InitialDelay  time.Duration // Initial retry delay
	MaxDelay      time.Duration // Cap on retry delay
	Multiplier    float64       // Exponential growth factor (e.g. 2.0)
	Jitter        bool          // Whether to apply full jitter to prevent thundering herds
	OnRetry       func(attempt int, delay time.Duration, err error)
}

// DefaultPolicy returns an enterprise-tuned default retry policy:
// 3 attempts, initial delay 100ms, max delay 2s, multiplier 2.0, with jitter.
func DefaultPolicy() Policy {
	return Policy{
		MaxAttempts:  3,
		InitialDelay: 100 * time.Millisecond,
		MaxDelay:     2 * time.Second,
		Multiplier:   2.0,
		Jitter:       true,
	}
}

// NoRetryPolicy returns a policy that executes only once without retrying.
func NoRetryPolicy() Policy {
	return Policy{
		MaxAttempts: 1,
	}
}

// Execute runs the given operation according to the retry policy.
func (p Policy) Execute(ctx context.Context, op func(ctx context.Context) error) error {
	maxAttempts := p.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 1
	}

	delay := p.InitialDelay
	if delay <= 0 {
		delay = 50 * time.Millisecond
	}
	multiplier := p.Multiplier
	if multiplier <= 1.0 {
		multiplier = 2.0
	}

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		// Check context before each attempt
		if err := ctx.Err(); err != nil {
			return err
		}

		lastErr = op(ctx)
		if lastErr == nil {
			return nil
		}

		// If this was our last attempt, break
		if attempt >= maxAttempts {
			break
		}

		// Calculate sleep delay with optional jitter
		actualDelay := delay
		if p.Jitter {
			// Full jitter: random sleep between 0 and actualDelay
			actualDelay = time.Duration(rand.Float64() * float64(delay))
		}

		if p.OnRetry != nil {
			p.OnRetry(attempt, actualDelay, lastErr)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(actualDelay):
		}

		// Increase delay for next round
		delay = time.Duration(float64(delay) * multiplier)
		if p.MaxDelay > 0 && delay > p.MaxDelay {
			delay = p.MaxDelay
		}
	}

	return lastErr
}
