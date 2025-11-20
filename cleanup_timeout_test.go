// SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package wrpkafka

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/kgo"
)

// captureFlushClient captures the context passed to Flush for testing.
type captureFlushClient struct {
	capturedCtx *context.Context
}

func (c *captureFlushClient) TryProduce(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error)) {
}
func (c *captureFlushClient) Produce(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error)) {
}
func (c *captureFlushClient) ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults {
	return kgo.ProduceResults{{Record: &kgo.Record{}}}
}
func (c *captureFlushClient) Flush(ctx context.Context) error {
	*c.capturedCtx = ctx
	return nil
}
func (c *captureFlushClient) Close()                        {}
func (c *captureFlushClient) BufferedProduceRecords() int64 { return 0 }
func (c *captureFlushClient) BufferedProduceBytes() int64   { return 0 }

// TestStop_CleanupTimeoutRespectsCaller tests that CleanupTimeout
// only applies when the caller's context has no deadline.
func TestStop_CleanupTimeoutRespectsCaller(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name             string
		cleanupTimeout   time.Duration
		callerContext    func() context.Context
		expectsDeadline  bool
		expectedDuration time.Duration
		allowedVariance  time.Duration
	}{
		{
			name:            "no_cleanup_timeout_no_caller_deadline",
			cleanupTimeout:  0,
			callerContext:   func() context.Context { return context.Background() },
			expectsDeadline: false,
		},
		{
			name:             "cleanup_timeout_no_caller_deadline",
			cleanupTimeout:   5 * time.Second,
			callerContext:    func() context.Context { return context.Background() },
			expectsDeadline:  true,
			expectedDuration: 5 * time.Second,
			allowedVariance:  100 * time.Millisecond,
		},
		{
			name:           "cleanup_timeout_with_caller_deadline_shorter",
			cleanupTimeout: 10 * time.Second,
			callerContext: func() context.Context {
				//nolint:govet // Context must remain valid after return; will timeout naturally
				ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
				return ctx
			},
			expectsDeadline:  true,
			expectedDuration: 2 * time.Second,
			allowedVariance:  100 * time.Millisecond,
		},
		{
			name:           "cleanup_timeout_with_caller_deadline_longer",
			cleanupTimeout: 2 * time.Second,
			callerContext: func() context.Context {
				//nolint:govet // Context must remain valid after return; will timeout naturally
				ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
				return ctx
			},
			expectsDeadline:  true,
			expectedDuration: 10 * time.Second,
			allowedVariance:  100 * time.Millisecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p := &Publisher{}
			p.Brokers = []string{"localhost:9092"}
			p.CleanupTimeout = tt.cleanupTimeout
			p.InitialDynamicConfig = DynamicConfig{
				TopicMap: []TopicRoute{{Pattern: "*", Topic: "test"}},
			}

			// Set a client factory that returns a client which captures the context passed to Flush
			var flushCtx context.Context
			captureClient := &captureFlushClient{capturedCtx: &flushCtx}
			p.clientFactory = func(opts ...kgo.Opt) (kafkaClient, error) {
				return captureClient, nil
			}

			// Start the publisher (this will use our factory to create the client)
			if err := p.Start(); err != nil {
				t.Fatalf("Start() failed: %v", err)
			}

			// Call Stop with the test context
			callerCtx := tt.callerContext()
			p.Stop(callerCtx)

			// Verify the context passed to Flush has the expected deadline
			if tt.expectsDeadline {
				deadline, ok := flushCtx.Deadline()
				assert.True(t, ok, "expected context to have deadline")

				timeUntilDeadline := time.Until(deadline)
				assert.InDelta(t,
					tt.expectedDuration.Seconds(),
					timeUntilDeadline.Seconds(),
					tt.allowedVariance.Seconds(),
					"deadline duration should be within expected range")
			} else {
				_, ok := flushCtx.Deadline()
				assert.False(t, ok, "expected context to have no deadline")
			}
		})
	}
}
