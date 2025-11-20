// SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package wrpkafka

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

// kafkaClient is an interface for the franz-go Kafka client methods we need.
// This allows us to mock the client for testing while using the real
// kgo.Client in production.
type kafkaClient interface {
	// TryProduce attempts to produce a record without blocking if the buffer is full.
	// Does not return anything (fire-and-forget behavior).
	TryProduce(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error))

	// Produce produces a record asynchronously, blocking if the buffer is full.
	Produce(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error))

	// ProduceSync produces records synchronously and waits for broker acknowledgment.
	ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults

	// Flush flushes all buffered records and waits for them to be sent.
	Flush(ctx context.Context) error

	// Close closes the Kafka client and releases resources.
	Close()

	// BufferedProduceRecords returns the current number of buffered records.
	BufferedProduceRecords() int64

	// BufferedProduceBytes returns the current number of buffered bytes.
	BufferedProduceBytes() int64
}

// Verify that *kgo.Client implements kafkaClient interface at compile time.
var _ kafkaClient = (*kgo.Client)(nil)
