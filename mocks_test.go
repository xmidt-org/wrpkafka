// SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package wrpkafka

import (
	"context"

	"github.com/stretchr/testify/mock"
	"github.com/twmb/franz-go/pkg/kgo"
)

// mockKafkaClient is a mock implementation of kafkaClient for testing.
type mockKafkaClient struct {
	mock.Mock
}

func (m *mockKafkaClient) Produce(ctx context.Context, r *kgo.Record, cb func(*kgo.Record, error)) {
	m.Called(ctx, r, cb)
}

func (m *mockKafkaClient) TryProduce(ctx context.Context, r *kgo.Record, cb func(*kgo.Record, error)) {
	m.Called(ctx, r, cb)
}

func (m *mockKafkaClient) Flush(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *mockKafkaClient) Close() {
	m.Called()
}

func (m *mockKafkaClient) BufferedProduceRecords() int64 {
	args := m.Called()
	return args.Get(0).(int64)
}

func (m *mockKafkaClient) BufferedProduceBytes() int64 {
	args := m.Called()
	return args.Get(0).(int64)
}

func (m *mockKafkaClient) ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults {
	args := m.Called(ctx, rs)
	return args.Get(0).(kgo.ProduceResults)
}
