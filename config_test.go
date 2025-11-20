// SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package wrpkafka

import (
	"crypto/tls"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
)

/*
// TestPublisherValidation tests Publisher field validation.
func TestPublisherValidation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		publisher *Publisher
		wantErr   bool
	}{
		// Valid configurations
		{
			name: "minimal valid config",
			publisher: &Publisher{
				Brokers: []string{"localhost:9092"},
				InitialDynamicConfig: DynamicConfig{
					TopicMap: []TopicRoute{{Pattern: "*", Topic: "events"}},
				},
			},
		},
		{
			name: "single topic route",
			publisher: &Publisher{
				Brokers: []string{"localhost:9092"},
				InitialDynamicConfig: DynamicConfig{
					TopicMap: []TopicRoute{{Pattern: "device-status", Topic: "status"}},
				},
			},
		},
		{
			name: "multi-topic with round-robin",
			publisher: &Publisher{
				Brokers: []string{"localhost:9092"},
				InitialDynamicConfig: DynamicConfig{
					TopicMap: []TopicRoute{
						{
							Pattern:            "device-*",
							Topics:             []string{"t1", "t2", "t3"},
							TopicShardStrategy: TopicShardRoundRobin,
						},
					},
				},
			},
		},
		{
			name: "multi-topic with deviceid",
			publisher: &Publisher{
				Brokers: []string{"localhost:9092"},
				InitialDynamicConfig: DynamicConfig{
					TopicMap: []TopicRoute{
						{
							Pattern:            "*",
							Topics:             []string{"shard-1", "shard-2"},
							TopicShardStrategy: TopicShardDeviceID,
						},
					},
				},
			},
		},
		{
			name: "multi-topic with metadata sharding",
			publisher: &Publisher{
				Brokers: []string{"localhost:9092"},
				InitialDynamicConfig: DynamicConfig{
					TopicMap: []TopicRoute{
						{
							Pattern:            "event-*",
							Topics:             []string{"a", "b"},
							TopicShardStrategy: "metadata:tenant_id",
						},
					},
				},
			},
		},
		{
			name: "valid compression codec",
			publisher: &Publisher{
				Brokers: []string{"localhost:9092"},
				InitialDynamicConfig: DynamicConfig{
					TopicMap:         []TopicRoute{{Pattern: "*", Topic: "events"}},
					CompressionCodec: CompressionSnappy,
				},
			},
		},
		{
			name: "valid acks",
			publisher: &Publisher{
				Brokers: []string{"localhost:9092"},
				InitialDynamicConfig: DynamicConfig{
					TopicMap: []TopicRoute{{Pattern: "*", Topic: "events"}},
					Acks:     AcksAll,
				},
			},
		},
		{
			name: "valid headers",
			publisher: &Publisher{
				Brokers: []string{"localhost:9092"},
				InitialDynamicConfig: DynamicConfig{
					TopicMap: []TopicRoute{{Pattern: "*", Topic: "events"}},
					Headers: map[string][]string{
						"service": {"test"},
						"source":  {"wrp.Source"},
					},
				},
			},
		},
		{
			name: "multiple routes",
			publisher: &Publisher{
				Brokers: []string{"localhost:9092"},
				InitialDynamicConfig: DynamicConfig{
					TopicMap: []TopicRoute{
						{Pattern: "online", Topic: "lifecycle"},
						{Pattern: "offline", Topic: "lifecycle"},
						{Pattern: "device-*", Topic: "devices"},
						{Pattern: "*", Topic: "default"},
					},
				},
			},
		},

		// Invalid - brokers
		{
			name:      "empty brokers",
			publisher: &Publisher{Brokers: []string{}},
			wantErr:   true,
		},
		{
			name:      "nil brokers",
			publisher: &Publisher{Brokers: nil},
			wantErr:   true,
		},

		// Invalid - topic map
		{
			name: "empty topic map",
			publisher: &Publisher{
				Brokers:              []string{"localhost:9092"},
				InitialDynamicConfig: DynamicConfig{TopicMap: []TopicRoute{}},
			},
			wantErr: true,
		},
		{
			name: "nil topic map",
			publisher: &Publisher{
				Brokers:              []string{"localhost:9092"},
				InitialDynamicConfig: DynamicConfig{TopicMap: nil},
			},
			wantErr: true,
		},

		// Invalid - pattern
		{
			name: "empty pattern",
			publisher: &Publisher{
				Brokers: []string{"localhost:9092"},
				InitialDynamicConfig: DynamicConfig{
					TopicMap: []TopicRoute{{Pattern: "", Topic: "events"}},
				},
			},
			wantErr: true,
		},

		// Invalid - topic/topics combinations
		{
			name: "both topic and topics set",
			publisher: &Publisher{
				Brokers: []string{"localhost:9092"},
				InitialDynamicConfig: DynamicConfig{
					TopicMap: []TopicRoute{
						{
							Pattern:            "*",
							Topic:              "single",
							Topics:             []string{"t1", "t2"},
							TopicShardStrategy: TopicShardRoundRobin,
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "neither topic nor topics set",
			publisher: &Publisher{
				Brokers: []string{"localhost:9092"},
				InitialDynamicConfig: DynamicConfig{
					TopicMap: []TopicRoute{{Pattern: "*"}},
				},
			},
			wantErr: true,
		},
		{
			name: "single topic with shard strategy",
			publisher: &Publisher{
				Brokers: []string{"localhost:9092"},
				InitialDynamicConfig: DynamicConfig{
					TopicMap: []TopicRoute{
						{
							Pattern:            "*",
							Topic:              "single",
							TopicShardStrategy: TopicShardRoundRobin,
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "multi-topic without shard strategy",
			publisher: &Publisher{
				Brokers: []string{"localhost:9092"},
				InitialDynamicConfig: DynamicConfig{
					TopicMap: []TopicRoute{
						{Pattern: "*", Topics: []string{"t1", "t2"}},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "empty topics list",
			publisher: &Publisher{
				Brokers: []string{"localhost:9092"},
				InitialDynamicConfig: DynamicConfig{
					TopicMap: []TopicRoute{
						{
							Pattern:            "*",
							Topics:             []string{},
							TopicShardStrategy: TopicShardRoundRobin,
						},
					},
				},
			},
			wantErr: true,
		},

		// Invalid - enums (test one invalid per enum type)
		{
			name: "invalid compression codec",
			publisher: &Publisher{
				Brokers: []string{"localhost:9092"},
				InitialDynamicConfig: DynamicConfig{
					TopicMap:         []TopicRoute{{Pattern: "*", Topic: "events"}},
					CompressionCodec: "invalid-codec",
				},
			},
			wantErr: true,
		},
		{
			name: "invalid acks",
			publisher: &Publisher{
				Brokers: []string{"localhost:9092"},
				InitialDynamicConfig: DynamicConfig{
					TopicMap: []TopicRoute{{Pattern: "*", Topic: "events"}},
					Acks:     "invalid-acks",
				},
			},
			wantErr: true,
		},
		{
			name: "invalid shard strategy",
			publisher: &Publisher{
				Brokers: []string{"localhost:9092"},
				InitialDynamicConfig: DynamicConfig{
					TopicMap: []TopicRoute{
						{
							Pattern:            "*",
							Topics:             []string{"t1", "t2"},
							TopicShardStrategy: "invalid-strategy",
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "metadata sharding without field name",
			publisher: &Publisher{
				Brokers: []string{"localhost:9092"},
				InitialDynamicConfig: DynamicConfig{
					TopicMap: []TopicRoute{
						{
							Pattern:            "*",
							Topics:             []string{"t1", "t2"},
							TopicShardStrategy: "metadata:",
						},
					},
				},
			},
			wantErr: true,
		},

		// Invalid - headers
		{
			name: "empty header key",
			publisher: &Publisher{
				Brokers: []string{"localhost:9092"},
				InitialDynamicConfig: DynamicConfig{
					TopicMap: []TopicRoute{{Pattern: "*", Topic: "events"}},
					Headers:  map[string][]string{"": {"value"}},
				},
			},
			wantErr: true,
		},
		{
			name: "empty header values",
			publisher: &Publisher{
				Brokers: []string{"localhost:9092"},
				InitialDynamicConfig: DynamicConfig{
					TopicMap: []TopicRoute{{Pattern: "*", Topic: "events"}},
					Headers:  map[string][]string{"key": {}},
				},
			},
			wantErr: true,
		},
		{
			name: "invalid wrp field reference",
			publisher: &Publisher{
				Brokers: []string{"localhost:9092"},
				InitialDynamicConfig: DynamicConfig{
					TopicMap: []TopicRoute{{Pattern: "*", Topic: "events"}},
					Headers:  map[string][]string{"field": {"wrp.InvalidField"}},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.publisher.validate()
			if tt.wantErr {
				assert.Error(t, err)
				assert.ErrorIs(t, err, ErrValidation)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
*/

// TestDynamicConfigValidation tests DynamicConfig validation.
func TestDynamicConfigValidation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		config  DynamicConfig
		wantErr bool
	}{
		{
			name: "valid minimal config",
			config: DynamicConfig{
				TopicMap: []TopicRoute{{Pattern: "*", Topic: "events"}},
			},
		},
		{
			name: "valid with all options",
			config: DynamicConfig{
				TopicMap: []TopicRoute{
					{Pattern: "device-*", Topic: "devices"},
					{Pattern: "*", Topic: "default"},
				},
				CompressionCodec: CompressionZstd,
				Acks:             AcksAll,
				Linger:           100 * time.Millisecond,
				Headers: map[string][]string{
					"service": {"test"},
				},
			},
		},
		{
			name:    "empty topic map",
			config:  DynamicConfig{TopicMap: []TopicRoute{}},
			wantErr: true,
		},
		{
			name: "invalid topic route",
			config: DynamicConfig{
				TopicMap: []TopicRoute{{Pattern: ""}},
			},
			wantErr: true,
		},
		{
			name: "invalid compression",
			config: DynamicConfig{
				TopicMap:         []TopicRoute{{Pattern: "*", Topic: "events"}},
				CompressionCodec: "bad",
			},
			wantErr: true,
		},
		{
			name: "invalid acks",
			config: DynamicConfig{
				TopicMap: []TopicRoute{{Pattern: "*", Topic: "events"}},
				Acks:     "bad",
			},
			wantErr: true,
		},
		{
			name: "invalid header",
			config: DynamicConfig{
				TopicMap: []TopicRoute{{Pattern: "*", Topic: "events"}},
				Headers:  map[string][]string{"": {"value"}},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.config.validate()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestToKgoOpts tests conversion of Publisher config to franz-go options.
func TestToKgoOpts(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		publisher *Publisher
	}{
		{
			name: "brokers option",
			publisher: &Publisher{
				Brokers: []string{"broker1:9092", "broker2:9092"},
			},
		},
		{
			name: "SASL option",
			publisher: &Publisher{
				Brokers: []string{"localhost:9092"},
				SASL: plain.Auth{
					User: "user",
					Pass: "pass",
				}.AsMechanism(),
			},
		},
		{
			name: "TLS option",
			publisher: &Publisher{
				Brokers: []string{"localhost:9092"},
				TLS:     &tls.Config{},
			},
		},
		{
			name: "buffer limits",
			publisher: &Publisher{
				Brokers:            []string{"localhost:9092"},
				MaxBufferedRecords: 1000,
				MaxBufferedBytes:   1024 * 1024,
			},
		},
		{
			name: "timeouts and retries",
			publisher: &Publisher{
				Brokers:        []string{"localhost:9092"},
				RequestTimeout: 30 * time.Second,
				MaxRetries:     5,
			},
		},
		{
			name: "all options",
			publisher: &Publisher{
				Brokers: []string{"localhost:9092"},
				SASL: plain.Auth{
					User: "user",
					Pass: "pass",
				}.AsMechanism(),
				TLS:                    &tls.Config{},
				MaxBufferedRecords:     1000,
				MaxBufferedBytes:       1024 * 1024,
				RequestTimeout:         30 * time.Second,
				MaxRetries:             5,
				AllowAutoTopicCreation: true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			opts := tt.publisher.toKgoOpts()

			assert.NotEmpty(t, opts)
		})
	}
}

// TestToKgoOpts_CreatesValidClient verifies that generated options create a valid client.
func TestToKgoOpts_CreatesValidClient(t *testing.T) {
	t.Parallel()
	publisher := &Publisher{
		Brokers:            []string{"localhost:9092"},
		MaxBufferedRecords: 1000,
		RequestTimeout:     30 * time.Second,
		MaxRetries:         5,
	}

	opts := publisher.toKgoOpts()
	require.NotEmpty(t, opts)

	// Should be able to create a client (won't connect without Start)
	client, err := kgo.NewClient(opts...)
	require.NoError(t, err)
	require.NotNil(t, client)
	client.Close()
}
