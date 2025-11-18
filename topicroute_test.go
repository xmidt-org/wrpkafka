// SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package wrpkafka

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xmidt-org/wrp-go/v5"
)

func TestTopicRoute_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		route   TopicRoute
		wantErr bool
	}{
		{
			name: "valid single topic",
			route: TopicRoute{
				Pattern: "event.type",
				Topic:   "my-topic",
			},
			wantErr: false,
		},
		{
			name: "valid multi-topic round-robin",
			route: TopicRoute{
				Pattern:            "event.*",
				Topics:             []string{"topic-1", "topic-2"},
				TopicShardStrategy: TopicShardRoundRobin,
			},
			wantErr: false,
		},
		{
			name: "invalid empty pattern",
			route: TopicRoute{
				Pattern: "",
				Topic:   "my-topic",
			},
			wantErr: true,
		},
		{
			name: "invalid both topic and topics set",
			route: TopicRoute{
				Pattern: "event.type",
				Topic:   "my-topic",
				Topics:  []string{"topic-1", "topic-2"},
			},
			wantErr: true,
		},
		{
			name: "invalid topic shard strategy with single topic",
			route: TopicRoute{
				Pattern:            "event.type",
				Topic:              "my-topic",
				TopicShardStrategy: TopicShardRoundRobin,
			},
			wantErr: true,
		},
		{
			name: "invalid multi-topic missing shard strategy",
			route: TopicRoute{
				Pattern: "event.*",
				Topics:  []string{"topic-1", "topic-2"},
			},
			wantErr: true,
		},
		{
			name: "invalid no topics or topic",
			route: TopicRoute{
				Pattern: "event.type",
			},
			wantErr: true,
		},
		{
			name: "invalid unknown shard strategy",
			route: TopicRoute{
				Pattern:            "event.*",
				Topics:             []string{"topic-1", "topic-2"},
				TopicShardStrategy: "unknownstrategy",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.route.validate()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestSelectRoundRobin tests the round-robin topic selection.
func TestSelectRoundRobin(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		topics []string
		calls  int
		want   []string
	}{
		{
			name:   "cycles through three topics",
			topics: []string{"topic-0", "topic-1", "topic-2"},
			calls:  4,
			want:   []string{"topic-0", "topic-1", "topic-2", "topic-0"},
		},
		{
			name:   "single topic returns same",
			topics: []string{"only-topic"},
			calls:  3,
			want:   []string{"only-topic", "only-topic", "only-topic"},
		},
		{
			name:   "empty topics returns empty",
			topics: []string{},
			calls:  1,
			want:   []string{""},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			route := &TopicRoute{
				Topics:  tt.topics,
				counter: &atomic.Uint64{},
			}

			for i := 0; i < tt.calls; i++ {
				got := route.selectRoundRobin()
				assert.Equal(t, tt.want[i], got)
			}
		})
	}
}

// TestSelectByDeviceID tests device ID based topic selection.
func TestSelectByDeviceID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		topics   []string
		deviceID string
		want     string
	}{
		{
			name:     "hashes device ID consistently",
			topics:   []string{"topic-0", "topic-1", "topic-2"},
			deviceID: "mac:112233445566",
			want:     "topic-0",
		},
		{
			name:     "hashes device ID consistently 2",
			topics:   []string{"topic-0", "topic-1", "topic-2"},
			deviceID: "mac:112233445577",
			want:     "topic-1",
		},
		{
			name:     "empty device ID falls back to round-robin",
			topics:   []string{"topic-0", "topic-1"},
			deviceID: "",
			want:     "topic-0", // First round-robin call
		},
		{
			name:     "empty topics returns empty",
			topics:   []string{},
			deviceID: "mac:112233445566",
			want:     "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			route := &TopicRoute{
				Topics:  tt.topics,
				counter: &atomic.Uint64{},
			}

			msg := &wrp.Message{Source: tt.deviceID}
			got := route.selectByDeviceID(msg)

			assert.Equal(t, tt.want, got)
		})
	}
}

// TestSelectByMetadata tests metadata field based topic selection.
func TestSelectByMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		topics    []string
		fieldName string
		metadata  map[string]string
		want      string
	}{
		{
			name:      "hashes metadata field consistently",
			topics:    []string{"topic-0", "topic-1", "topic-2"},
			fieldName: "tenant_id",
			metadata:  map[string]string{"tenant_id": "tenant-a"},
			want:      "topic-0",
		},
		{
			name:      "hashes metadata field consistently 2",
			topics:    []string{"topic-0", "topic-1", "topic-2"},
			fieldName: "tenant_id",
			metadata:  map[string]string{"tenant_id": "tenant-b"},
			want:      "topic-2",
		},
		{
			name:      "missing field falls back to round-robin",
			topics:    []string{"topic-0", "topic-1"},
			fieldName: "tenant_id",
			metadata:  map[string]string{},
			want:      "topic-0", // First round-robin call
		},
		{
			name:      "empty field value falls back to round-robin",
			topics:    []string{"topic-0", "topic-1"},
			fieldName: "tenant_id",
			metadata:  map[string]string{"tenant_id": ""},
			want:      "topic-0", // First round-robin call
		},
		{
			name:      "nil metadata falls back to round-robin",
			topics:    []string{"topic-0", "topic-1"},
			fieldName: "tenant_id",
			metadata:  nil,
			want:      "topic-0", // First round-robin call
		},
		{
			name:      "empty topics returns empty",
			topics:    []string{},
			fieldName: "tenant_id",
			metadata:  map[string]string{"tenant_id": "tenant-a"},
			want:      "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			route := &TopicRoute{
				Topics:  tt.topics,
				counter: &atomic.Uint64{},
			}

			msg := &wrp.Message{Metadata: tt.metadata}
			got := route.selectByMetadata(msg, tt.fieldName)

			assert.Equal(t, tt.want, got)
		})
	}
}

// TestSelectTopic tests the main topic selection logic.
func TestSelectTopic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		route  TopicRoute
		msg    *wrp.Message
		want   string
		verify func(t *testing.T, route *TopicRoute, got string)
	}{
		{
			name: "single topic route",
			route: TopicRoute{
				Topic: "my-topic",
			},
			msg:  &wrp.Message{Source: "mac:112233445566"},
			want: "my-topic",
		},
		{
			name: "round-robin strategy",
			route: TopicRoute{
				Topics:             []string{"topic-0", "topic-1"},
				TopicShardStrategy: TopicShardRoundRobin,
				counter:            &atomic.Uint64{},
			},
			msg:  &wrp.Message{Source: "mac:112233445566"},
			want: "topic-0",
		},
		{
			name: "device ID strategy",
			route: TopicRoute{
				Topics:             []string{"topic-0", "topic-1", "topic-2"},
				TopicShardStrategy: TopicShardDeviceID,
				counter:            &atomic.Uint64{},
			},
			msg: &wrp.Message{Source: "mac:112233445566"},
			verify: func(t *testing.T, route *TopicRoute, got string) {
				assert.NotEmpty(t, got)
				// Verify it's one of the configured topics
				assert.Contains(t, route.Topics, got)
			},
		},
		{
			name: "metadata strategy",
			route: TopicRoute{
				Topics:             []string{"topic-0", "topic-1"},
				TopicShardStrategy: "metadata:region",
				counter:            &atomic.Uint64{},
			},
			msg: &wrp.Message{
				Source:   "mac:112233445566",
				Metadata: map[string]string{"region": "us-west"},
			},
			verify: func(t *testing.T, route *TopicRoute, got string) {
				assert.NotEmpty(t, got)
				assert.Contains(t, route.Topics, got)
			},
		},
		{
			name: "unknown strategy returns empty",
			route: TopicRoute{
				Topics:             []string{"topic-0", "topic-1"},
				TopicShardStrategy: "unknown-strategy",
				counter:            &atomic.Uint64{},
			},
			msg:  &wrp.Message{Source: "mac:112233445566"},
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := tt.route.selectTopic(tt.msg)
			if tt.verify != nil {
				tt.verify(t, &tt.route, got)
			} else {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

// TestSelectRoundRobin_Concurrency tests concurrent round-robin selection.
func TestSelectRoundRobin_Concurrency(t *testing.T) {
	t.Parallel()

	route := &TopicRoute{
		Topics:  []string{"topic-0", "topic-1", "topic-2"},
		counter: &atomic.Uint64{},
	}

	const goroutines = 10
	const iterations = 100
	done := make(chan struct{}, goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for j := 0; j < iterations; j++ {
				topic := route.selectRoundRobin()
				require.NotEmpty(t, topic)
			}
		}()
	}

	// Wait for all goroutines
	for i := 0; i < goroutines; i++ {
		<-done
	}

	// Counter should equal total iterations
	assert.Equal(t, uint64(goroutines*iterations), route.counter.Load())
}
