// SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package wrpkafka_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xmidt-org/wrpkafka"
)

// TestIntegration_BasicPublish tests basic message publishing to Kafka.
//
// Verifies:
// - Single message publish with high QoS
// - Verification of message content in Kafka
func TestIntegration_BasicPublish(t *testing.T) {
	t.Parallel()
	_, broker := setupKafka(t)

	// Create publisher
	pub := createTestPublisher(t, broker, []wrpkafka.TopicRoute{
		{Pattern: "device-*", Topic: "device-events"},
		{Pattern: "*", Topic: "default-events"},
	})

	err := pub.Start()
	require.NoError(t, err)
	defer pub.Stop(context.Background())

	// Publish message
	msg := createTestMessage("device-status", "mac:112233445566", 75)

	outcome, err := pub.Produce(context.Background(), msg)
	require.NoError(t, err)
	assert.Equal(t, wrpkafka.Accepted, outcome)

	// Verify message in Kafka
	records := consumeMessages(t, broker, "device-events", messageConsumeWait)
	require.Len(t, records, 1, "Expected exactly 1 message in Kafka")
	verifyWRPMessage(t, records[0], msg)
}

// TestIntegration_QoSLevels tests all three QoS levels.
//
// Verifies:
// - Low QoS (0-24): Fire-and-forget
// - Medium QoS (25-74): Async with retry
// - High QoS (75-99): Sync with confirmation
func TestIntegration_QoSLevels(t *testing.T) {
	t.Parallel()
	_, broker := setupKafka(t)

	pub := createTestPublisher(t, broker, []wrpkafka.TopicRoute{
		{Pattern: "*", Topic: "qos-test"},
	})

	err := pub.Start()
	require.NoError(t, err)
	defer pub.Stop(context.Background())

	tests := []struct {
		name            string
		qos             int64
		expectedOutcome wrpkafka.Outcome
	}{
		{"Low QoS (0-24)", 10, wrpkafka.Attempted},
		{"Medium QoS (25-74)", 50, wrpkafka.Queued},
		{"High QoS (75-99)", 90, wrpkafka.Accepted},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := createTestMessage("qos-test", "mac:112233445566", tt.qos)

			outcome, err := pub.Produce(context.Background(), msg)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedOutcome, outcome)
		})
	}

	// Verify all messages arrived
	records := consumeMessages(t, broker, "qos-test", messageConsumeWait)
	assert.GreaterOrEqual(t, len(records), 3, "Expected at least 3 messages")
}

// TestIntegration_TopicRouting tests pattern-based topic routing.
//
// Verifies:
// - Exact pattern matching
// - Prefix pattern matching
// - Catch-all pattern (*)
func TestIntegration_TopicRouting(t *testing.T) {
	t.Parallel()
	_, broker := setupKafka(t)

	pub := createTestPublisher(t, broker, []wrpkafka.TopicRoute{
		{Pattern: "online", Topic: "lifecycle-events"},
		{Pattern: "offline", Topic: "lifecycle-events"},
		{Pattern: "device-*", Topic: "device-events"},
		{Pattern: "*", Topic: "default-events"},
	})

	err := pub.Start()
	require.NoError(t, err)
	defer pub.Stop(context.Background())

	tests := []struct {
		eventType     string
		expectedTopic string
	}{
		{"online", "lifecycle-events"},
		{"offline", "lifecycle-events"},
		{"device-status", "device-events"},
		{"device-config", "device-events"},
		{"unknown-event", "default-events"},
	}

	for i, tt := range tests {
		t.Run(tt.eventType, func(t *testing.T) {
			// Use unique device ID for each subtest to avoid race conditions
			// when subtests run in parallel with -race flag
			deviceID := fmt.Sprintf("mac:11223344%04d", 5566+i)
			msg := createTestMessage(tt.eventType, deviceID, 75)

			outcome, err := pub.Produce(context.Background(), msg)
			require.NoError(t, err)
			assert.Equal(t, wrpkafka.Accepted, outcome)

			// Verify message in expected topic
			records := consumeMessages(t, broker, tt.expectedTopic, messageConsumeWait)
			require.GreaterOrEqual(t, len(records), 1, "Expected message in topic %s", tt.expectedTopic)

			// Find our message (there might be others from previous tests)
			found := false
			for _, r := range records {
				decoded := decodeWRPMessage(t, r)
				if decoded.Source == deviceID {
					verifyWRPMessage(t, r, msg)
					found = true
					break
				}
			}
			assert.True(t, found, "Message not found in topic %s", tt.expectedTopic)
		})
	}
}

// TestIntegration_RoundRobinSharding tests round-robin distribution across topics.
//
// Verifies:
// - Even distribution across multiple topics
// - 9 messages → 3 per shard
func TestIntegration_RoundRobinSharding(t *testing.T) {
	t.Parallel()
	_, broker := setupKafka(t)

	pub := createTestPublisher(t, broker, []wrpkafka.TopicRoute{
		{
			Pattern:            "*",
			Topics:             []string{"shard-0", "shard-1", "shard-2"},
			TopicShardStrategy: wrpkafka.TopicShardRoundRobin,
		},
	})

	err := pub.Start()
	require.NoError(t, err)
	defer pub.Stop(context.Background())

	// Publish 9 messages
	numMessages := 9
	for i := 0; i < numMessages; i++ {
		msg := createTestMessage("test-event", "mac:112233445566", 75)
		outcome, err := pub.Produce(context.Background(), msg)
		require.NoError(t, err)
		assert.Equal(t, wrpkafka.Accepted, outcome)
	}

	// Verify distribution across shards
	shard0 := consumeMessages(t, broker, "shard-0", messageConsumeWait)
	shard1 := consumeMessages(t, broker, "shard-1", messageConsumeWait)
	shard2 := consumeMessages(t, broker, "shard-2", messageConsumeWait)

	t.Logf("Distribution: shard-0=%d, shard-1=%d, shard-2=%d", len(shard0), len(shard1), len(shard2))

	// Verify all messages received
	total := len(shard0) + len(shard1) + len(shard2)
	assert.Equal(t, numMessages, total, "All messages should be received")

	// Verify even distribution (each shard should get 3 messages)
	assert.Equal(t, 3, len(shard0), "shard-0 should have 3 messages")
	assert.Equal(t, 3, len(shard1), "shard-1 should have 3 messages")
	assert.Equal(t, 3, len(shard2), "shard-2 should have 3 messages")
}

// TestIntegration_DeviceIDSharding tests device ID-based sharding.
//
// Verifies:
// - Same device → same topic
// - Hash consistency verification
func TestIntegration_DeviceIDSharding(t *testing.T) {
	t.Parallel()
	_, broker := setupKafka(t)

	pub := createTestPublisher(t, broker, []wrpkafka.TopicRoute{
		{
			Pattern:            "*",
			Topics:             []string{"device-shard-0", "device-shard-1", "device-shard-2"},
			TopicShardStrategy: wrpkafka.TopicShardDeviceID,
		},
	})

	err := pub.Start()
	require.NoError(t, err)
	defer pub.Stop(context.Background())

	// Publish multiple messages from same device
	deviceID := "mac:AABBCCDDEEFF"
	for i := 0; i < 5; i++ {
		msg := createTestMessage("test-event", deviceID, 75)
		outcome, err := pub.Produce(context.Background(), msg)
		require.NoError(t, err)
		assert.Equal(t, wrpkafka.Accepted, outcome)
	}

	// Consume from all shards
	shard0 := consumeMessages(t, broker, "device-shard-0", messageConsumeWait)
	shard1 := consumeMessages(t, broker, "device-shard-1", messageConsumeWait)
	shard2 := consumeMessages(t, broker, "device-shard-2", messageConsumeWait)

	t.Logf("Distribution: shard-0=%d, shard-1=%d, shard-2=%d", len(shard0), len(shard1), len(shard2))

	// Verify all messages from same device went to same shard
	shardsWithMessages := 0
	if len(shard0) > 0 {
		shardsWithMessages++
		assert.Equal(t, 5, len(shard0), "All messages should be in one shard")
	}
	if len(shard1) > 0 {
		shardsWithMessages++
		assert.Equal(t, 5, len(shard1), "All messages should be in one shard")
	}
	if len(shard2) > 0 {
		shardsWithMessages++
		assert.Equal(t, 5, len(shard2), "All messages should be in one shard")
	}

	assert.Equal(t, 1, shardsWithMessages, "Messages from same device should go to exactly one shard")
}

// TestIntegration_EventListeners tests PublishEvent listener notifications.
//
// Verifies:
// - PublishEvent dispatched on success
// - Event fields populated correctly
func TestIntegration_EventListeners(t *testing.T) {
	t.Parallel()
	_, broker := setupKafka(t)

	var events []*wrpkafka.PublishEvent
	var mu sync.Mutex

	pub := createTestPublisher(t, broker,
		[]wrpkafka.TopicRoute{{Pattern: "*", Topic: "listener-test"}})

	pub.InitialPublishEventListeners = []func(*wrpkafka.PublishEvent){
		func(e *wrpkafka.PublishEvent) {
			mu.Lock()
			defer mu.Unlock()
			events = append(events, e)
		},
	}

	err := pub.Start()
	require.NoError(t, err)
	defer pub.Stop(context.Background())

	// Publish message
	msg := createTestMessage("test-event", "mac:112233445566", 75)
	outcome, err := pub.Produce(context.Background(), msg)
	require.NoError(t, err)
	assert.Equal(t, wrpkafka.Accepted, outcome)

	// Wait a bit for async listener
	time.Sleep(100 * time.Millisecond)

	// Verify event was dispatched
	mu.Lock()
	defer mu.Unlock()

	require.Len(t, events, 1, "Expected exactly 1 event")
	event := events[0]

	assert.Equal(t, "test-event", event.EventType)
	assert.Equal(t, "listener-test", event.Topic)
	assert.Equal(t, "", event.TopicShardStrategy) // TopicShardNone for single-topic routes
	assert.NoError(t, event.Error)
	assert.Empty(t, event.ErrorType)
	assert.Greater(t, event.Duration, time.Duration(0))
}

// TestIntegration_DynamicConfigUpdate tests runtime configuration updates.
//
// Verifies:
// - Runtime TopicMap changes
// - Old messages use old config
// - New messages use new config
func TestIntegration_DynamicConfigUpdate(t *testing.T) {
	t.Parallel()
	_, broker := setupKafka(t)

	// Initial config
	pub := createTestPublisher(t, broker, []wrpkafka.TopicRoute{
		{Pattern: "*", Topic: "topic-v1"},
	})

	err := pub.Start()
	require.NoError(t, err)
	defer pub.Stop(context.Background())

	// Publish with initial config
	msg1 := createTestMessage("test-event", "mac:112233445566", 75)
	outcome, err := pub.Produce(context.Background(), msg1)
	require.NoError(t, err)
	assert.Equal(t, wrpkafka.Accepted, outcome)

	// Update config
	newConfig := wrpkafka.DynamicConfig{
		TopicMap: []wrpkafka.TopicRoute{
			{Pattern: "*", Topic: "topic-v2"},
		},
	}
	err = pub.UpdateConfig(newConfig)
	require.NoError(t, err)

	// Publish with new config
	msg2 := createTestMessage("test-event", "mac:112233445566", 75)
	outcome, err = pub.Produce(context.Background(), msg2)
	require.NoError(t, err)
	assert.Equal(t, wrpkafka.Accepted, outcome)

	// Verify messages in correct topics
	v1Records := consumeMessages(t, broker, "topic-v1", messageConsumeWait)
	v2Records := consumeMessages(t, broker, "topic-v2", messageConsumeWait)

	assert.Len(t, v1Records, 1, "topic-v1 should have 1 message")
	assert.Len(t, v2Records, 1, "topic-v2 should have 1 message")
}

// TestIntegration_ConcurrentPublish tests concurrent Produce() calls.
//
// Verifies:
// - 100 messages from 10 goroutines
// - Thread safety verification
func TestIntegration_ConcurrentPublish(t *testing.T) {
	t.Parallel()
	_, broker := setupKafka(t)

	pub := createTestPublisher(t, broker, []wrpkafka.TopicRoute{
		{Pattern: "*", Topic: "concurrent-test"},
	})

	err := pub.Start()
	require.NoError(t, err)
	defer pub.Stop(context.Background())

	// Publish concurrently from multiple goroutines
	numGoroutines := 10
	messagesPerGoroutine := 10
	var wg sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < messagesPerGoroutine; j++ {
				msg := createTestMessage("concurrent-event", "mac:112233445566", 50)
				_, err := pub.Produce(context.Background(), msg)
				if err != nil {
					t.Errorf("Goroutine %d message %d failed: %v", id, j, err)
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify all messages arrived
	time.Sleep(2 * time.Second) // Give async messages time to flush
	records := consumeMessages(t, broker, "concurrent-test", messageConsumeWait)

	expectedTotal := numGoroutines * messagesPerGoroutine
	assert.GreaterOrEqual(t, len(records), expectedTotal-5,
		"Expected approximately %d messages (allowing for some async timing)", expectedTotal)
}

// TestIntegration_StartStopMultipleTimes tests lifecycle management.
//
// Verifies:
// - Multiple Start/Stop cycles
// - State cleanup verification
func TestIntegration_StartStopMultipleTimes(t *testing.T) {
	t.Parallel()
	_, broker := setupKafka(t)

	pub := createTestPublisher(t, broker, []wrpkafka.TopicRoute{
		{Pattern: "*", Topic: "lifecycle-test"},
	})

	// Start/Stop cycle 1
	err := pub.Start()
	require.NoError(t, err)

	msg1 := createTestMessage("test-event", "mac:112233445566", 75)
	_, err = pub.Produce(context.Background(), msg1)
	require.NoError(t, err)

	pub.Stop(context.Background())

	// Start/Stop cycle 2
	err = pub.Start()
	require.NoError(t, err)

	msg2 := createTestMessage("test-event", "mac:112233445566", 75)
	_, err = pub.Produce(context.Background(), msg2)
	require.NoError(t, err)

	pub.Stop(context.Background())

	// Verify both messages arrived
	records := consumeMessages(t, broker, "lifecycle-test", messageConsumeWait)
	assert.GreaterOrEqual(t, len(records), 2, "Expected at least 2 messages")
}
