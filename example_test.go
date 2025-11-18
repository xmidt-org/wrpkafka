// SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package wrpkafka_test

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/xmidt-org/wrp-go/v5"
	"github.com/xmidt-org/wrpkafka"
)

// Example demonstrates basic usage of the wrpkafka publisher.
func Example() {
	// Create publisher with routing configuration
	publisher := &wrpkafka.Publisher{
		Brokers: []string{"localhost:9092"},
		InitialDynamicConfig: wrpkafka.DynamicConfig{
			TopicMap: []wrpkafka.TopicRoute{
				{Pattern: "device-status-*", Topic: "device-status"},
				{Pattern: "*", Topic: "device-events"}, // catch-all
			},
		},
	}

	// Start publisher (connects to Kafka)
	if err := publisher.Start(); err != nil {
		log.Fatal(err)
	}
	defer publisher.Stop(context.Background())

	// Publish a WRP message
	msg := &wrp.Message{
		Type:             wrp.SimpleEventMessageType,
		Source:           "mac:112233445566",
		Destination:      "event:device-status/mac:112233445566",
		QualityOfService: 50, // Medium priority
		Payload:          []byte(`{"status":"online"}`),
	}

	outcome, err := publisher.Produce(context.Background(), msg)
	if err != nil {
		log.Printf("Publish failed: %v", err)
		return
	}

	fmt.Printf("Published with outcome: %v\n", outcome)
}

// ExamplePublisher demonstrates creating and configuring a Publisher.
func ExamplePublisher() {
	// Create a publisher with all configuration options
	publisher := &wrpkafka.Publisher{
		// Kafka cluster configuration
		Brokers: []string{"localhost:9092", "localhost:9093"},

		// Buffer limits (optional - 0 means unlimited)
		MaxBufferedRecords: 10000,
		MaxBufferedBytes:   10 * 1024 * 1024, // 10 MB

		// Timeouts (optional - 0 means no timeout)
		RequestTimeout: 30 * 1000000000, // 30 seconds (time.Duration)
		CleanupTimeout: 5 * 1000000000,  // 5 seconds

		// Retry behavior (optional - 0 means fail fast)
		MaxRetries: 3,

		// Topic routing configuration
		InitialDynamicConfig: wrpkafka.DynamicConfig{
			TopicMap: []wrpkafka.TopicRoute{
				{Pattern: "online", Topic: "device-lifecycle"},
				{Pattern: "offline", Topic: "device-lifecycle"},
				{Pattern: "device-status-*", Topic: "device-status"},
				{Pattern: "*", Topic: "device-events"}, // catch-all
			},
			CompressionCodec: "snappy",
			Acks:             "all",
		},
	}

	// Start the publisher
	if err := publisher.Start(); err != nil {
		log.Fatalf("Failed to start publisher: %v", err)
	}
	defer publisher.Stop(context.Background())

	fmt.Println("Publisher started successfully")
	// Output: Publisher started successfully
}

// Example_topicRouting demonstrates pattern-based topic routing.
func Example_topicRouting() {
	publisher := &wrpkafka.Publisher{
		Brokers: []string{"localhost:9092"},
		InitialDynamicConfig: wrpkafka.DynamicConfig{
			TopicMap: []wrpkafka.TopicRoute{
				// Exact match
				{Pattern: "online", Topic: "device-lifecycle"},
				{Pattern: "offline", Topic: "device-lifecycle"},

				// Prefix match
				{Pattern: "device-status-*", Topic: "device-status"},

				// Case-insensitive match
				{Pattern: "error", Topic: "errors", CaseInsensitive: true},

				// Catch-all (must be last)
				{Pattern: "*", Topic: "device-events"},
			},
		},
	}
	defer publisher.Stop(context.Background())

	fmt.Printf("TopicMap configured with %d routes\n", len(publisher.InitialDynamicConfig.TopicMap))
	// Output: TopicMap configured with 5 routes
}

// Example_shardingRoundRobin demonstrates round-robin sharding across multiple topics.
func Example_shardingRoundRobin() {
	publisher := &wrpkafka.Publisher{
		Brokers: []string{"localhost:9092"},
		InitialDynamicConfig: wrpkafka.DynamicConfig{
			TopicMap: []wrpkafka.TopicRoute{
				{
					Pattern:            "telemetry",
					Topics:             []string{"telemetry-0", "telemetry-1", "telemetry-2"},
					TopicShardStrategy: wrpkafka.TopicShardRoundRobin,
				},
			},
		},
	}
	defer publisher.Stop(context.Background())

	fmt.Printf("Round-robin sharding across %d topics\n", len(publisher.InitialDynamicConfig.TopicMap[0].Topics))
	// Output: Round-robin sharding across 3 topics
}

// Example_shardingDeviceID demonstrates device ID-based sharding for consistent partitioning.
func Example_shardingDeviceID() {
	publisher := &wrpkafka.Publisher{
		Brokers: []string{"localhost:9092"},
		InitialDynamicConfig: wrpkafka.DynamicConfig{
			TopicMap: []wrpkafka.TopicRoute{
				{
					Pattern:            "device-*",
					Topics:             []string{"device-0", "device-1", "device-2"},
					TopicShardStrategy: wrpkafka.TopicShardDeviceID,
				},
			},
		},
	}

	defer publisher.Stop(context.Background())

	fmt.Printf("Device ID sharding across %d topics\n", len(publisher.InitialDynamicConfig.TopicMap[0].Topics))
	// Output: Device ID sharding across 3 topics
}

// Example_shardingMetadata demonstrates metadata field-based sharding.
func Example_shardingMetadata() {
	publisher := &wrpkafka.Publisher{
		Brokers: []string{"localhost:9092"},
		InitialDynamicConfig: wrpkafka.DynamicConfig{
			TopicMap: []wrpkafka.TopicRoute{
				{
					Pattern:            "*",
					Topics:             []string{"region-east", "region-west"},
					TopicShardStrategy: "metadata:region",
				},
			},
		},
	}

	defer publisher.Stop(context.Background())

	fmt.Printf("Metadata sharding using field: region\n")
	// Output: Metadata sharding using field: region
}

// Example_dynamicConfiguration demonstrates runtime configuration updates.
func Example_dynamicConfiguration() {
	publisher := &wrpkafka.Publisher{
		Brokers: []string{"localhost:9092"},
		InitialDynamicConfig: wrpkafka.DynamicConfig{
			TopicMap: []wrpkafka.TopicRoute{{Pattern: "*", Topic: "default"}},
		},
	}

	defer publisher.Stop(context.Background())

	// Update configuration at runtime
	newConfig := wrpkafka.DynamicConfig{
		TopicMap: []wrpkafka.TopicRoute{
			{Pattern: "online", Topic: "device-lifecycle-v2"},
			{Pattern: "*", Topic: "device-events-v2"},
		},
		CompressionCodec: wrpkafka.CompressionZstd,
		Acks:             wrpkafka.AcksAll,
	}

	if err := publisher.UpdateConfig(newConfig); err != nil {
		log.Printf("Config update failed: %v", err)
	}

	fmt.Println("Configuration updated")
	// Output: Configuration updated
}

// Example_headers demonstrates static headers and WRP field extraction.
func Example_headers() {
	publisher := &wrpkafka.Publisher{
		Brokers: []string{"localhost:9092"},
		InitialDynamicConfig: wrpkafka.DynamicConfig{
			TopicMap: []wrpkafka.TopicRoute{{Pattern: "*", Topic: "events"}},
			Headers: map[string][]string{
				// Static values
				"service":     {"my-service"},
				"environment": {"production"},

				// Extract from WRP message (wrp.* prefix)
				"source":      {"wrp.Source"},
				"device-id":   {"wrp.DeviceID"},
				"partner-ids": {"wrp.PartnerIDs"}, // Multi-valued field
			},
		},
	}

	defer publisher.Stop(context.Background())

	fmt.Printf("Headers configured: %d entries\n", len(publisher.InitialDynamicConfig.Headers))
	// Output: Headers configured: 5 entries
}

// Example_observability demonstrates event listeners for metrics collection.
func Example_observability() {
	publisher := &wrpkafka.Publisher{
		Brokers: []string{"localhost:9092"},
		InitialDynamicConfig: wrpkafka.DynamicConfig{
			TopicMap: []wrpkafka.TopicRoute{{Pattern: "*", Topic: "events"}},
		},
	}

	publisher.InitialPublishEventListeners = []func(*wrpkafka.PublishEvent){
		func(event *wrpkafka.PublishEvent) {
			// Log or emit metrics
			if event.Error != nil {
				log.Printf("Publish failed: %s to %s (error: %s)",
					event.EventType, event.Topic, event.ErrorType)
			} else {
				log.Printf("Published: %s to %s in %v",
					event.EventType, event.Topic, event.Duration)
			}
		},
	}
	defer publisher.Stop(context.Background())

	fmt.Println("Event listener registered")
	// Output: Event listener registered
}

// Example_bufferMonitoring demonstrates querying buffer state.
func Example_bufferMonitoring() {
	publisher := &wrpkafka.Publisher{
		Brokers:            []string{"localhost:9092"},
		MaxBufferedRecords: 10000,
		MaxBufferedBytes:   10 * 1024 * 1024, // 10 MB
		InitialDynamicConfig: wrpkafka.DynamicConfig{
			TopicMap: []wrpkafka.TopicRoute{{Pattern: "*", Topic: "events"}},
		},
	}
	defer publisher.Stop(context.Background())

	currentRec, maxRec, currentBytes, maxBytes := publisher.BufferedRecords()
	recUtilization := float64(currentRec) / float64(maxRec) * 100
	byteUtilization := float64(currentBytes) / float64(maxBytes) * 100

	fmt.Printf("Records: %d/%d (%.1f%% full)\n", currentRec, maxRec, recUtilization)
	fmt.Printf("Bytes: %d/%d (%.1f%% full)\n", currentBytes, maxBytes, byteUtilization)
}

// Example_errorHandling demonstrates typed error handling.
func Example_errorHandling() {
	publisher := &wrpkafka.Publisher{
		Brokers: []string{"localhost:9092"},
		InitialDynamicConfig: wrpkafka.DynamicConfig{
			TopicMap: []wrpkafka.TopicRoute{{Pattern: "*", Topic: "events"}},
		},
	}

	defer publisher.Stop(context.Background())

	msg := &wrp.Message{
		Type:        wrp.SimpleEventMessageType,
		Source:      "mac:112233445566",
		Destination: "event:test",
	}

	outcome, err := publisher.Produce(context.Background(), msg)
	if err != nil {
		switch {
		case errors.Is(err, wrpkafka.ErrNotStarted):
			fmt.Println("Publisher not started")
		case errors.Is(err, wrpkafka.ErrNoTopicMatch):
			fmt.Println("No route matched event type")
		case errors.Is(err, wrpkafka.ErrValidation):
			fmt.Println("Configuration validation error")
		default:
			fmt.Println("Kafka or network error")
		}
		return
	}

	switch outcome {
	case wrpkafka.Accepted:
		fmt.Println("Confirmed by broker (QoS 75-99)")
	case wrpkafka.Queued:
		fmt.Println("Buffered for async delivery (QoS 25-74)")
	case wrpkafka.Attempted:
		fmt.Println("Fire-and-forget attempted (QoS 0-24)")
	case wrpkafka.Dropped:
		fmt.Println("Dropped due to buffer full (reported via callback)")
	case wrpkafka.Failed:
		fmt.Println("Synchronous delivery failed (QoS 75-99 only)")
	}
}

// ExamplePublisher_UpdateConfig demonstrates hot-reloading configuration at runtime.
func ExamplePublisher_UpdateConfig() {
	// Create publisher with initial configuration
	publisher := &wrpkafka.Publisher{
		Brokers: []string{"localhost:9092"},
		InitialDynamicConfig: wrpkafka.DynamicConfig{
			TopicMap: []wrpkafka.TopicRoute{
				{Pattern: "*", Topic: "events-v1"},
			},
		},
	}

	if err := publisher.Start(); err != nil {
		log.Fatal(err)
	}
	defer publisher.Stop(context.Background())

	// Later, update routing configuration without restart
	newConfig := wrpkafka.DynamicConfig{
		TopicMap: []wrpkafka.TopicRoute{
			{Pattern: "online", Topic: "lifecycle-v2"},
			{Pattern: "offline", Topic: "lifecycle-v2"},
			{Pattern: "*", Topic: "events-v2"},
		},
		CompressionCodec: "zstd",
		Acks:             "all",
		Headers: map[string][]string{
			"version": {"v2"},
		},
	}

	if err := publisher.UpdateConfig(newConfig); err != nil {
		log.Printf("Failed to update config: %v", err)
		return
	}

	fmt.Println("Configuration updated successfully")
	// Output: Configuration updated successfully
}

// ExamplePublisher_AddPublishEventListener demonstrates observability via event listeners.
func ExamplePublisher_AddPublishEventListener() {
	publisher := &wrpkafka.Publisher{
		Brokers: []string{"localhost:9092"},
		InitialDynamicConfig: wrpkafka.DynamicConfig{
			TopicMap: []wrpkafka.TopicRoute{
				{Pattern: "*", Topic: "events"},
			},
		},
	}

	// Add listener for metrics/logging
	cancel := publisher.AddPublishEventListener(func(event *wrpkafka.PublishEvent) {
		if event.Error != nil {
			// Record error metrics
			fmt.Printf("ERROR: %s -> %s (%s) after %v\n",
				event.EventType, event.Topic, event.ErrorType, event.Duration)
		} else {
			// Record success metrics
			fmt.Printf("SUCCESS: %s -> %s via %s in %v\n",
				event.EventType, event.Topic, event.TopicShardStrategy, event.Duration)
		}
	})

	// Listener can be removed later
	defer cancel()

	if err := publisher.Start(); err != nil {
		log.Fatal(err)
	}
	defer publisher.Stop(context.Background())

	fmt.Println("Event listener registered")
	// Output: Event listener registered
}
