// SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

// Package wrpkafka provides a high-performance, QoS-aware publisher for routing
// WRP (Web Routing Protocol) device events to Apache Kafka topics.
//
// # Overview
//
// The wrpkafka library enables applications to publish WRP messages to Kafka
// with configurable routing strategies, quality-of-service guarantees, and
// framework-agnostic observability. The library handles connection management,
// message routing, and delivery confirmation automatically.
//
// # Quick Start
//
// Create a Publisher by setting fields directly:
//
//	publisher := &wrpkafka.Publisher{
//	    Brokers: []string{"localhost:9092"},
//	    InitialDynamicConfig: wrpkafka.DynamicConfig{
//	        TopicMap: []wrpkafka.TopicRoute{
//	            {Pattern: "*", Topic: "device-events"},
//	        },
//	    },
//	}
//	defer publisher.Stop(context.Background())
//
//	if err := publisher.Start(); err != nil {
//	    log.Fatal(err)
//	}

//	msg := &wrp.Message{
//	    Type:             wrp.SimpleEventMessageType,
//	    Source:           "mac:112233445566",
//	    Destination:      "event:device-status/mac:112233445566",
//	    QualityOfService: 50,
//	    Payload:          []byte(`{"status":"online"}`),
//	}
//
//	outcome, err := publisher.Produce(context.Background(), msg)
//	if err != nil {
//	    log.Printf("Publish failed: %v", err)
//	}
//
// # Quality of Service
//
// The library supports three QoS levels based on WRP QualityOfService field:
//
//   - QoS 0-24 (Low Priority): Fire-and-forget delivery. Messages are dropped
//     if the buffer is full. Never blocks the caller.
//
//   - QoS 25-74 (Medium Priority): Asynchronous delivery with automatic retries.
//     Waits if buffer is full. Returns Queued outcome.
//
//   - QoS 75-99 (High Priority): Synchronous delivery with broker confirmation.
//     Blocks until acknowledged. Returns Accepted or Failed outcome.
//
// # Topic Routing
//
// Messages are routed to Kafka topics based on configurable patterns that match
// against the WRP event type extracted from the Destination field:
//
//	TopicMap: []wrpkafka.TopicRoute{
//	    {Pattern: "online", Topic: "device-lifecycle"},
//	    {Pattern: "offline", Topic: "device-lifecycle"},
//	    {Pattern: "device-status-*", Topic: "device-status"},
//	    {Pattern: "*", Topic: "device-events"}, // catch-all
//	}
//
// The library supports multiple topic selection strategies:
//
//   - Single topic: Route all matching messages to one topic
//   - Round-robin: Distribute evenly across multiple topics
//   - Device-based: Route based on device ID for consistent partitioning
//   - Metadata-based: Route based on custom WRP metadata fields
//
// # Observability
//
// The library provides framework-agnostic observability via event listeners
// and logging (using franz-go's kgo.Logger interface for flexibility):
//
//	publisher := &wrpkafka.Publisher{}
//	publisher.Brokers = []string{"localhost:9092"}
//	publisher.Logger = logger
//	publisher.InitialDynamicConfig = wrpkafka.DynamicConfig{
//	    TopicMap: []wrpkafka.TopicRoute{
//	        {Pattern: "*", Topic: "device-events"},
//	    },
//	}
//	publisher.InitialPublishEventListeners = []func(*wrpkafka.PublishEvent){
//	    func(e *wrpkafka.PublishEvent) {
//	        if e.Error != nil {
//	            metrics.ErrorCounter.WithLabelValues(e.EventType, e.Topic, e.TopicShardStrategy, e.ErrorType).Inc()
//	        } else {
//	            metrics.PublishCounter.WithLabelValues(e.EventType, e.Topic, e.TopicShardStrategy).Inc()
//	        }
//	        metrics.LatencyHistogram.WithLabelValues(e.EventType, e.Topic, e.TopicShardStrategy).Observe(e.Duration.Seconds())
//	    },
//	}
//
// # Runtime Configuration Updates
//
// The library supports hot-reload of routing rules, compression settings, and
// headers without requiring a restart:
//
//	newConfig := wrpkafka.DynamicConfig{
//	    TopicMap: []wrpkafka.TopicRoute{
//	        {Pattern: "*", Topic: "events-v2"},
//	    },
//	    CompressionCodec: wrpkafka.CompressionSnappy,
//	    Acks:             wrpkafka.AcksAll,
//	}
//
//	if err := publisher.UpdateConfig(newConfig); err != nil {
//	    log.Printf("Config update failed: %v", err)
//	}
//
// # Thread Safety
//
// The Publisher type is safe for concurrent use by multiple goroutines.
// All methods (Start, Stop, Produce, UpdateConfig, BufferedRecords) can be
// called concurrently without external synchronization.
//
// # Performance
//
// The library is optimized for high-throughput message publishing:
//
//   - Lock-free configuration reads using atomic pointers
//   - Pattern matching completes in <10ns for common patterns
//   - Zero-allocation message routing for steady state
//   - O(1) round-robin distribution
//   - Sustained throughput of 10,000+ messages/second per instance
//
// For complete documentation, see the README and specification documents
// in the specs/ directory.
package wrpkafka
