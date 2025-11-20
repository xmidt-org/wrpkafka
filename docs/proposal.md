<!-- SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC -->
<!-- SPDX-License-Identifier: Apache-2.0 -->

# Proposal: WRP-to-Kafka Event Publisher Library using franz-go

## Executive Summary

A reusable Go library (`github.com/xmidt-org/wrpkafka`) that publishes WRP (Web Routing Protocol) messages to Kafka topics using the [franz-go](https://github.com/twmb/franz-go) client library. Initially designed for Talaria integration, the library provides a high-throughput, reliable event streaming mechanism that can run alongside existing delivery systems. The library is decoupled from specific metrics and logging frameworks, making it suitable for various applications beyond Talaria.

**Key Features:**
- Single `Produce()` method with WRP QoS-aware routing:
  - QoS 0-24 (Low): Fire-and-forget, drops if buffer full
  - QoS 25-74 (Medium/High): Async delivery with automatic retries (franz-go handles retry logic)
  - QoS 75-99 (Critical): Synchronous with Kafka confirmation
  - Returns explicit outcome (Accepted, Queued, Dropped, Failed)
- Multiple topic sharding strategies (round-robin, device-based, metadata-based)
- Configurable with runtime dependencies via options pattern
- Metrics-agnostic with callback interface
- Per-device message ordering guarantees
- Production-ready with comprehensive error handling
- Works directly with `wrp.Message` for maximum flexibility

## Table of Contents

- [Executive Summary](#executive-summary)
1. [Library Architecture](#1-library-architecture)
   - [Design Principles](#11-design-principles)
   - [Component Design](#12-component-design)
   - [Quick Start Example](#13-quick-start-example)
   - [Configuration Mutability and Hot-Reload](#14-configuration-mutability-and-hot-reload)
   - [Public API Surface](#15-public-api-surface)
   - [Component Details](#16-component-details)
   - [Message Format](#17-message-format)
   - [WRP Event Processing](#18-wrp-event-processing)
   - [Topic Selection Strategies](#19-topic-selection-strategies)
   - [Partitioning](#110-partitioning)
   - [Topic Matching with Pattern Matching](#111-topic-matching-with-pattern-matching)
   - [franz-go Promise Callback Pattern](#112-franz-go-promise-callback-pattern)
   - [WRP QoS-Aware Message Production](#113-wrp-qos-aware-message-production)
   - [Integration Example (Talaria)](#114-integration-example-talaria)
   - [Configuration Schema](#115-configuration-schema)
   - [Metrics](#116-metrics)
   - [Error Handling](#117-error-handling)
   - [Testing Strategy](#118-testing-strategy)
2. [Advantages of This Library Design](#2-advantages-of-this-library-design)
3. [Operational Considerations](#3-operational-considerations)
4. [Use Case: Talaria Integration](#4-use-case-talaria-integration)
   - [Talaria's Current Event Flow](#41-talarias-current-event-flow)
   - [Key Integration Points](#42-key-integration-points)
5. [Dependencies](#5-dependencies)
6. [Alternatives Considered](#6-alternatives-considered)
7. [Implementation Phases](#7-implementation-phases)
   - [Phase 1: Core Library Implementation](#71-phase-1-core-library-implementation)
   - [Phase 2: Quality & Testing](#72-phase-2-quality--testing)
   - [Phase 3: Integration & Performance](#73-phase-3-integration--performance)
   - [Phase 4: Talaria Integration](#74-phase-4-talaria-integration)
8. [Success Criteria](#8-success-criteria)
   - [Library Quality](#81-library-quality)
   - [Talaria Integration Success](#82-talaria-integration-success)

## 1. Library Architecture

### 1.1 Design Principles
- **Reusability**: Decoupled from specific applications, metrics, or logging frameworks
- **Flexibility**: Supports multiple topic routing strategies (round-robin, device-based, metadata-based)
- **Configuration-Driven**: YAML-based configuration for all routing and producer settings
- **Observability**: Metrics via callback interface - works with any metrics backend
- **Reliability**: Leverages franz-go's built-in retry, buffering, and compression
- **Ordering Guarantees**: Per-device message ordering via partition keys

### 1.2 Component Design

**Design Philosophy**: The library uses a functional options pattern:
- **Configuration struct** (`Config`) for data loaded from YAML files (via [goschtalt](https://github.com/goschtalt/goschtalt))
- **Option functions** (`WithConfig`, `WithLogger`, `WithMessagesPublished`, etc.) for configuration and runtime dependencies
- **Single `New()` constructor** that accepts variadic options

This provides clean separation between configuration and runtime dependencies, with optional metric callbacks for observability.

### 1.3 Quick Start Example

```go
package main

import (
    "github.com/xmidt-org/wrp-go/v3"
    "github.com/xmidt-org/wrpkafka"
    "go.uber.org/zap"
)

func main() {
    // Load configuration from YAML (via goschtalt or similar)
    config := wrpkafka.Config{
        Brokers: []string{"localhost:9092"},
        TopicMap: []wrpkafka.TopicRoute{
            {Pattern: "online", Topic: "device-lifecycle"},
            {Pattern: "offline", Topic: "device-lifecycle"},
            {Pattern: "*", Topic: "device-events"},
        },
        Headers: map[string]string{
            "X-Source-Instance": "my-app-instance-1",
        },
        CompressionCodec: wrpkafka.CompressionSnappy,
        Acks:             wrpkafka.AcksAll,
    }

    // Create logger
    logger, _ := zap.NewProduction()

    // Create publisher with options
    // Note: WithConfig() is required - New() returns error if omitted
    // WithLogger() is optional - defaults to zap.NewNop() if not provided
    publisher, err := wrpkafka.New(
        wrpkafka.WithConfig(config),  // Required
        wrpkafka.WithLogger(logger),  // Optional
        wrpkafka.WithMessagesPublished(func(eventType, topic, strategy string) {
            // Your metrics here
        }),
    )
    if err != nil {
        // Validation error if WithConfig() not called or config is invalid
        panic(err)
    }

    // Start the publisher (connects to Kafka)
    if err := publisher.Start(); err != nil {
        panic(err)
    }
    defer publisher.Stop(context.Background())

    // Create a WRP message with QoS
    msg := &wrp.Message{
        Type:             wrp.SimpleEventMessageType,
        Source:           "mac:112233445566",
        Destination:      "event:device-status/mac:112233445566",
        QualityOfService: 50, // Medium priority
        // ... other fields
    }

    // Produce with QoS-aware routing
    // QoS determines behavior automatically:
    //   0-24: Fire-and-forget (drops if buffer full)
    //   25-74: Async with automatic retries (waits if buffer full, franz-go retries)
    //   75-99: Synchronous with confirmation
    outcome, err := publisher.Produce(context.Background(), msg)

    switch outcome {
    case wrpkafka.Accepted:
        // Message confirmed by Kafka (QoS 75-99 only)
        log.Info("critical message delivered")
    case wrpkafka.Queued:
        // Message buffered, async delivery (QoS 0-74)
        log.Debug("message buffered for delivery")
    case wrpkafka.Dropped:
        // Low priority message dropped (QoS 0-24, buffer full)
        log.Warn("low priority message dropped")
    case wrpkafka.Failed:
        // Critical message failed (QoS 75-99 only)
        log.Error("critical message failed", zap.Error(err))
    }
}
```

### 1.4 Configuration Mutability and Hot-Reload

The library supports runtime configuration updates for operational flexibility without requiring restarts.

**Static Configuration (Requires Restart):**
These fields affect the underlying [franz-go](https://github.com/twmb/franz-go) client and Kafka connections:
- `Brokers` - Broker list, requires new connections
- `SASL` - Authentication, requires reconnection
- `TLS` - Encryption settings, requires reconnection
- `MaxBufferedRecords` - Buffer allocation at startup
- `RequestTimeout` - Client-level timeout configuration
- `MaxRetries` - Client-level retry behavior

**Dynamic Configuration (Hot-Reloadable):**
These fields can be updated at runtime via `UpdateConfig()`:
- `TopicMap` - Routing rules (patterns, topics, sharding strategies)
- `Headers` - Kafka record headers
- `CompressionCodec` - Compression algorithm
- `Linger` - Batching delay
- `Acks` - Acknowledgment requirements

**Hot-Reload Example:**
```go
// Initial configuration from YAML
publisher, err := wrpkafka.New(
    wrpkafka.WithConfig(config),
    wrpkafka.WithLogger(logger),
)
publisher.Start()

// Later: Update routing without restart
newDynamic := wrpkafka.DynamicConfig{
    TopicMap: []wrpkafka.TopicRoute{
        {Pattern: "online", Topic: "device-lifecycle-v2"},
        {Pattern: "offline", Topic: "device-lifecycle-v2"},
        {Pattern: "*", Topic: "device-events-v2"},
    },
    Headers:          config.Headers,
    CompressionCodec: wrpkafka.CompressionSnappy,
    Linger:           10 * time.Millisecond,
    Acks:             wrpkafka.AcksAll,
}

if err := publisher.UpdateConfig(newDynamic); err != nil {
    log.Error("config update failed", zap.Error(err))
    return
}
// New Produce() calls immediately use the new configuration
// In-flight messages complete with old configuration
```

**Benefits:**
- **Operational flexibility**: Update routing, headers, compression without downtime
- **Safe updates**: Validation occurs before applying changes
- **Atomic**: Updates take effect immediately for new messages
- **No disruption**: In-flight messages complete normally
- **Thread-safe**: UpdateConfig() can be called concurrently with Produce()

**Implementation Details:**
- Uses `atomic.Pointer[DynamicConfig]` for lock-free reads during Produce()
- Round-robin counters are reinitialized on TopicMap changes
- Validation ensures configuration consistency before applying

### 1.5 Public API Surface

The library exports the following public types and functions:

**Core Types:**
- `Config` - Configuration struct (unmarshaled from YAML)
- `DynamicConfig` - Runtime-updatable configuration subset
- `TopicRoute` - Topic routing configuration
- `Option` - Functional option type
- `Outcome` - Enum describing message delivery outcome

**Outcome Constants:**
- `Accepted` - Message delivered and acknowledged by Kafka (QoS 75-99 only)
- `Queued` - Message buffered but not confirmed (QoS 0-74)
- `Dropped` - Message dropped due to buffer full (QoS 0-24 only)
- `Failed` - Synchronous delivery failed (QoS 75-99 only)

**Constructor & Options:**
- `New(opts ...Option) (*Publisher, error)` - Main constructor
- `WithConfig(Config) Option` - Set configuration from struct
- `WithLogger(*zap.Logger) Option` - Set logger instance
- `WithMessagesPublished(func(...) ) Option` - Metric callback for successful publishes
- `WithMessageErrored(func(...) ) Option` - Metric callback for failed publishes
- `WithPublishLatency(func(...) ) Option` - Metric callback for latency tracking

**Main Interface:**
- `(*Publisher) Start() error` - Start the publisher and connect to Kafka
- `(*Publisher) Stop()` - Stop the publisher and flush buffered messages
- `(*Publisher) Produce(context.Context, *wrp.Message) (Outcome, error)` - Publish a WRP message with QoS-aware routing (thread-safe)
- `(*Publisher) UpdateConfig(DynamicConfig) error` - Atomically update runtime configuration (thread-safe, takes effect immediately for new messages)
- `(*Publisher) BufferedRecords() (current int, max int)` - Returns current buffered record count and max buffer size (thread-safe, zero overhead)

### 1.5 Component Details

#### 1.5.1 Config (Configuration)
```go
// Config contains all configuration loaded from YAML.
// This struct contains only configuration data, no runtime dependencies.
//
// Configuration is divided into static and dynamic fields:
// - Static fields (Brokers, SASL, TLS, MaxBufferedRecords, RequestTimeout, MaxRetries)
//   can only be set at startup via WithConfig(). Changing these requires restart.
// - Dynamic fields (TopicMap, Headers, CompressionCodec, Linger, Acks)
//   can be updated at runtime via UpdateConfig(). See DynamicConfig type.
//
// Note: In actual implementation, each field will have full GoDoc comments
// above the field definition following Go documentation best practices.
type Config struct {
    // STATIC CONFIGURATION (requires restart to change)

    // Brokers is the list of Kafka broker addresses to connect to.
    // Each address must be in "host:port" format.
    // Example: []string{"kafka-1:9092", "kafka-2:9092", "kafka-3:9092"}
    // STATIC: Changing requires restart
    Brokers []string

    // SASL configures SASL authentication for broker connections.
    // Optional. If nil, no authentication is used.
    // Uses [franz-go](https://github.com/twmb/franz-go)'s sasl.Config from github.com/twmb/franz-go/pkg/sasl
    // STATIC: Changing requires restart
    SASL *sasl.Config

    // TLS configures TLS encryption for broker connections.
    // Optional. If nil, plaintext connections are used.
    // Uses standard library crypto/tls.Config
    // STATIC: Changing requires restart
    TLS *tls.Config

    // MaxBufferedRecords sets the maximum number of records to buffer in memory.
    // Zero or negative values disable buffering.
    // Default: 0 (no buffering)
    // Higher values increase throughput at the cost of memory usage.
    // STATIC: Changing requires restart
    MaxBufferedRecords int

    // RequestTimeout sets the maximum time to wait for broker responses.
    // Zero or negative values mean no timeout.
    // Default: 0 (no timeout)
    // Example: 30*time.Second
    // STATIC: Changing requires restart
    RequestTimeout time.Duration

    // MaxRetries controls retry behavior on broker failures.
    // -1: No retries, fail immediately
    // 0: Unlimited retries (default), retry until success or timeout
    // >0: Retry up to this many times before failing
    // Default: 0 (unlimited retries)
    // STATIC: Changing requires restart
    MaxRetries int

    // DYNAMIC CONFIGURATION (can be updated via UpdateConfig)

    // TopicMap defines routing rules from WRP event types to Kafka topics.
    // Patterns are evaluated in order; first match wins.
    // Must not be empty.
    // DYNAMIC: Can be updated at runtime via UpdateConfig()
    TopicMap []TopicRoute

    // Headers defines additional Kafka record headers to include.
    // Optional. Map keys are header names, values are either:
    //   - Literal strings (e.g., "my-app-v1.0")
    //   - WRP field references (e.g., "wrp.Source", "wrp.TransactionUUID")
    // DYNAMIC: Can be updated at runtime via UpdateConfig()
    Headers map[string]string

    // CompressionCodec specifies the compression algorithm for message payloads.
    // Valid values: "snappy", "gzip", "lz4", "zstd", "none"
    // Default: "" (no compression)
    // Snappy provides good compression with low CPU overhead.
    // DYNAMIC: Can be updated at runtime via UpdateConfig()
    CompressionCodec CompressionCodec

    // Linger sets how long to wait for additional messages before sending a batch.
    // Zero or negative values disable lingering (send immediately).
    // Default: 0 (no linger)
    // Example: 10*time.Millisecond trades latency for higher throughput
    // DYNAMIC: Can be updated at runtime via UpdateConfig()
    Linger time.Duration

    // Acks controls the number of broker acknowledgments required.
    // Valid values: "all" (all ISR replicas), "leader" (leader only), "none" (no acks)
    // Default: "" (franz-go default, typically "all")
    // "all" provides strongest durability guarantees.
    // DYNAMIC: Can be updated at runtime via UpdateConfig()
    Acks Acks
}

// DynamicConfig contains configuration fields that can be updated at runtime
// without restarting the Publisher. Use UpdateConfig() to apply changes.
type DynamicConfig struct {
    // TopicMap defines routing rules from WRP event types to Kafka topics.
    // Patterns are evaluated in order; first match wins.
    // Must not be empty.
    TopicMap []TopicRoute

    // Headers defines additional Kafka record headers to include.
    // Optional. Map keys are header names, values are either:
    //   - Literal strings (e.g., "my-app-v1.0")
    //   - WRP field references (e.g., "wrp.Source", "wrp.TransactionUUID")
    Headers map[string]string

    // CompressionCodec specifies the compression algorithm for message payloads.
    // Valid values: "snappy", "gzip", "lz4", "zstd", "none"
    CompressionCodec CompressionCodec

    // Linger sets how long to wait for additional messages before sending a batch.
    // Zero or negative values disable lingering (send immediately).
    Linger time.Duration

    // Acks controls the number of broker acknowledgments required.
    // Valid values: "all" (all ISR replicas), "leader" (leader only), "none" (no acks)
    Acks Acks
}

type CompressionCodec string

const (
    CompressionSnappy CompressionCodec = "snappy"
    CompressionGzip   CompressionCodec = "gzip"
    CompressionLz4    CompressionCodec = "lz4"
    CompressionZstd   CompressionCodec = "zstd"
    CompressionNone   CompressionCodec = "none"
)

type Acks string

const (
    AcksAll    Acks = "all"
    AcksLeader Acks = "leader"
    AcksNone   Acks = "none"
)

type TopicRoute struct {
    Pattern     string                       // Glob pattern to match event types (case-sensitive by default)
    CaseInsensitive  bool                         // If true, pattern matching is case-insensitive
    Topic       string                       // Single target topic (mutually exclusive with Topics)
    Topics      []string                     // Multiple target topics (mutually exclusive with Topic)
    TopicShardStrategy     string                       // `"roundrobin"`, `"metadata:<fieldname>"`, `"deviceid"`, or empty (only valid with Topics)

    // Internal state (initialized during New(), not from config)
    roundRobinCounter *atomic.Uint64         // Counter for round-robin distribution (only used when `TopicShardStrategy="roundrobin"`)
}
```

#### 1.5.2 Publisher

The main type that publishes events to Kafka:

```go
type Publisher struct {
    // Static configuration (immutable after New())
    client              *kgo.Client
    logger              *zap.Logger

    // Dynamic configuration (atomic access)
    dynamicConfig       atomic.Pointer[DynamicConfig]

    // Optional metric callbacks (nil-safe)
    messagesPublished   func(eventType, topic, shardStrategy string)
    messageErrored      func(eventType, topic, shardStrategy, errorType string)
    publishLatency      func(eventType, topic, shardStrategy string, duration time.Duration)
}

// New creates a new Publisher with the provided options.
// Does not connect to Kafka - call Start() to begin operation.
// WithConfig() is required - returns error if not provided.
// WithLogger() is optional - defaults to zap.NewNop() if not provided.
func New(opts ...Option) (*Publisher, error) {
    // Create publisher with defaults (logger = zap.NewNop())
    // Apply all options
    // Call validate() to check configuration
    // Returns error if WithConfig() not called or validation fails
}

// validate is a package-private function that validates the Publisher configuration.
// Returns error if configuration is invalid.
func validate(p *Publisher) error {
    // Validates all configuration requirements (see validation rules below)
}

// Start connects to Kafka and begins accepting events.
// Returns error if connection fails or configuration is invalid.
func (p *Publisher) Start() error

// Stop gracefully shuts down the publisher and flushes buffered messages.
// Blocks until all buffered messages are sent or timeout occurs.
func (p *Publisher) Stop()

// Produce publishes a WRP message to Kafka. The message is processed according
// to its QualityOfService field, which determines the routing strategy and
// delivery guarantees. Depending on the QoS level, the method may block until
// the message is accepted or dropped. The outcome of the operation is returned
// as an Outcome enum, along with any error for critical failures.
//
// QoS-aware routing:
//   - 0-24 (Low): Fire-and-forget via TryProduce, drops if buffer full
//   - 25-74 (Medium/High): Async delivery via Produce, waits if buffer full
//   - 75-99 (Critical): Synchronous via ProduceSync, waits for ack
//
// Returns:
//   - Outcome: What happened (Accepted, Queued, Dropped, Failed)
//   - error: Non-nil only for QoS 75-99 synchronous failures
func (p *Publisher) Produce(ctx context.Context, msg *wrp.Message) (Outcome, error)

// BufferedRecords returns the current and maximum buffered record counts.
// Thread-safe. Zero overhead - simply calls franz-go's client.BufferedProduceRecords().
// Returns (0, 0) if MaxBufferedRecords is 0 (buffering disabled).
// Application can calculate utilization as: float64(current) / float64(max)
func (p *Publisher) BufferedRecords() (current int, max int)

// UpdateConfig atomically updates the dynamic configuration.
// The update takes effect immediately for new Produce() calls.
// In-flight messages continue using the previous configuration.
//
// This method validates the new configuration before applying it:
//   - TopicMap must not be empty
//   - All patterns must follow simplified syntax
//   - TopicRoute combinations must be valid
//   - TopicShardStrategy values must be valid
//   - CompressionCodec and Acks must be valid enum values
//
// Round-robin counters are initialized for new routes with TopicShardStrategy="roundrobin".
// Note: Existing routes that match the old config will reset their counters.
//
// Thread-safe: Can be called concurrently with Produce().
func (p *Publisher) UpdateConfig(next DynamicConfig) error

// Key methods (simplified - implementation details omitted):
// - getEventType: Extracts event type from WRP Destination
// - matchTopicRoute: Finds matching TopicRoute using glob patterns
// - selectTopic: Chooses topic based on TopicShardStrategy strategy
//   - For round-robin: uses route.roundRobinCounter directly (no map lookup)
// - getDeviceID: Extracts device ID for partition key
// - extractHeaders: Builds Kafka headers from config
```

**Responsibilities**:
- Convert `wrp.Message` to Kafka records
- Extract event type from WRP Destination field (event-type portion only, see [wrp-go](https://github.com/xmidt-org/wrp-go) for details)
- Match event type to TopicRoute using glob patterns (configured order, first match wins)
- Select specific topic from route's Topics list based on TopicShardStrategy strategy
- Extract device ID for partition key (ensures ordering per device, see [wrp-go](https://github.com/xmidt-org/wrp-go) for Source field format)
- Extract configured headers from WRP message (see [wrp-go](https://github.com/xmidt-org/wrp-go) for field definitions)
- Encode WRP messages (msgpack format)
- **Produce()**: QoS-aware routing - inspects msg.QualityOfService and routes to appropriate [franz-go](https://github.com/twmb/franz-go) method
  - QoS 0-24: Uses TryProduce (drops if buffer full, returns Dropped)
  - QoS 25-74: Uses Produce (waits if buffer full, returns Queued)
  - QoS 75-99: Uses ProduceSync (waits for ack, returns Accepted/Failed)
- Record metrics for successes/failures (nil-safe - checks before invoking callbacks)
- Thread-safe: Produce() method can be called concurrently from multiple goroutines

**Initialization**:
- TopicMap order is preserved as configured (first match wins)
- Validates all patterns at startup (must follow simplified pattern syntax)
- Validates TopicShardStrategy values at startup (must be `""`, `"roundrobin"`, `"deviceid"`, or `"metadata:<field>"`)
- Initializes `roundRobinCounter` on each TopicRoute where `TopicShardStrategy="roundrobin"` (eliminates map lookup overhead)
- Stores headers config as-is (processed at message publish time)

**Validation Rules** (checked by `validate()` function in `New()`):

1. **Required Configuration**:
   - `Config` must be set (WithConfig() must be called)
   - `Brokers` list must not be empty
   - `TopicMap` must not be empty

2. **TopicRoute Validation** (for each route):
   - `Pattern` must be non-empty
   - Pattern must follow simplified syntax (exact match, `*`, prefix with trailing `*`, or escaped `\*`)
   - Exactly one of the following two combinations:
     - **Single topic**: `Topic` set, `Topics` empty, `TopicShardStrategy` empty
     - **Multi-topic**: `Topics` set and non-empty, `Topic` empty, `TopicShardStrategy` set
   - `TopicShardStrategy` (when used) must be one of: `"roundrobin"`, `"deviceid"`, or `"metadata:<fieldname>"`
   - If `TopicShardStrategy` is set, `Topics` must have at least 1 topic

3. **Enum Validation**:
   - `CompressionCodec` must be one of the defined enum values (if set)
   - `Acks` must be one of the defined enum values (if set)

4. **Headers Validation**:
   - Header keys must be non-empty
   - Header values starting with "wrp." are validated against known WRP field names (warnings logged, not errors)

5. **Numeric Validation**:
   - No validation needed - zero/negative values have defined behaviors:
     - `MaxBufferedRecords < 1`: buffering disabled
     - `RequestTimeout <= 0`: no timeout
     - `Linger <= 0`: no linger
     - `MaxRetries`: -1 (no retries), 0 (unlimited), >0 (specific count)

After validation passes, `New()` initializes round-robin counters on routes where applicable.

### 1.6 Message Format

**Kafka Record Structure**:
```
Key:    Device ID (string)
Value:  msgpack-encoded WRP message ([]byte)
Headers: Configurable via headers map
```

The Kafka message value is the WRP message encoded as msgpack, identical to what would be sent via HTTP to Caduceus. No envelope wrapping.

**Headers**:
- Values starting with `wrp.` extract from WRP message fields (e.g., `wrp.Source`, `wrp.Destination`)
- Other values are used as literal strings
- Empty headers map `{}` is valid (no headers)
- Invalid or empty WRP field references result in header omission

**Example Configuration**:
```yaml
headers:
  src: wrp.Source                  # Extract wrp.Message.Source field
  dst: wrp.Destination             # Extract wrp.Message.Destination field
  X-Source-Instance: talaria-prod-1  # Literal: source instance identifier
  region: us-east-1                # Literal: deployment region
```

**Common Use Cases**:
- **Instance tracing**: Add `"X-Source-Instance": "app-1"` to identify which instance sent the message
- **WRP field extraction**: Use `"wrp.FieldName"` to extract fields from the WRP message
- **Metadata tagging**: Add literal strings for environment, region, version, etc.

### 1.7 WRP Event Processing

This library processes WRP (Web Routing Protocol) events. For complete details on WRP message structure, fields, and formats, see the [wrp-go documentation](https://github.com/xmidt-org/wrp-go) and [xmidt.io](https://xmidt.io).

**Key WRP Details**:
- **Device Events**: WRP messages of type `event` contain device lifecycle and data events
- **Device ID**: Extracted from the WRP message `Source` field for use as Kafka partition key
- **Event Type**: Extracted from the WRP message `Destination` field (event-type portion)
- **Metadata**: WRP messages may contain a `Metadata` map for custom attributes
- **Message Encoding**: WRP messages are encoded as msgpack for Kafka transport

See the wrp-go repository for:
- Complete WRP message field definitions
- Event type extraction from `Destination` field format
- Device ID format and extraction from `Source` field
- Metadata structure and field types
- WRP message type enumeration

### 1.8 Topic Selection Strategies

When a TopicRoute matches an event, a specific topic must be selected. The `Topic` and `Topics` fields are mutually exclusive:

**Strategy: Single Topic** (using `Topic` field)
- Use `Topic` (singular) for the single-topic case
- `Topics` must be empty and `TopicShardStrategy` must be empty (validated at startup)
- Clearest configuration for the common single-topic scenario
- Example:
  ```yaml
  - pattern: online
    topic: device-lifecycle
  ```

**Strategy: Round Robin** (using `Topics` field with `TopicShardStrategy: "roundrobin"`)
- Use `Topics` (plural) with multiple topic targets
- Distributes messages evenly across topics in a rotating fashion
- Uses an atomic counter stored directly in the TopicRoute (no map lookup overhead)
- Counter increments on each message
- Best for load balancing across equivalent topic partitions
- Example:
  ```yaml
  - pattern: device-status
    topics: [device-status-0, device-status-1, device-status-2]
    shardBy: roundrobin
  ```

**Strategy: Device ID Sharding** (using `Topics` field with `TopicShardStrategy: "deviceid"`)
- Use `Topics` (plural) with multiple topic targets
- Hashes the device ID (from `wrp.Source`) to select a topic
- Ensures all events for a given device go to the same topic
- Provides topic-level ordering per device (in addition to partition-level ordering)
- Example:
  ```yaml
  - pattern: qos-*
    topics: [qos-shard-0, qos-shard-1, qos-shard-2]
    shardBy: deviceid
  ```

**Strategy: Metadata Field Sharding** (using `Topics` field with `TopicShardStrategy: "metadata:<fieldname>"`)
- Use `Topics` (plural) with multiple topic targets
- Hashes a specific field from `wrp.Metadata` map to select a topic
- Useful for grouping events by custom attributes (region, account, etc.)
- If the metadata field is missing or empty, falls back to round-robin
- Example:
  ```yaml
  - pattern: "*"
    topics: [events-east, events-west, events-central]
    shardBy: metadata:region
  ```

**Hashing Implementation**:
- Uses Go's `fnv` hash (FNV-1a) for consistent, fast hashing
- Formula: `hash(value) % len(topics)`
- Same input always maps to same topic index
- Stateless and deterministic

**Validation**:
- `Pattern` must be set (non-empty) for all routes
- Only two valid TopicRoute combinations:
  1. **Pattern is set, Topic is set, everything else is empty** (single topic route)
  2. **Pattern is set, Topics is set, TopicShardStrategy is set, Topic is empty** (multi-topic route with sharding)
- All other combinations are invalid and cause startup failure
- `TopicShardStrategy` must be one of: `"roundrobin"`, `"deviceid"`, or `"metadata:<fieldname>"` (when used with Topics)
- Invalid `TopicShardStrategy` values cause startup failure
- Metadata field names are not validated at startup (runtime fallback to round-robin)
- `CompressionCodec` must be one of the defined enum values
- `Acks` must be one of the defined enum values

### 1.9 Partitioning

Messages are always partitioned by device ID. This provides:
- **Ordering**: All events for a given device go to the same partition, preserving event order per device
- **Parallelism**: Different devices distribute across partitions, enabling parallel consumption
- **Flexibility**: Consumers can process events with or without state, with ordering guarantees when needed

**Partition Assignment**:
- [franz-go](https://github.com/twmb/franz-go) uses Kafka's default partitioner: `hash(device_id) % num_partitions`
- Consistent hashing ensures the same device always goes to the same partition
- No configuration needed - automatic and deterministic

**Topic Selection vs Partitioning**:
- Topic selection (TopicShardStrategy) happens first - chooses which topic
- Partition assignment happens second - chooses which partition within that topic
- Both use device ID by default, providing consistent routing at both levels

### 1.10 Topic Matching with Pattern Matching

**Event Type Extraction**:
- The event type for pattern matching is extracted from the **event-type field** of `wrp.Destination`
- See [WRP specification](https://github.com/xmidt-org/wrp-go) for Destination field format
- For example, `event:device-status/mac:112233445566` → event type is `device-status`
- For Connect/Disconnect events, the event type is `online` or `offline`

**Pattern Matching Rules**:
The library supports a simplified pattern matching syntax optimized for performance:

1. **Catch-all pattern**: `*` matches any event type
2. **Exact match**: Pattern must equal event type exactly (case-sensitive)
3. **Prefix match**: Pattern ending with `*` matches event types starting with that prefix
   - Example: `device-status-*` matches `device-status-update`, `device-status-battery`, etc.
   - Only the final character can be `*` (wildcard in middle or beginning is not supported)
4. **Escaped asterisk**: Use `\*` to match a literal `*` character in the event type
   - Example: `foo\*` matches exactly `foo*` (not a wildcard)
   - Example: `foo-\*-bar` matches exactly `foo-*-bar`

**Important Validation Notes**:
- Patterns with `*` in the middle or beginning (e.g., `foo-*-bar`, `*-suffix`) **will fail validation**
- To match event types containing literal `*` characters, use `\*` escape sequence
- This restriction reserves future design space for additional matching features while maintaining backwards compatibility

**Pattern Matching Behavior**:
- **Case Sensitivity**: All pattern matching is case-sensitive by default
  - Set `ignoreCase: true` on a route to enable case-insensitive matching for that pattern
  - When `ignoreCase: true`, both pattern and event type are lowercased before matching
- Patterns evaluated in configured order
- First match wins
- No match = error (dropped message + metric)

**Performance Characteristics**:
This simplified pattern matching is optimized for high-throughput message routing:
- Catch-all (`*`): ~2 ns/op
- Exact match: ~2 ns/op
- Prefix match (`foo-*`): ~4 ns/op
- **30x faster** than `filepath.Match` (~130 ns/op)
- **100x faster** than compiled regexp (~450 ns/op)

(Benchmarked on AMD Ryzen 9 3900X 12-Core Processor; see [pattern_bench_test.go](../pattern_bench_test.go) for details)

**Example Configuration**:
```yaml
topicMap:
  # Single topic - use Topic field (no Topics, no TopicShardStrategy)
  - pattern: online
    topic: device-lifecycle
  - pattern: offline
    topic: device-lifecycle

  # Multiple topics with round-robin distribution
  - pattern: device-status
    topics: [device-status-0, device-status-1, device-status-2]
    shardBy: roundrobin

  # Multiple topics sharded by device ID
  - pattern: qos-*
    topics: [qos-shard-0, qos-shard-1, qos-shard-2]
    shardBy: deviceid

  # Multiple topics sharded by metadata field
  - pattern: iot-*
    topics: [iot-east, iot-west, iot-central]
    shardBy: metadata:region

  # Catch-all with single topic (must be last)
  - pattern: "*"
    topic: device-events
```

**Important**: Order matters - place more specific patterns before generic wildcards.

**Validation**:
- `TopicMap` must not be empty (fails at startup)
- Each route must match one of two valid combinations:
  1. `Pattern` set, `Topic` set, everything else empty/default (single topic route)
  2. `Pattern` set, `Topics` set, `TopicShardStrategy` set, `Topic` empty (multi-topic route)
- Invalid patterns are detected at startup (must follow simplified pattern syntax)
- `TopicShardStrategy` must be one of: `"roundrobin"`, `"deviceid"`, or `"metadata:<fieldname>"` (when Topics is used)
- `Headers` map is optional (empty map is valid)
- Header values starting with `wrp.` reference WRP message fields (see [wrp-go documentation](https://github.com/xmidt-org/wrp-go))
- `CompressionCodec` must be one of the defined enum values
- `Acks` must be one of the defined enum values

### 1.11 franz-go Promise Callback Pattern

The Produce() method uses [franz-go](https://github.com/twmb/franz-go)'s promise callback pattern for asynchronous result notification (for QoS 0-74 messages).

**Promise Callback Signature**: `func(*kgo.Record, error)`

**When Called**:
- After the Kafka broker responds (success or failure)
- Asynchronously in franz-go's internal goroutines (for QoS 0-74)
- NOT during the Produce() call itself
- For QoS 75-99, result is returned synchronously (no callback)

**Callback Parameters**:
- **`*kgo.Record`**: The same Record object you passed, updated with broker metadata:
  - Partition assignment
  - Timestamp
  - Offset (if successful)
- **`error`**:
  - `nil` if successful
  - Non-nil if failed (timeout, retries exhausted, broker error, etc.)

**Important Constraints**:
- Executes in [franz-go](https://github.com/twmb/franz-go)'s internal goroutines (must be thread-safe)
- Should NOT perform long-running operations (blocks other record processing)
- Keep callback logic lightweight (metrics, logging, channel sends)

**How wrpkafka Uses Promises**:
- For QoS 0-24 and 25-74: Passes callback to [franz-go](https://github.com/twmb/franz-go)'s TryProduce/Produce methods
- Callback invokes application's `messageErrored` or `messagesPublished` metrics
- Callback tracks latency (start to broker ack)
- For QoS 75-99: Uses ProduceSync, no callback (result returned synchronously)
- All application callbacks must be thread-safe

**Buffer Utilization Tracking via On-Demand Method**:

Instead of callbacks and background polling, buffer state is exposed via a simple method:

**Method**:
- `BufferedRecords() (current int, max int)` - Returns current buffered count and max buffer size

**Implementation**:
- Simply calls [franz-go](https://github.com/twmb/franz-go)'s `client.BufferedProduceRecords()` for current count
- Returns configured `MaxBufferedRecords` as max
- Zero overhead - no background goroutines, no callbacks
- Thread-safe - can be called concurrently

**Application Integration Options**:

**Option 1: [Prometheus](https://prometheus.io/) GaugeFunc** (simplest):
```go
bufferUtilization := prometheus.NewGaugeFunc(
    prometheus.GaugeOpts{
        Name: "kafka_buffer_utilization",
        Help: "Kafka buffer utilization (0.0-1.0)",
    },
    func() float64 {
        current, max := publisher.BufferedRecords()
        if max == 0 {
            return 0.0
        }
        return float64(current) / float64(max)
    },
)
```

**Option 2: [Prometheus](https://prometheus.io/) Collector** (for multiple metrics):
```go
type bufferCollector struct {
    publisher *wrpkafka.Publisher
    descUtil   *prometheus.Desc
    descCurrent *prometheus.Desc
}

func (c *bufferCollector) Describe(ch chan<- *prometheus.Desc) {
    ch <- c.descUtil
    ch <- c.descCurrent
}

func (c *bufferCollector) Collect(ch chan<- prometheus.Metric) {
    current, max := c.publisher.BufferedRecords()

    // Report utilization percentage
    utilization := 0.0
    if max > 0 {
        utilization = float64(current) / float64(max)
    }
    ch <- prometheus.MustNewConstMetric(c.descUtil, prometheus.GaugeValue, utilization)

    // Report raw count
    ch <- prometheus.MustNewConstMetric(c.descCurrent, prometheus.GaugeValue, float64(current))
}
```

**Option 3: Manual Polling**:
```go
go func() {
    ticker := time.NewTicker(5 * time.Second)
    defer ticker.Stop()
    for range ticker.C {
        current, max := publisher.BufferedRecords()
        if max > 0 {
            utilization := float64(current) / float64(max)
            bufferUtilGauge.Set(utilization)
        }
        bufferCountGauge.Set(float64(current))
    }
}()
```

**Option 4: On-Demand**:
```go
// Check before critical operations
current, max := publisher.BufferedRecords()
if max > 0 && float64(current)/float64(max) > 0.9 {
    log.Warn("buffer nearly full", zap.Int("current", current), zap.Int("max", max))
}
```

**Benefits**:
- Zero overhead if not called
- Application controls when/how often to check
- No background goroutines in the library
- No callbacks to configure
- Returns both values - application decides how to use them
- Works perfectly with Prometheus collectors (called on scrape)
- Can report raw count AND utilization percentage
- Simpler implementation

### 1.12 WRP QoS-Aware Message Production

The library provides a single `Produce()` method that automatically routes messages based on their WRP QualityOfService field. This aligns with [WRP QoS semantics](https://xmidt.io/docs/wrp/basics/#qos-description-qos) and provides explicit outcome feedback.

#### QoS Routing Behavior

**QoS 0-24 (Low Priority - "Fire and forget"):**
- Uses [franz-go](https://github.com/twmb/franz-go)'s `TryProduce()` internally
- Returns immediately, never blocks caller
- If buffer full: returns `Dropped` with `nil` error
- If accepted: returns `Queued` with `nil` error
- Async delivery failures reported via `messageErrored` callback
- **Use case**: Best-effort telemetry, non-critical events

**QoS 25-74 (Medium/High Priority - "Enqueue and confirm"):**
- Uses [franz-go](https://github.com/twmb/franz-go)'s `Produce()` internally
- Waits if buffer full (backpressure)
- Returns `Queued` with `nil` error
- Async delivery with automatic retries until success ([franz-go](https://github.com/twmb/franz-go) retries internally)
- Success/failure reported via callbacks
- **Use case**: Standard device events, metrics

**QoS 75-99 (Critical Priority - "Any means necessary"):**
- Uses [franz-go](https://github.com/twmb/franz-go)'s `ProduceSync()` internally
- Blocks until Kafka broker acknowledges receipt
- On success: returns `Accepted` with `nil` error
- On failure: returns `Failed` with non-nil error
- Also reports via callbacks for metrics consistency
- **Use case**: Critical alerts, compliance events, audit logs

#### Outcome Types

```go
type Outcome int

const (
    // Accepted - Message delivered AND confirmed by Kafka
    // Only returned for QoS 75-99 that successfully complete
    Accepted Outcome = iota

    // Queued - Message buffered but NOT confirmed
    // For QoS 25-74: Will retry async, callback on result
    Queued

    // Attempted - Message attempted, outcome unknown
    // For QoS 0-24: Fire-and-forget (may fail async, no retry)
    Attempted

    // Dropped - Message dropped due to buffer full
    // Only reported via async callback for QoS 0-24
    Dropped

    // Failed - Synchronous delivery failed
    // Only for QoS 75-99 when Kafka rejects or times out
    Failed
)
```

#### Usage Example

```go
outcome, err := publisher.Produce(ctx, msg)

switch outcome {
case wrpkafka.Accepted:
    // QoS 75-99 only - delivery guaranteed
    logger.Info("critical message confirmed by Kafka",
        zap.String("device", msg.Source))

case wrpkafka.Queued:
    // QoS 25-74 - buffered with async retry until success
    logger.Debug("message buffered with retry",
        zap.Int("qos", msg.QualityOfService))

case wrpkafka.Attempted:
    // QoS 0-24 - fire-and-forget (outcome unknown, may fail async)
    logger.Debug("message attempted fire-and-forget",
        zap.Int("qos", msg.QualityOfService))

case wrpkafka.Dropped:
    // Only via async callback - buffer was full
    logger.Warn("low priority message dropped",
        zap.String("device", msg.Source))
    droppedMessagesCounter.Inc()

case wrpkafka.Failed:
    // QoS 75-99 only - sync delivery failed
    logger.Error("critical message delivery failed",
        zap.Error(err),
        zap.String("device", msg.Source))
    // Consider alternative delivery mechanism
    fallbackDelivery(msg)
}
```

#### Performance Characteristics

- **QoS 0-24**: Highest throughput, never blocks, may drop under load
- **QoS 25-74**: High throughput via batching, brief blocking if buffer full
- **QoS 75-99**: Lower throughput due to synchronous confirmation
- Thread-safe: can be called concurrently from multiple goroutines
- [franz-go](https://github.com/twmb/franz-go) handles batching and optimization automatically

### 1.13 Integration Example (Talaria)

This shows how to integrate the library into Talaria's existing event processing pattern. Other applications would follow similar patterns adapted to their architectures.

**Kafka Adapter for device.Listener**:
```go
// kafkaAdapter adapts wrpkafka.Publisher to Talaria's device.Listener interface
type kafkaAdapter struct {
    publisher *wrpkafka.Publisher
    logger    *zap.Logger
}

// OnDeviceEvent implements device.Listener interface
func (k *kafkaAdapter) OnDeviceEvent(event *device.Event) {
    // Extract WRP message from device event
    if event == nil || event.Message == nil {
        k.logger.Warn("received nil event or message")
        return
    }

    // Produce with QoS-aware routing (library handles QoS automatically)
    outcome, err := k.publisher.Produce(context.Background(), event.Message)

    // Handle outcome based on application needs
    switch outcome {
    case wrpkafka.Accepted:
        // Critical message confirmed - QoS 75-99
        k.logger.Debug("critical message confirmed",
            zap.String("device", event.Device.ID()))

    case wrpkafka.Queued:
        // Standard/low priority buffered - QoS 0-74
        // No action needed - async callbacks handle metrics

    case wrpkafka.Dropped:
        // Low priority dropped due to backpressure - QoS 0-24
        k.logger.Warn("low priority message dropped",
            zap.String("device", event.Device.ID()),
            zap.Int("qos", event.Message.QualityOfService))

    case wrpkafka.Failed:
        // Critical message failed - QoS 75-99
        k.logger.Error("critical message delivery failed",
            zap.Error(err),
            zap.String("device", event.Device.ID()))
        // Could trigger alternative delivery mechanism here
    }
}
```

**Extend Talaria's Outbounder Startup**:
```go
func (o *Outbounder) Start(om OutboundMeasures) ([]device.Listener, error) {
    listeners := []device.Listener{}

    // Existing HTTP dispatcher
    eventDispatcher, outbounds, err := NewEventDispatcher(om, o, nil)
    if err != nil {
        return nil, err
    }
    workerPool := NewWorkerPool(om, o, outbounds)
    workerPool.Run()
    listeners = append(listeners, eventDispatcher)

    // New Kafka publisher (if configured in this environment)
    // The application decides whether to enable Kafka by creating the publisher or not
    if kafkaConfig != nil {
        kafkaPublisher, err := wrpkafka.New(
            wrpkafka.WithConfig(kafkaConfig),
            wrpkafka.WithLogger(logger),
            wrpkafka.WithMessagesPublished(func(eventType, topic, shardStrategy string) {
                messagesPublished.WithLabelValues(eventType, topic, shardStrategy).Inc()
            }),
            wrpkafka.WithMessageErrored(func(eventType, topic, shardStrategy, errorType string) {
                messagesErrored.WithLabelValues(eventType, topic, shardStrategy, errorType).Inc()
            }),
            wrpkafka.WithPublishLatency(func(eventType, topic, shardStrategy string, duration time.Duration) {
                publishLatency.WithLabelValues(eventType, topic, shardStrategy).Observe(duration.Seconds())
            }),
        )
        if err != nil {
            return nil, err
        }

        // Start the Kafka publisher
        if err := kafkaPublisher.Start(); err != nil {
            return nil, err
        }

        // Register shutdown hook to flush buffered messages on graceful shutdown
        lc.Append(fx.Hook{
            OnStop: func(ctx context.Context) error {
                kafkaPublisher.Stop(ctx) // Blocks until messages flushed or timeout
                return nil
            },
        })

        // Wrap in adapter to implement device.Listener
        adapter := &kafkaAdapter{
            publisher: kafkaPublisher,
            logger:    logger,
        }
        listeners = append(listeners, adapter)
    }

    // Existing ack dispatcher
    ackDispatcher, err := NewAckDispatcher(om, o)
    listeners = append(listeners, ackDispatcher)

    return listeners, nil
}
```

**Configuration Loading** (using [goschtalt](https://github.com/goschtalt/goschtalt)):
```go
var kafkaConfig kafka.Config
if err := v.Unmarshal("device.outbound.kafka", &kafkaConfig); err != nil {
    return err
}
```

### 1.14 Configuration Schema
```yaml
device:
  outbound:
    # Existing HTTP configuration
    method: "POST"
    eventEndpoints:
      default: http://caduceus:6000/api/v4/notify

    # New Kafka configuration
    # Note: Whether Kafka is enabled is determined by the application
    # (by whether it creates the publisher), not by configuration
    kafka:
      # Broker connection settings
      # All brokers in the list should be part of the same Kafka cluster
      # and will use the same SASL/TLS configuration
      brokers:
        - kafka-broker-1:9092
        - kafka-broker-2:9092
        - kafka-broker-3:9092

      # Optional SASL authentication (applied to all broker connections)
      # sasl:
      #   mechanism: SCRAM-SHA-512
      #   username: talaria
      #   password: ${KAFKA_PASSWORD}

      # Optional TLS encryption (applied to all broker connections)
      # tls:
      #   enabled: true
      #   caFile: /etc/talaria/kafka-ca.pem

      # Routing: Map event patterns to topics (simplified pattern syntax)
      # Evaluated in order - first match wins
      # Pattern syntax: exact match, "*" (catch-all), "prefix-*" (prefix match), or "foo\*" (escaped asterisk)
      # Note: event type is extracted from wrp.Destination event-type field (before first /)
      topicMap:
        # Single topic examples - use topic field (not topics/shardBy)
        - pattern: online
          topic: device-lifecycle
        - pattern: offline
          topic: device-lifecycle
          # ignoreCase: true     # Optional: set to true for case-insensitive matching

        # Round-robin across multiple topics
        - pattern: device-status
          topics: [device-status-0, device-status-1, device-status-2]
          shardBy: roundrobin

        # Shard by device ID
        - pattern: qos-*
          topics: [qos-shard-0, qos-shard-1, qos-shard-2]
          shardBy: deviceid

        # Shard by metadata field
        - pattern: iot-*
          topics: [iot-east, iot-west, iot-central]
          shardBy: metadata:region

        # Catch-all with single topic (must be last)
        - pattern: "*"
          topic: device-events

      # Optional: Configurable headers to include in Kafka records
      # Values starting with "wrp." extract from WRP message, otherwise literal strings
      # Empty map {} is valid (no headers)
      headers:
        src: wrp.Source                  # Extract from WRP message
        dst: wrp.Destination             # Extract from WRP message
        X-Source-Instance: talaria-prod-1  # Literal: identifies this instance
        region: us-east-1                # Literal: deployment region

      # Producer performance settings
      # maxBufferedRecords: 0  # Default: 0 (no buffering), <1 disables buffering
      compressionCodec: snappy  # Enum: snappy, gzip, lz4, zstd, none
      # linger: 0               # Default: 0 (no linger), <=0 is no linger

      # Producer reliability settings
      acks: all                 # Enum: all, leader, none
      # Retry configuration: 0=unlimited (default), -1=no retries, >0=specific count
      maxRetries: 0
      # requestTimeout: 0       # Default: 0 (no timeout), <=0 is no timeout
```

### 1.15 Metrics

The library is decoupled from [Prometheus](https://prometheus.io/) through optional metric callback functions. Applications integrate by creating [Prometheus](https://prometheus.io/) metrics and passing them as options.

**Example [Prometheus](https://prometheus.io/) Integration**:

```go
// Define Prometheus metrics
var (
    messagesPublished = promauto.NewCounterVec(
        prometheus.CounterOpts{
            Name: "kafka_messages_published_total",
            Help: "Total number of messages published to Kafka",
        },
        []string{"event_type", "topic", "shard_strategy"},
    )

    messagesErrored = promauto.NewCounterVec(
        prometheus.CounterOpts{
            Name: "kafka_messages_errored_total",
            Help: "Total number of Kafka publish errors",
        },
        []string{"event_type", "topic", "shard_strategy", "error_type"},
    )

    publishLatency = promauto.NewHistogramVec(
        prometheus.HistogramOpts{
            Name:    "kafka_publish_duration_seconds",
            Help:    "Duration of Kafka publish operations",
            Buckets: prometheus.DefBuckets,
        },
        []string{"event_type", "topic", "shard_strategy"},
    )
)
```

**Metric Labels**:
- `event_type`: online, offline, message
- `topic`: Selected Kafka topic name (after sharding)
- `shard_strategy`: `"roundrobin"`, `"deviceid"`, `"metadata"`, `"single"` (for observability of routing)
- `error_type`: For failures (`"timeout"`, `"buffer_full"`, `"encoding_error"`, `"header_extraction_error"`, `"missing_metadata_field"`, etc.)

**Callback Invocation Details**:

**messagesPublished(eventType, topic, shardStrategy)**:
- **When**: Invoked after Kafka broker acknowledges successful write
- **QoS 0-74**: Invoked asynchronously in franz-go callback when broker acks
- **QoS 75-99**: Invoked synchronously before returning (after broker acks)
- **Thread**: May be invoked from internal franz-go goroutines (must be thread-safe)
- **Frequency**: Once per successfully published message

**messageErrored(eventType, topic, shardStrategy, errorType)**:
- **When**: Invoked when message publishing fails (any stage: encoding, routing, broker error)
- **QoS 0-74**: Invoked asynchronously in franz-go callback or immediately for pre-publish errors
- **QoS 75-99**: Invoked in addition to returning error (for metrics tracking)
- **Thread**: May be invoked from internal franz-go goroutines (must be thread-safe)
- **Frequency**: Once per failed message
- **Error Types**:
  - `"encoding_error"`: msgpack encoding failed
  - `"no_topic_match"`: No TopicRoute matched event type
  - `"buffer_full"`: Buffer capacity exceeded (QoS 0-24 only, returns Dropped outcome)
  - `"broker_error"`: Kafka broker rejected message
  - `"timeout"`: Request timeout exceeded
  - `"missing_metadata_field"`: Metadata field for sharding not found (falls back to round-robin, informational)

**publishLatency(eventType, topic, shardStrategy, duration)**:
- **When**: Invoked after message publish completes (success or failure)
- **Measurement**: From method call to Kafka broker acknowledgment
  - Start: When Produce() is called
  - End: When Kafka broker acknowledges write (or error occurs)
- **QoS 0-74**: Invoked asynchronously in franz-go callback
- **QoS 75-99**: Invoked synchronously before returning
- **Thread**: May be invoked from internal franz-go goroutines (must be thread-safe)
- **Frequency**: Once per message (successful or failed, except QoS 0-24 buffer-full)
- **Duration**: Includes encoding, routing, network latency, and broker processing time

**Buffer State via On-Demand Method**:

Instead of a callback, buffer state is accessed via a method on the Publisher:

**Method**:
- `BufferedRecords() (current int, max int)` - Returns current and max buffer counts

**Usage with Prometheus**:
```go
// Option 1: Use GaugeFunc for utilization (simplest)
bufferUtil := prometheus.NewGaugeFunc(
    prometheus.GaugeOpts{
        Name: "kafka_buffer_utilization",
        Help: "Kafka buffer utilization (0.0-1.0)",
    },
    func() float64 {
        current, max := publisher.BufferedRecords()
        if max == 0 {
            return 0.0
        }
        return float64(current) / float64(max)
    },
)

// Option 2: Use GaugeFunc for raw count
bufferCount := prometheus.NewGaugeFunc(
    prometheus.GaugeOpts{
        Name: "kafka_buffer_records",
        Help: "Number of records currently buffered",
    },
    func() float64 {
        current, _ := publisher.BufferedRecords()
        return float64(current)
    },
)
```

**Return Values**:
- `current`: Number of records currently buffered (from [franz-go](https://github.com/twmb/franz-go)'s `client.BufferedProduceRecords()`)
- `max`: Configured `MaxBufferedRecords` value
- Returns `(0, 0)` if buffering disabled (`MaxBufferedRecords` = 0)
- Utilization calculation: `float64(current) / float64(max)`

**Examples**:
- `(0, 10000)`: Buffer empty
- `(5000, 10000)`: Buffer 50% full (utilization = 0.5)
- `(10000, 10000)`: Buffer full (utilization = 1.0, QoS 25-74 wait, QoS 0-24 drop)
- `(0, 0)`: Buffering disabled

**Benefits**:
- Zero overhead if not called
- No background goroutines
- No callbacks to configure
- Returns both raw values - application decides how to use them
- Can report both utilization AND raw count
- Called on-demand (e.g., during Prometheus scrape)
- Only relevant when `MaxBufferedRecords > 0` (buffering enabled)

### 1.16 Error Handling

Error handling is QoS-aware based on the message's QualityOfService field:

#### QoS 0-24 (Low Priority - Fire and Forget)
- **Encoding Failures**: Return `Dropped` outcome with nil error, invoke `messageErrored` callback
- **Buffer Full**: Return `Dropped` outcome with nil error (expected behavior for low priority)
- **Broker Unavailable**: Retries controlled by `maxRetries` config, errors reported via callback async
- **No Topic Route Match**: Return `Dropped` outcome, invoke `messageErrored` callback
- **Missing Metadata Field**: Fall back to round-robin, log warning, invoke callback
- Uses [franz-go](https://github.com/twmb/franz-go)'s TryProduce internally - never blocks caller

#### QoS 25-74 (Medium/High Priority - Enqueue and Confirm)
- **Encoding Failures**: Return `Queued` outcome (will fail async), invoke `messageErrored` callback
- **Buffer Full**: Waits/blocks until space available, returns `Queued` outcome
- **Broker Unavailable**: Retries controlled by `maxRetries` config ([franz-go](https://github.com/twmb/franz-go) retries automatically), errors reported via callback async
- **No Topic Route Match**: Return outcome with error, invoke `messageErrored` callback
- **Missing Metadata Field**: Fall back to round-robin, log warning, invoke callback
- Uses [franz-go](https://github.com/twmb/franz-go)'s Produce internally - async with automatic retries

#### QoS 75-99 (Critical Priority - Synchronous Confirmation)
- **Encoding Failures**: Return `Failed` outcome with error
- **Buffer Full**: N/A (synchronous path bypasses buffer)
- **Broker Unavailable**:
  - Retries controlled by `maxRetries` config:
    - `-1`: No retries, return `Failed` with error immediately
    - `0`: Unlimited retries (default), blocks until success or timeout
    - `>0`: Retry up to N times, return `Failed` if exhausted
  - [franz-go](https://github.com/twmb/franz-go) handles exponential backoff automatically
- **No Topic Route Match**: Return `Failed` outcome with error
- **Missing Metadata Field**: Fall back to round-robin (same as others)
- Uses [franz-go](https://github.com/twmb/franz-go)'s ProduceSync internally - blocks for acknowledgment
- Still invokes metric callbacks in addition to returning error

#### Startup Validation
- **Empty Topics List**: Fail at startup (invalid configuration)
- **Invalid TopicShardStrategy Value**: Fail at startup (invalid configuration)
- **Invalid Configuration**: Fail at startup if TopicMap is empty, patterns are invalid, or TopicRoute combinations are invalid
- **Authentication Failure**: Fail at Start() (don't accept events until connected)

### 1.17 Testing Strategy

1. **Unit Tests**:
   - Message encoding/formatting
   - Glob pattern matching (various patterns, edge cases, case-sensitivity, ignoreCase flag)
   - Produce() method with QoS routing:
     - QoS 0-24: Returns Dropped when buffer full, Queued when accepted
     - QoS 25-74: Waits if buffer full, returns Queued
     - QoS 75-99: Blocks for ack, returns Accepted/Failed
     - Thread-safe concurrent calls
     - Correct outcome for each QoS level and error condition
   - Topic selection strategies:
     - Round-robin distribution (verify even distribution, counter in TopicRoute)
     - Verify round-robin counter initialization during New()
     - Verify thread-safe concurrent round-robin access
     - Device ID sharding (verify consistent routing per device)
     - Metadata field sharding (verify consistent routing per field value)
     - Metadata field fallback (missing/empty field → round-robin)
     - Single topic (no sharding logic)
   - Device ID extraction from events
   - Header extraction (wrp.* references, literal strings, missing/empty fields, empty config)
   - Configuration validation (validate() function):
     - **Required fields**: Config must be set, Brokers non-empty, TopicMap non-empty
     - **TopicRoute validation**:
       - Pattern must be set (non-empty)
       - Pattern must follow simplified syntax (exact, `*`, prefix-`*`, or escaped `\*`)
       - Only two valid combinations (single topic vs multi-topic)
       - Topic/Topics mutual exclusivity
       - Empty Topics list in multi-topic route
       - TopicShardStrategy required for multi-topic, empty for single-topic
       - Invalid TopicShardStrategy values (must be `"roundrobin"`, `"deviceid"`, or `"metadata:<field>"`)
     - **Enum validation**:
       - Invalid CompressionCodec enum values
       - Invalid Acks enum values
     - **Header validation**:
       - Empty header keys
       - WRP field name validation (wrp.* references)
     - **Numeric fields**: Zero/negative handling (no validation errors, defined behaviors)
     - CaseInsensitive flag behavior
   - Options pattern:
     - **WithConfig() required**: New() returns error if not called
     - **WithLogger() optional**: Defaults to zap.NewNop() if not provided
     - **New() with no options**: Returns validation error (no Config)
     - Test all individual metric options (WithMessagesPublished, etc.)
     - Test option ordering/precedence
     - Test all validation error cases from validate()
   - Metric recording:
     - Verify callbacks invoked with correct parameters
     - Nil-safety (all callbacks nil, partial callbacks nil)
     - Verify shard_strategy label included
   - Buffer state tracking:
     - Verify BufferedRecords() returns correct current and max values
     - Verify returns (0, 0) when MaxBufferedRecords = 0
     - Verify thread-safe concurrent calls
     - Test utilization calculation (current/max)

2. **Integration Tests**:
   - Mock Kafka broker using testcontainers
   - Test Produce() with different QoS levels:
     - QoS 0-24: Verify Dropped outcome when buffer full, async delivery when accepted
     - QoS 25-74: Verify Queued outcome, backpressure/waiting when buffer full
     - QoS 75-99: Verify Accepted outcome on success, Failed on error, synchronous behavior
     - Verify error callbacks invoked correctly for each QoS level
     - High-volume throughput testing across all QoS levels
   - Verify message delivery to correct topics (all sharding strategies)
   - Verify partition assignment by device ID
   - Test message ordering (same device ID goes to same partition within selected topic)
   - Multi-topic routing with different strategies

3. **Load Tests**:
   - Measure throughput vs HTTP delivery
   - Verify backpressure handling
   - Memory usage under load
   - Performance of glob matching at scale
   - Performance impact of topic selection (hashing vs round-robin)
   - Verify round-robin distribution under load

## 2. Advantages of This Library Design

1. **Reusability**: Not tied to Talaria - works directly with wrp.Message for maximum flexibility
2. **Non-Breaking** (for Talaria): Existing HTTP delivery continues unchanged when integrated via adapter
3. **WRP QoS Integration**:
   - Single Produce() method automatically handles WRP QualityOfService semantics
   - QoS 0-24: Fire-and-forget (drops if buffer full)
   - QoS 25-74: Async with automatic retries (waits if buffer full, franz-go retries)
   - QoS 75-99: Synchronous confirmation (blocks for ack)
   - Explicit outcome feedback (Accepted, Queued, Dropped, Failed)
   - No manual QoS mapping needed - library understands WRP semantics
4. **Performance**:
   - [franz-go](https://github.com/twmb/franz-go) is highly optimized, async by default
   - QoS-based routing uses optimal [franz-go](https://github.com/twmb/franz-go) method for each priority level
   - Low priority (QoS 0-24): Never blocks, maximum throughput
   - Medium/High (QoS 25-74): Batching for high throughput with reliability
   - Critical (QoS 75-99): Synchronous confirmation when needed
   - Round-robin counters stored in TopicRoute (no map lookup overhead)
   - Zero-overhead buffer utilization via on-demand BufferedRecords() method
5. **Reliability**:
   - Built-in retries, buffering, and compression via [franz-go](https://github.com/twmb/franz-go)
   - QoS 75-99 provides immediate error feedback for critical messages
   - QoS 0-24 fails fast when buffer full (explicit Dropped outcome)
   - QoS 25-74 retries async until success
6. **Clean API Design**:
   - Functional options pattern for configuration and runtime dependencies
   - `WithConfig()` option accepts structs loaded from YAML
   - Individual `With*` options for logger and metric callbacks
   - Single `New()` constructor handles everything
   - Optional callbacks (all are nil-safe)
7. **Observability**:
   - Comprehensive metrics including topic selection strategy
   - Decoupled from Prometheus via callback interface
   - Works with any metrics backend (Prometheus, StatsD, custom, etc.)
   - Metrics are optional (all callbacks nil-safe)
8. **Ordering**: Automatic per-device ordering via device_id partition key at both topic and partition levels
9. **Flexible Routing**: Glob patterns allow sophisticated topic routing without code changes
10. **Scalability**:
   - Kafka handles much higher throughput than HTTP webhooks
   - Multiple topic sharding strategies enable horizontal scaling
   - Round-robin for load balancing
   - Device/metadata sharding for consistent routing
11. **Operational Control**: Topic selection strategies configurable without code changes, enabling:
   - Gradual migration across topic sets
   - Regional/datacenter isolation via metadata sharding
   - Load distribution across topic partitions
12. **Library Independence**: No hard dependencies on metrics or logging frameworks beyond interfaces
13. **Testability**: Options pattern makes testing easy with mock dependencies

## 3. Operational Considerations

1. **Kafka Cluster Requirements**:
   - Minimum 3 brokers for HA
   - Appropriate topic configurations (partitions, replication factor)
   - Monitoring (lag, throughput, errors)

2. **Migration Path** (for existing deployments):
   - Deploy code with Kafka publisher creation commented out or conditionally disabled
   - Enable via feature flag or environment-specific configuration
   - Monitor metrics and compare with existing delivery mechanisms
   - Gradually increase adoption across environments

3. **Rollback Strategy**:
   - Remove publisher creation from startup code or disable via feature flag
   - Restart application instances
   - No configuration file changes needed (config remains valid)

4. **Topic Selection Strategy Planning**:
   - **Single Topic**: Simplest approach, good for low-volume or uniform event types
   - **Round-Robin**: Use for load balancing when event order across devices doesn't matter
   - **Device ID Sharding**: Use when per-device ordering must span topic boundaries
   - **Metadata Sharding**: Use for regional/datacenter isolation or tenant separation
   - Monitor `shard_strategy` metric label to observe distribution patterns
   - Validate metadata field availability before using metadata sharding in production

## 4. Use Case: Talaria Integration

This library is initially designed for integration with Talaria, an xmidt-org WebSocket connection manager. Understanding Talaria's architecture helps illustrate the library's purpose and integration pattern.

### 4.1 Talaria's Current Event Flow
1. **Device Events** → Devices generate events via WebSocket, and Talaria generates some events internally (e.g., online/offline)
2. **Event Processing Pattern** → `device.Listener` interface processes events through:
   - `eventDispatcher`: Converts events to HTTP requests, queues them
   - `ackDispatcher`: Handles QOS acknowledgments back to devices
3. **WorkerPool** → Processes outbound HTTP request queue (100 workers default)
4. **HTTP Transport** → Sends to configured endpoints (typically Caduceus)

### 4.2 Key Integration Points
- **Listener interface**: Simple `device.Listener` interface with `OnDeviceEvent(*device.Event)` method
  - Applications implement a thin adapter that extracts `wrp.Message` from `device.Event` and calls `Send()` or `Transfer()`
  - The library works directly with `wrp.Message` for maximum flexibility
  - Other applications can provide their own event sources
- **Configuration pattern**: YAML-based configuration using [goschtalt](https://github.com/goschtalt/goschtalt)
- **Metrics pattern**: Applications provide metrics via callback functions

## 5. Dependencies

- **[franz-go](https://github.com/twmb/franz-go)**: `github.com/twmb/franz-go` (latest stable: v1.18+)
  - `franz-go/pkg/kgo`: Core Kafka client
  - `franz-go/pkg/sasl`: SASL authentication (for Config.SASL field)
  - `franz-go/pkg/kadm`: Admin operations (optional, for topic creation)
- **crypto/tls**: Go standard library TLS (for Config.TLS field)
- **[wrp-go](https://github.com/xmidt-org/wrp-go)**: `github.com/xmidt-org/wrp-go` - WRP message definitions and processing

## 6. Alternatives Considered

1. **Tightly couple to Talaria**: Rejected - library should be reusable
2. **Use [Sarama](https://github.com/IBM/sarama)**: Rejected - [franz-go](https://github.com/twmb/franz-go) has better performance and simpler API
3. **Use [Confluent Go Client](https://github.com/confluentinc/confluent-kafka-go)**: Rejected - [franz-go](https://github.com/twmb/franz-go) is pure Go, better performance
4. **Hard-code [Prometheus](https://prometheus.io/)**: Rejected - limits reusability, callback interface more flexible
5. **Traditional constructor with many parameters**: Rejected - options pattern more extensible

## 7. Success Criteria

### 7.1 Library Quality
1. **Performance**: High-throughput event publishing with low latency, suitable for production workloads
2. **Reliability**: Handle network failures gracefully with configurable retry behavior
3. **Testability**: Comprehensive unit and integration test coverage
4. **Documentation**: Complete API documentation and usage examples
5. **Minimal Dependencies**: No hard dependencies on metrics frameworks, uses callback interface for observability

### 7.2 Talaria Integration Success
1. Zero impact on existing HTTP delivery performance when running in parallel
2. Comprehensive metrics visible in monitoring system
3. Successful canary deployment in staging environment
4. Production-ready observability and error handling

