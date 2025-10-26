<!-- SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC -->
<!-- SPDX-License-Identifier: Apache-2.0 -->

# Proposal: WRP-to-Kafka Event Publisher Library using franz-go

## 1. Executive Summary

A reusable Go library (`github.com/xmidt-org/wrpkafka`) that publishes WRP (Web Routing Protocol) messages to Kafka topics using the franz-go client library. Initially designed for Talaria integration, the library provides a high-throughput, reliable event streaming mechanism that can run alongside existing delivery systems. The library is decoupled from specific metrics and logging frameworks, making it suitable for various applications beyond Talaria.

**Key Features:**
- Three publish modes aligning with franz-go API:
  - `Produce()` - async with callback, waits if buffer full (high throughput)
  - `TryProduce()` - async with callback, fails immediately if buffer full (non-blocking)
  - `ProduceSync()` - synchronous with acknowledgment (immediate feedback)
- Multiple topic sharding strategies (round-robin, device-based, metadata-based)
- Configurable via YAML with runtime dependencies via options pattern
- Metrics-agnostic with callback interface
- Per-device message ordering guarantees
- Production-ready with comprehensive error handling
- Works directly with `wrp.Message` for maximum flexibility

## 2. Use Case: Talaria Integration

This library is initially designed for integration with Talaria, an xmidt-org WebSocket connection manager. Understanding Talaria's architecture helps illustrate the library's purpose and integration pattern.

### 2.1 Talaria's Current Event Flow
1. **Device Events** → Devices generate events (Connect, Disconnect, MessageReceived) via WebSocket
2. **Dispatcher Pattern** → `device.Listener` interface processes events through:
   - `eventDispatcher`: Converts events to HTTP requests, queues them
   - `ackDispatcher`: Handles QOS acknowledgments back to devices
3. **WorkerPool** → Processes outbound HTTP request queue (100 workers default)
4. **HTTP Transport** → Sends to configured endpoints (typically Caduceus)

### 2.2 Key Integration Points
- **Dispatcher interface**: Simple `device.Listener` interface with `OnDeviceEvent(*device.Event)` method
  - Applications implement a thin adapter that extracts `wrp.Message` from `device.Event` and calls `Send()` or `Transfer()`
  - The library works directly with `wrp.Message` for maximum flexibility
  - Other applications can provide their own event sources
- **Configuration pattern**: YAML-based configuration using goschtalt
- **Metrics pattern**: Applications provide metrics via callback functions

## 3. Library Architecture

### 3.1 Design Principles
1. **Reusability**: Decoupled from specific applications, metrics, or logging frameworks
2. **Flexibility**: Supports multiple topic routing strategies (round-robin, device-based, metadata-based)
3. **Configuration-Driven**: YAML-based configuration for all routing and producer settings
4. **Observability**: Metrics via callback interface - works with any metrics backend
5. **Reliability**: Leverages franz-go's built-in retry, buffering, and compression
6. **Ordering Guarantees**: Per-device message ordering via partition keys

### 3.2 Component Design

**Design Philosophy**: The library uses a functional options pattern:
- **Configuration struct** (`Config`) for data loaded from YAML files (via goschtalt)
- **Option functions** (`WithConfig`, `WithLogger`, `WithMessagesPublished`, etc.) for configuration and runtime dependencies
- **Single `New()` constructor** that accepts variadic options

This provides clean separation between configuration and runtime dependencies, with optional metric callbacks for observability.

### 3.3 Quick Start Example

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

    // Create dispatcher with options
    // Note: WithConfig() is required - New() returns error if omitted
    // WithLogger() is optional - defaults to zap.NewNop() if not provided
    dispatcher, err := wrpkafka.New(
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

    // Start the dispatcher (connects to Kafka)
    if err := dispatcher.Start(); err != nil {
        panic(err)
    }
    defer dispatcher.Stop()

    // Create a WRP message
    msg := &wrp.Message{
        Type:        wrp.SimpleEventMessageType,
        Source:      "mac:112233445566",
        Destination: "event:device-status/mac:112233445566",
        // ... other fields
    }

    // Option 1: Async with callback, waits if buffer full (high throughput)
    dispatcher.Produce(context.Background(), msg)

    // Option 2: Async with callback, fails immediately if buffer full (non-blocking)
    if err := dispatcher.TryProduce(context.Background(), msg); err != nil {
        // Handle buffer full error
    }

    // Option 3: Sync with immediate acknowledgment (immediate feedback)
    if err := dispatcher.ProduceSync(context.Background(), msg); err != nil {
        // Handle error
    }
}
```

### 3.4 Public API Surface

The library exports the following public types and functions:

**Core Types:**
- `Config` - Configuration struct (unmarshaled from YAML)
- `TopicRoute` - Topic routing configuration
- `Option` - Functional option type

**Constructor & Options:**
- `New(opts ...Option) (*Dispatcher, error)` - Main constructor
- `WithConfig(Config) Option` - Set configuration from struct
- `WithLogger(*zap.Logger) Option` - Set logger instance
- `WithMessagesPublished(func(...) ) Option` - Metric callback for successful publishes
- `WithMessageErrored(func(...) ) Option` - Metric callback for failed publishes
- `WithPublishLatency(func(...) ) Option` - Metric callback for latency tracking

**Main Interface:**
- `(*Dispatcher) Start() error` - Start the dispatcher and connect to Kafka
- `(*Dispatcher) Stop()` - Stop the dispatcher and flush buffered messages
- `(*Dispatcher) Produce(context.Context, *wrp.Message)` - Publish a WRP message to Kafka asynchronously with callback (thread-safe, waits if buffer full)
- `(*Dispatcher) TryProduce(context.Context, *wrp.Message) error` - Publish a WRP message to Kafka asynchronously, returns error immediately if buffer full (thread-safe, non-blocking)
- `(*Dispatcher) ProduceSync(context.Context, *wrp.Message) error` - Publish a WRP message to Kafka and wait for acknowledgment (thread-safe, blocking)
- `(*Dispatcher) BufferedRecords() (current int, max int)` - Returns current buffered record count and max buffer size (thread-safe, zero overhead)

### 3.5 Component Details

#### 3.5.1 Config (Configuration)
```go
// Config contains all configuration loaded from YAML.
// This struct contains only configuration data, no runtime dependencies.
type Config struct {
    // Broker connection settings (applies to all brokers in the cluster)
    Brokers           []string               // Kafka broker addresses (host:port format, e.g., "kafka-1:9092")
    SASL              *sasl.Config           // Optional: SASL authentication from franz-go/pkg/sasl (same config for all brokers)
    TLS               *tls.Config            // Optional: TLS encryption from crypto/tls (same config for all brokers)

    // Routing settings
    TopicMap          []TopicRoute           // Event type patterns mapped to target topics
    Headers           map[string]string      // Optional: Kafka record headers - name -> "wrp.Field" or literal value

    // Producer performance settings
    MaxBufferedRecords  int                  // Default: 0 (no buffering), <1 disables buffering
    CompressionCodec    CompressionCodec     // Enum: snappy, gzip, lz4, zstd, none
    Linger              time.Duration        // Default: 0 (no linger), <=0 is no linger

    // Producer reliability settings
    Acks                Acks                 // Enum: all, leader, none
    MaxRetries          int                  // Default: 0 (unlimited retries), -1 (no retries), >0 (specific count)
    RequestTimeout      time.Duration        // Default: 0 (no timeout), <=0 is no timeout
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
    IgnoreCase  bool                         // If true, pattern matching is case-insensitive
    Topic       string                       // Single target topic (mutually exclusive with Topics)
    Topics      []string                     // Multiple target topics (mutually exclusive with Topic)
    ShardBy     string                       // "roundrobin", "metadata:<fieldname>", "deviceid", or empty (only valid with Topics)

    // Internal state (initialized during New(), not from config)
    roundRobinCounter *atomic.Uint64         // Counter for round-robin distribution (only used when ShardBy="roundrobin")
}
```

#### 3.5.2 Dispatcher

The main type that publishes events to Kafka:

```go
type Dispatcher struct {
    config              Config
    logger              *zap.Logger
    client              *kgo.Client

    // Optional metric callbacks (nil-safe)
    messagesPublished   func(eventType, topic, shardStrategy string)
    messageErrored      func(eventType, topic, shardStrategy, errorType string)
    publishLatency      func(eventType, topic, shardStrategy string, duration time.Duration)
}

// New creates a new Dispatcher with the provided options.
// Does not connect to Kafka - call Start() to begin operation.
// WithConfig() is required - returns error if not provided.
// WithLogger() is optional - defaults to zap.NewNop() if not provided.
func New(opts ...Option) (*Dispatcher, error) {
    // Create dispatcher with defaults (logger = zap.NewNop())
    // Apply all options
    // Call validate() to check configuration
    // Returns error if WithConfig() not called or validation fails
}

// validate is a package-private function that validates the Dispatcher configuration.
// Returns error if configuration is invalid.
func validate(d *Dispatcher) error {
    // Validates all configuration requirements (see validation rules below)
}

// Start connects to Kafka and begins accepting events.
// Returns error if connection fails or configuration is invalid.
func (d *Dispatcher) Start() error

// Stop gracefully shuts down the dispatcher and flushes buffered messages.
// Blocks until all buffered messages are sent or timeout occurs.
func (d *Dispatcher) Stop()

// Produce publishes a WRP message to Kafka asynchronously with callback.
// Thread-safe. If buffer is full, waits until space is available.
// Errors are reported via the messageErrored callback, not returned.
// Maps to franz-go's Produce() method.
// Use this for high-throughput scenarios where immediate feedback is not required.
func (d *Dispatcher) Produce(ctx context.Context, msg *wrp.Message)

// TryProduce publishes a WRP message to Kafka asynchronously with callback.
// Thread-safe and non-blocking. Returns error immediately if buffer is full.
// Errors are reported via the messageErrored callback AND returned if buffer full.
// Maps to franz-go's TryProduce() method.
// Use this when you want async behavior but need to know if buffer is full.
func (d *Dispatcher) TryProduce(ctx context.Context, msg *wrp.Message) error

// ProduceSync publishes a WRP message to Kafka synchronously and waits for acknowledgment.
// Thread-safe but blocking. Returns error if publish fails.
// Maps to franz-go's ProduceSync() method.
// Use this when you need immediate confirmation of success/failure.
func (d *Dispatcher) ProduceSync(ctx context.Context, msg *wrp.Message) error

// BufferedRecords returns the current and maximum buffered record counts.
// Thread-safe. Zero overhead - simply calls franz-go's client.BufferedProduceRecords().
// Returns (0, 0) if MaxBufferedRecords is 0 (buffering disabled).
// Application can calculate utilization as: float64(current) / float64(max)
func (d *Dispatcher) BufferedRecords() (current int, max int)

// Key methods (simplified - implementation details omitted):
// - getEventType: Extracts event type from WRP Destination
// - matchTopicRoute: Finds matching TopicRoute using glob patterns
// - selectTopic: Chooses topic based on ShardBy strategy
//   - For round-robin: uses route.roundRobinCounter directly (no map lookup)
// - getDeviceID: Extracts device ID for partition key
// - extractHeaders: Builds Kafka headers from config
```

**Responsibilities**:
- Convert `wrp.Message` to Kafka records
- Extract event type from WRP Destination field (event-type portion only, see wrp-go for details)
- Match event type to TopicRoute using glob patterns (configured order, first match wins)
- Select specific topic from route's Topics list based on ShardBy strategy
- Extract device ID for partition key (ensures ordering per device, see wrp-go for Source field format)
- Extract configured headers from WRP message (see wrp-go for field definitions)
- Encode WRP messages (msgpack format)
- **Produce()**: Publish asynchronously with callbacks, waits if buffer full, errors reported via messageErrored callback
- **TryProduce()**: Publish asynchronously with callbacks, returns error if buffer full (non-blocking)
- **ProduceSync()**: Publish synchronously and return error on failure
- Record metrics for successes/failures (nil-safe - checks before invoking callbacks)
- Thread-safe: All production methods can be called concurrently from multiple goroutines

**Initialization**:
- TopicMap order is preserved as configured (first match wins)
- Validates all glob patterns at startup using `filepath.Match`
- Validates ShardBy values at startup (must be "", "roundrobin", "deviceid", or "metadata:<field>")
- Initializes `roundRobinCounter` on each TopicRoute where ShardBy="roundrobin" (eliminates map lookup overhead)
- Stores headers config as-is (processed at message publish time)

**Validation Rules** (checked by `validate()` function in `New()`):

1. **Required Configuration**:
   - `Config` must be set (WithConfig() must be called)
   - `Brokers` list must not be empty
   - `TopicMap` must not be empty

2. **TopicRoute Validation** (for each route):
   - `Pattern` must be non-empty
   - Pattern must be valid for `filepath.Match`
   - Exactly one of the following two combinations:
     - **Single topic**: `Topic` set, `Topics` empty, `ShardBy` empty
     - **Multi-topic**: `Topics` set and non-empty, `Topic` empty, `ShardBy` set
   - `ShardBy` (when used) must be one of: "roundrobin", "deviceid", or "metadata:<fieldname>"
   - If `ShardBy` is set, `Topics` must have at least 1 topic

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

### 3.6 Message Format

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

### 3.7 WRP Event Processing

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

### 3.8 Topic Selection Strategies

When a TopicRoute matches an event, a specific topic must be selected. The `Topic` and `Topics` fields are mutually exclusive:

**Strategy: Single Topic** (using `Topic` field)
- Use `Topic` (singular) for the single-topic case
- `Topics` must be empty and `ShardBy` must be empty (validated at startup)
- Clearest configuration for the common single-topic scenario
- Example:
  ```yaml
  - pattern: online
    topic: device-lifecycle
  ```

**Strategy: Round Robin** (using `Topics` field with `ShardBy: "roundrobin"`)
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

**Strategy: Device ID Sharding** (using `Topics` field with `ShardBy: "deviceid"`)
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

**Strategy: Metadata Field Sharding** (using `Topics` field with `ShardBy: "metadata:<fieldname>"`)
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
  2. **Pattern is set, Topics is set, ShardBy is set, Topic is empty** (multi-topic route with sharding)
- All other combinations are invalid and cause startup failure
- `ShardBy` must be one of: "roundrobin", "deviceid", or "metadata:<fieldname>" (when used with Topics)
- Invalid `ShardBy` values cause startup failure
- Metadata field names are not validated at startup (runtime fallback to round-robin)
- `CompressionCodec` must be one of the defined enum values
- `Acks` must be one of the defined enum values

### 3.9 Partitioning

Messages are always partitioned by device ID. This provides:
- **Ordering**: All events for a given device go to the same partition, preserving event order per device
- **Parallelism**: Different devices distribute across partitions, enabling parallel consumption
- **Flexibility**: Consumers can process events with or without state, with ordering guarantees when needed

**Partition Assignment**:
- Franz-go uses Kafka's default partitioner: `hash(device_id) % num_partitions`
- Consistent hashing ensures the same device always goes to the same partition
- No configuration needed - automatic and deterministic

**Topic Selection vs Partitioning**:
- Topic selection (ShardBy) happens first - chooses which topic
- Partition assignment happens second - chooses which partition within that topic
- Both use device ID by default, providing consistent routing at both levels

### 3.10 Topic Matching with Glob Patterns

**Event Type Extraction**:
- The event type for pattern matching is extracted from the **event-type field** of `wrp.Destination`
- See [WRP specification](https://github.com/xmidt-org/wrp-go) for Destination field format
- For example, `event:device-status/mac:112233445566` → event type is `device-status`
- For Connect/Disconnect events, the event type is `online` or `offline`

**Pattern Matching**:
- Uses Go standard library `path/filepath.Match` syntax:
  - `*` - matches any sequence of characters
  - `?` - matches any single character
  - `[abc]` - character class
  - `[a-z]` - character range
  - `[!abc]` - negated character class
- **Case Sensitivity**: All pattern matching is case-sensitive by default
  - Set `ignoreCase: true` on a route to enable case-insensitive matching for that pattern
  - When `ignoreCase: true`, both pattern and event type are lowercased before matching
- Patterns evaluated in configured order
- First match wins
- No match = error (dropped message + metric)

**Example Configuration**:
```yaml
topicMap:
  # Single topic - use Topic field (no Topics, no ShardBy)
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
  2. `Pattern` set, `Topics` set, `ShardBy` set, `Topic` empty (multi-topic route)
- Invalid glob patterns are detected at startup using `filepath.Match` validation
- `ShardBy` must be one of: "roundrobin", "deviceid", or "metadata:<fieldname>" (when Topics is used)
- `Headers` map is optional (empty map is valid)
- Header values starting with `wrp.` reference WRP message fields (see [wrp-go documentation](https://github.com/xmidt-org/wrp-go))
- `CompressionCodec` must be one of the defined enum values
- `Acks` must be one of the defined enum values

### 3.11 franz-go Promise Callback Pattern

The Produce() and TryProduce() methods use franz-go's promise callback pattern for asynchronous result notification.

**Promise Callback Signature**: `func(*kgo.Record, error)`

**When Called**:
- After the Kafka broker responds (success or failure)
- Asynchronously in franz-go's internal goroutines
- NOT during the Produce/TryProduce call itself

**Callback Parameters**:
- **`*kgo.Record`**: The same Record object you passed, updated with broker metadata:
  - Partition assignment
  - Timestamp
  - Offset (if successful)
- **`error`**:
  - `nil` if successful
  - Non-nil if failed (timeout, retries exhausted, broker error, etc.)

**Important Constraints**:
- Executes in franz-go's internal goroutines (must be thread-safe)
- Should NOT perform long-running operations (blocks other record processing)
- Keep callback logic lightweight (metrics, logging, channel sends)

**How wrpkafka Uses Promises**:
- Produce() and TryProduce() pass a callback to franz-go's methods
- Callback invokes application's `messageErrored` or `messagesPublished` metrics
- Callback tracks latency (start to broker ack)
- All application callbacks must be thread-safe

**Buffer Utilization Tracking via On-Demand Method**:

Instead of callbacks and background polling, buffer state is exposed via a simple method:

**Method**:
- `BufferedRecords() (current int, max int)` - Returns current buffered count and max buffer size

**Implementation**:
- Simply calls franz-go's `client.BufferedProduceRecords()` for current count
- Returns configured `MaxBufferedRecords` as max
- Zero overhead - no background goroutines, no callbacks
- Thread-safe - can be called concurrently

**Application Integration Options**:

**Option 1: Prometheus GaugeFunc** (simplest):
```go
bufferUtilization := prometheus.NewGaugeFunc(
    prometheus.GaugeOpts{
        Name: "kafka_buffer_utilization",
        Help: "Kafka buffer utilization (0.0-1.0)",
    },
    func() float64 {
        current, max := dispatcher.BufferedRecords()
        if max == 0 {
            return 0.0
        }
        return float64(current) / float64(max)
    },
)
```

**Option 2: Prometheus Collector** (for multiple metrics):
```go
type bufferCollector struct {
    dispatcher *wrpkafka.Dispatcher
    descUtil   *prometheus.Desc
    descCurrent *prometheus.Desc
}

func (c *bufferCollector) Describe(ch chan<- *prometheus.Desc) {
    ch <- c.descUtil
    ch <- c.descCurrent
}

func (c *bufferCollector) Collect(ch chan<- prometheus.Metric) {
    current, max := c.dispatcher.BufferedRecords()

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
        current, max := dispatcher.BufferedRecords()
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
current, max := dispatcher.BufferedRecords()
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

### 3.12 Production Methods: Usage Patterns

The library provides three methods for publishing WRP messages to Kafka, aligning with franz-go's API:

#### Produce() - Asynchronous with Buffering

**Characteristics**:
- Non-blocking callback-based, returns immediately
- If buffer is full, **waits** until space is available (backpressure)
- High throughput, messages buffered and sent asynchronously
- Errors reported via `messageErrored` callback, not returned
- Metrics recorded via callback functions
- Maps to franz-go's `Produce()` method

**When to Use**:
- High-throughput scenarios where immediate feedback is not required
- Event logging, metrics, telemetry streams
- Can tolerate brief blocking if buffer fills
- Talaria's existing event flow (async by nature)

**Example**:
```go
// High throughput - buffers and batches, waits if buffer full
dispatcher.Produce(ctx, msg)
```

#### TryProduce() - Asynchronous Non-Blocking

**Characteristics**:
- Non-blocking callback-based, returns immediately
- If buffer is full, **fails immediately** with error (no waiting)
- Returns error only for buffer-full condition
- Other errors reported via `messageErrored` callback
- Maps to franz-go's `TryProduce()` method

**When to Use**:
- High-throughput scenarios that must never block
- Need to detect backpressure/buffer full conditions
- Can handle dropped messages or implement custom retry logic
- Real-time systems with strict latency requirements

**Example**:
```go
// Non-blocking - fails fast if buffer full
if err := dispatcher.TryProduce(ctx, msg); err != nil {
    // err is only ErrMaxBuffered, handle backpressure
    log.Warn("buffer full, dropping message")
}
```

#### ProduceSync() - Synchronous with Acknowledgment

**Characteristics**:
- Blocking, waits for Kafka broker acknowledgment
- Returns error immediately if publish fails
- Lower throughput due to synchronous nature
- Provides immediate success/failure feedback
- Still thread-safe, can be called concurrently
- Maps to franz-go's `ProduceSync()` method

**When to Use**:
- Critical messages where confirmation is required
- Request/response patterns
- User-facing operations where error feedback is needed
- Lower-volume, higher-reliability scenarios

**Example**:
```go
// Wait for broker confirmation
if err := dispatcher.ProduceSync(ctx, msg); err != nil {
    log.Error("failed to publish", zap.Error(err))
    // Handle error, retry, etc.
}
```

#### Performance Considerations

- **Produce()**: Highest throughput via batching, may briefly block if buffer fills
- **TryProduce()**: High throughput, never blocks, may drop messages if buffer full
- **ProduceSync()**: Each call waits for broker ack, lowest throughput
- All methods can be used concurrently from multiple goroutines
- franz-go handles batching and optimization internally

### 3.13 Integration Example (Talaria)

This shows how to integrate the library into Talaria's existing dispatcher pattern. Other applications would follow similar patterns adapted to their architectures.

**Kafka Adapter for device.Listener**:
```go
// kafkaAdapter adapts wrpkafka.Dispatcher to Talaria's device.Listener interface
type kafkaAdapter struct {
    dispatcher *wrpkafka.Dispatcher
    logger     *zap.Logger
}

// OnDeviceEvent implements device.Listener interface
func (k *kafkaAdapter) OnDeviceEvent(event *device.Event) {
    // Extract WRP message from device event
    if event == nil || event.Message == nil {
        k.logger.Warn("received nil event or message")
        return
    }

    // Option 1: Use Produce() for high throughput (waits if buffer full)
    k.dispatcher.Produce(context.Background(), event.Message)

    // Option 2: Use TryProduce() if you can't tolerate blocking
    // if err := k.dispatcher.TryProduce(context.Background(), event.Message); err != nil {
    //     k.logger.Warn("buffer full, message dropped", zap.Error(err))
    // }

    // Option 3: Use ProduceSync() if you need immediate error feedback
    // if err := k.dispatcher.ProduceSync(context.Background(), event.Message); err != nil {
    //     k.logger.Error("failed to publish message", zap.Error(err))
    // }
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

    // New Kafka dispatcher (if configured in this environment)
    // The application decides whether to enable Kafka by creating the dispatcher or not
    if kafkaConfig != nil {
        kafkaDispatcher, err := wrpkafka.New(
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

        // Start the Kafka dispatcher
        if err := kafkaDispatcher.Start(); err != nil {
            return nil, err
        }
        // TODO: Add shutdown hook to call kafkaDispatcher.Stop()

        // Wrap in adapter to implement device.Listener
        adapter := &kafkaAdapter{
            dispatcher: kafkaDispatcher,
            logger:     logger,
        }
        listeners = append(listeners, adapter)
    }

    // Existing ack dispatcher
    ackDispatcher, err := NewAckDispatcher(om, o)
    listeners = append(listeners, ackDispatcher)

    return listeners, nil
}
```

**Configuration Loading** (using goschtalt):
```go
var kafkaConfig kafka.Config
if err := v.Unmarshal("device.outbound.kafka", &kafkaConfig); err != nil {
    return err
}
```

### 3.14 Configuration Schema
```yaml
device:
  outbound:
    # Existing HTTP configuration
    method: "POST"
    eventEndpoints:
      default: http://caduceus:6000/api/v4/notify

    # New Kafka configuration
    # Note: Whether Kafka is enabled is determined by the application
    # (by whether it creates the dispatcher), not by configuration
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

      # Routing: Map event patterns to topics (filepath.Match glob patterns)
      # Evaluated in order - first match wins
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

### 3.15 Metrics

The library is decoupled from Prometheus through optional metric callback functions. Applications integrate by creating Prometheus metrics and passing them as options.

**Example Prometheus Integration**:

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
- `shard_strategy`: roundrobin, deviceid, metadata, single (for observability of routing)
- `error_type`: For failures (timeout, buffer_full, encoding_error, header_extraction_error, missing_metadata_field, etc.)

**Callback Invocation Details**:

**messagesPublished(eventType, topic, shardStrategy)**:
- **When**: Invoked after Kafka broker acknowledges successful write
- **Produce()**: Invoked asynchronously in franz-go callback when broker acks
- **TryProduce()**: Invoked asynchronously in franz-go callback when broker acks
- **ProduceSync()**: Invoked synchronously before returning (after broker acks)
- **Thread**: May be invoked from internal franz-go goroutines (must be thread-safe)
- **Frequency**: Once per successfully published message

**messageErrored(eventType, topic, shardStrategy, errorType)**:
- **When**: Invoked when message publishing fails (any stage: encoding, routing, broker error)
- **Produce()**: Invoked asynchronously in franz-go callback or immediately for pre-publish errors (encoding, no route match)
- **TryProduce()**: Not invoked for buffer_full (returned as error); invoked for other failures in franz-go callback
- **ProduceSync()**: Invoked in addition to returning error (for metrics tracking)
- **Thread**: May be invoked from internal franz-go goroutines (must be thread-safe)
- **Frequency**: Once per failed message
- **Error Types**:
  - `encoding_error`: msgpack encoding failed
  - `no_topic_match`: No TopicRoute matched event type
  - `buffer_full`: Buffer capacity exceeded (Produce only, TryProduce returns error)
  - `broker_error`: Kafka broker rejected message
  - `timeout`: Request timeout exceeded
  - `missing_metadata_field`: Metadata field for sharding not found (falls back to round-robin, informational)

**publishLatency(eventType, topic, shardStrategy, duration)**:
- **When**: Invoked after message publish completes (success or failure)
- **Measurement**: From method call to Kafka broker acknowledgment
  - Start: When Produce()/TryProduce()/ProduceSync() is called
  - End: When Kafka broker acknowledges write (or error occurs)
- **Produce()**: Invoked asynchronously in franz-go callback
- **TryProduce()**: Invoked asynchronously in franz-go callback (not invoked if buffer full)
- **ProduceSync()**: Invoked synchronously before returning
- **Thread**: May be invoked from internal franz-go goroutines (must be thread-safe)
- **Frequency**: Once per message (successful or failed, except TryProduce buffer-full)
- **Duration**: Includes encoding, routing, network latency, and broker processing time

**Buffer State via On-Demand Method**:

Instead of a callback, buffer state is accessed via a method on the Dispatcher:

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
        current, max := dispatcher.BufferedRecords()
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
        current, _ := dispatcher.BufferedRecords()
        return float64(current)
    },
)
```

**Return Values**:
- `current`: Number of records currently buffered (from franz-go's `client.BufferedProduceRecords()`)
- `max`: Configured `MaxBufferedRecords` value
- Returns `(0, 0)` if buffering disabled (`MaxBufferedRecords` = 0)
- Utilization calculation: `float64(current) / float64(max)`

**Examples**:
- `(0, 10000)`: Buffer empty
- `(5000, 10000)`: Buffer 50% full (utilization = 0.5)
- `(10000, 10000)`: Buffer full (utilization = 1.0, Produce waits, TryProduce fails)
- `(0, 0)`: Buffering disabled

**Benefits**:
- Zero overhead if not called
- No background goroutines
- No callbacks to configure
- Returns both raw values - application decides how to use them
- Can report both utilization AND raw count
- Called on-demand (e.g., during Prometheus scrape)
- Only relevant when `MaxBufferedRecords > 0` (buffering enabled)

### 3.16 Error Handling

Error handling differs between the three production methods:

#### Produce() Error Handling (Asynchronous with Backpressure)
- **Encoding Failures**: Log error, invoke `messageErrored` callback, drop message
- **Buffer Full**: Waits/blocks until space available (backpressure)
- **Broker Unavailable**: Retries controlled by `maxRetries` config, errors reported via callback
- **No Topic Route Match**: Log error, invoke `messageErrored` callback with `no_topic_match` error type
- **Missing Metadata Field**: Fall back to round-robin, log warning, invoke callback
- All errors are asynchronous - reported via `messageErrored` callback in franz-go promise, not returned

#### TryProduce() Error Handling (Asynchronous Non-Blocking)
- **Encoding Failures**: Log error, invoke `messageErrored` callback, drop message
- **Buffer Full**: Returns `ErrMaxBuffered` immediately (no callback invoked for this error)
- **Broker Unavailable**: Retries controlled by `maxRetries` config, errors reported via callback
- **No Topic Route Match**: Log error, invoke `messageErrored` callback with `no_topic_match` error type
- **Missing Metadata Field**: Fall back to round-robin, log warning, invoke callback
- Buffer-full errors returned immediately; all other errors asynchronous via callback

#### ProduceSync() Error Handling (Synchronous)
- **Encoding Failures**: Return error immediately
- **Buffer Full**: Bypasses buffer (synchronous), not applicable
- **Broker Unavailable**:
  - Retries controlled by `maxRetries` config:
    - `-1`: No retries, fail immediately, return error
    - `0`: Unlimited retries (default), blocks until success or timeout
    - `>0`: Retry up to N times, return error if exhausted
  - Franz-go handles exponential backoff automatically
- **No Topic Route Match**: Return error immediately
- **Missing Metadata Field**: Fall back to round-robin (same as others)
- All errors are synchronous - returned from the ProduceSync() call
- Still invokes metric callbacks (in addition to returning error)

#### Startup Validation (Both Methods)
- **Empty Topics List**: Fail at startup (invalid configuration)
- **Invalid ShardBy Value**: Fail at startup (invalid configuration)
- **Invalid Configuration**: Fail at startup if TopicMap is empty, patterns are invalid, or TopicRoute combinations are invalid
- **Authentication Failure**: Fail at Start() (don't accept events until connected)

### 3.17 Testing Strategy

1. **Unit Tests**:
   - Message encoding/formatting
   - Glob pattern matching (various patterns, edge cases, case-sensitivity, ignoreCase flag)
   - Production methods:
     - Produce() waits if buffer full, callback-based errors
     - TryProduce() fails immediately if buffer full, returns error + callback
     - ProduceSync() blocks until acknowledgment, returns error
     - All methods are thread-safe (concurrent calls)
     - Error handling differences (callback vs return vs mixed)
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
       - Pattern must be valid for filepath.Match
       - Only two valid combinations (single topic vs multi-topic)
       - Topic/Topics mutual exclusivity
       - Empty Topics list in multi-topic route
       - ShardBy required for multi-topic, empty for single-topic
       - Invalid ShardBy values (must be roundrobin, deviceid, or metadata:<field>)
     - **Enum validation**:
       - Invalid CompressionCodec enum values
       - Invalid Acks enum values
     - **Header validation**:
       - Empty header keys
       - WRP field name validation (wrp.* references)
     - **Numeric fields**: Zero/negative handling (no validation errors, defined behaviors)
     - IgnoreCase flag behavior
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
   - Test Produce() method:
     - Verify async message delivery to correct topics
     - Verify error callbacks are invoked on failures
     - Verify backpressure/waiting when buffer full
     - High-volume throughput testing
   - Test TryProduce() method:
     - Verify async message delivery
     - Verify immediate error return when buffer full
     - Verify callbacks for non-buffer errors
   - Test ProduceSync() method:
     - Verify synchronous acknowledgment
     - Verify errors are returned immediately
     - Test timeout and retry behavior
   - Verify message delivery to correct topics (all sharding strategies)
   - Verify partition assignment by device ID
   - Test message ordering (same device ID goes to same partition within selected topic)
   - Multi-topic routing with different strategies

3. **Load Tests**:
   - Measure throughput vs HTTP dispatcher
   - Verify backpressure handling
   - Memory usage under load
   - Performance of glob matching at scale
   - Performance impact of topic selection (hashing vs round-robin)
   - Verify round-robin distribution under load

## 4. Implementation Phases

### 4.1 Phase 1: Core Library Implementation
- [ ] Add franz-go and wrp-go dependencies
- [ ] Define enum types (CompressionCodec, Acks) with constants
- [ ] Implement options pattern (WithConfig, WithLogger, individual metric options)
- [ ] Implement glob pattern matching using `path/filepath.Match` for topic routing
  - [ ] Case-sensitive matching by default
  - [ ] Case-insensitive matching when IgnoreCase flag is set
- [ ] Implement topic selection strategies:
  - [ ] Single topic (no sharding)
  - [ ] Round-robin with atomic counters stored in TopicRoute
  - [ ] Device ID sharding with FNV hashing
  - [ ] Metadata field sharding with fallback
- [ ] Initialize round-robin counters on TopicRoute during New() for routes with ShardBy="roundrobin"
- [ ] Implement configurable header extraction (wrp.* references + literal strings, see wrp-go)
- [ ] Implement `Config` configuration struct with:
  - [ ] Topic/Topics mutual exclusivity validation
  - [ ] IgnoreCase field support
  - [ ] CompressionCodec and Acks enum validation
  - [ ] MaxBufferedRecords, RequestTimeout, Linger zero/negative handling
- [ ] Implement `Dispatcher` with:
  - [ ] Thread-safe message publishing
  - [ ] Start() and Stop() lifecycle methods
  - [ ] Produce() method - async with callback, waits if buffer full (maps to franz-go Produce)
  - [ ] TryProduce() method - async with callback, fails immediately if buffer full (maps to franz-go TryProduce)
  - [ ] ProduceSync() method - sync, waits for ack, returns error (maps to franz-go ProduceSync)
  - [ ] All methods concurrent-safe (can be called from multiple goroutines)
  - [ ] Promise callback integration with application metrics callbacks
  - [ ] BufferedRecords() method returns (current int, max int)
  - [ ] Use client.BufferedProduceRecords() for current count
  - [ ] Return configured MaxBufferedRecords as max value
- [ ] Implement device ID extraction from WRP Source field (see wrp-go)
- [ ] Add configuration validation (TopicRoute combinations, enum values, patterns)
- [ ] Implement `New()` constructor with option application and validation

### 4.2 Phase 2: Quality & Testing
- [ ] Comprehensive error handling:
  - [ ] No-match scenarios
  - [ ] Header extraction errors
  - [ ] Missing metadata field fallback
  - [ ] Empty topics list
  - [ ] Nil callback safety
- [ ] Unit tests (>80% coverage):
  - [ ] Options pattern (Config, Logger, Measures options)
  - [ ] Option precedence and validation
  - [ ] Glob matching with various patterns
  - [ ] All topic selection strategies
  - [ ] Device ID extraction
  - [ ] Header extraction (wrp.* fields, literal strings, missing/empty fields)
  - [ ] Metadata field extraction and fallback
  - [ ] Configuration validation (empty TopicMap, Topic/Topics mutual exclusivity, invalid patterns, etc.)
  - [ ] Metric callback invocations (nil-safe, correct parameters)
- [ ] API documentation (GoDoc)
- [ ] Usage examples and README

### 4.3 Phase 3: Integration & Performance
- [ ] SASL/TLS authentication support
- [ ] Integration tests with testcontainers
  - [ ] All sharding strategies
  - [ ] Partition assignment verification
  - [ ] Message ordering verification
- [ ] Performance benchmarking
  - [ ] Throughput testing
  - [ ] Latency measurements
  - [ ] Memory profiling under load
  - [ ] Comparison of sharding strategies

### 4.4 Phase 4: Talaria Integration
- [ ] Create integration example code
- [ ] Talaria-specific configuration documentation
- [ ] Deployment guide for Talaria
- [ ] Monitoring/alerting recommendations
- [ ] Canary deployment in staging environment

## 5. Advantages of This Library Design

1. **Reusability**: Not tied to Talaria - works directly with wrp.Message for maximum flexibility
2. **Non-Breaking** (for Talaria): Existing HTTP delivery continues unchanged when integrated via adapter
3. **Flexibility**:
   - Can enable Kafka per-environment or per-event-type
   - Three publish modes for different use cases:
     - Produce() - high throughput with backpressure
     - TryProduce() - non-blocking for real-time systems
     - ProduceSync() - immediate feedback for critical messages
   - Choose async/sync and blocking/non-blocking per use case
4. **Performance**:
   - franz-go is highly optimized, async by default
   - Produce() and TryProduce() provide maximum throughput via batching
   - Aligns directly with franz-go API for optimal performance
   - Round-robin counters stored in TopicRoute (no map lookup overhead)
   - Zero-overhead buffer utilization via on-demand BufferedRecords() method
5. **Reliability**:
   - Built-in retries, buffering, and compression
   - ProduceSync() provides immediate error feedback for critical messages
   - TryProduce() enables detecting and handling backpressure
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

## 6. Operational Considerations

1. **Kafka Cluster Requirements**:
   - Minimum 3 brokers for HA
   - Appropriate topic configurations (partitions, replication factor)
   - Monitoring (lag, throughput, errors)

2. **Migration Path** (for existing deployments):
   - Deploy code with Kafka dispatcher creation commented out or conditionally disabled
   - Enable via feature flag or environment-specific configuration
   - Monitor metrics and compare with existing delivery mechanisms
   - Gradually increase adoption across environments

3. **Rollback Strategy**:
   - Remove dispatcher creation from startup code or disable via feature flag
   - Restart application instances
   - No configuration file changes needed (config remains valid)

4. **Topic Selection Strategy Planning**:
   - **Single Topic**: Simplest approach, good for low-volume or uniform event types
   - **Round-Robin**: Use for load balancing when event order across devices doesn't matter
   - **Device ID Sharding**: Use when per-device ordering must span topic boundaries
   - **Metadata Sharding**: Use for regional/datacenter isolation or tenant separation
   - Monitor `shard_strategy` metric label to observe distribution patterns
   - Validate metadata field availability before using metadata sharding in production

## 7. Dependencies

- **franz-go**: `github.com/twmb/franz-go` (latest stable: v1.18+)
  - `franz-go/pkg/kgo`: Core Kafka client
  - `franz-go/pkg/sasl`: SASL authentication (for Config.SASL field)
  - `franz-go/pkg/kadm`: Admin operations (optional, for topic creation)
- **crypto/tls**: Go standard library TLS (for Config.TLS field)
- **wrp-go**: `github.com/xmidt-org/wrp-go` - WRP message definitions and processing

## 8. Alternatives Considered

1. **Tightly couple to Talaria**: Rejected - library should be reusable
2. **Use Sarama**: Rejected - franz-go has better performance and simpler API
3. **Use Confluent Go Client**: Rejected - franz-go is pure Go, better performance
4. **Hard-code Prometheus**: Rejected - limits reusability, callback interface more flexible
5. **Traditional constructor with many parameters**: Rejected - options pattern more extensible

## 9. Success Criteria

### 9.1 Library Quality
1. **Performance**: High-throughput event publishing with low latency, suitable for production workloads
2. **Reliability**: Handle network failures gracefully with configurable retry behavior
3. **Testability**: Comprehensive unit and integration test coverage
4. **Documentation**: Complete API documentation and usage examples
5. **Minimal Dependencies**: No hard dependencies on metrics frameworks, uses callback interface for observability

### 9.2 Talaria Integration Success
1. Zero impact on existing HTTP delivery performance when running in parallel
2. Comprehensive metrics visible in monitoring system
3. Successful canary deployment in staging environment
4. Production-ready observability and error handling

