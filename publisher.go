// SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package wrpkafka

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/xmidt-org/eventor"
	"github.com/xmidt-org/wrp-go/v5"
)

// PublishEvent represents an event when a message has been published or failed to publish.
type PublishEvent struct {
	// EventType is the event type extracted from the WRP message.
	EventType string

	// Topic is the Kafka topic the message was published to (or attempted to publish to).
	Topic string

	// TopicShardStrategy is the topic sharding strategy used ("single", "roundrobin", "deviceid", "metadata").
	TopicShardStrategy string

	// Error is the error that occurred during publishing (nil for successful publishes).
	Error error

	// ErrorType is the error classification (empty for successful publishes).
	// Values: "encoding_error", "no_topic_match", "buffer_full", "broker_error", "timeout", etc.
	ErrorType string

	// Duration is the time taken from Produce() call to completion (success or failure).
	Duration time.Duration
}

// clientFactory is a function that creates a Kafka client from options.
// This allows dependency injection for testing.
type clientFactory func(opts ...kgo.Opt) (kafkaClient, error)

// defaultClientFactory is the production client factory that uses franz-go.
func defaultClientFactory(opts ...kgo.Opt) (kafkaClient, error) {
	return kgo.NewClient(opts...)
}

// Publisher is the main type that publishes WRP messages to Kafka.
//
// Thread Safety: All methods are safe for concurrent use by multiple goroutines.
// Multiple goroutines may call Start(), Stop(), Produce(), UpdateConfig(), and
// BufferedRecords() simultaneously without external synchronization.
//
// Publisher publishes WRP messages to Kafka with flexible routing and QoS support.
type Publisher struct {
	// --- STATIC CONFIGURATION (set before Start, immutable after) ---

	// Brokers is the list of Kafka broker addresses.
	// Required. Each address must be in "host:port" format.
	Brokers []string

	// SASL configures SASL authentication.
	// Optional. If nil, no authentication is used.
	SASL sasl.Mechanism

	// TLS configures TLS encryption.
	// Optional. If nil, plaintext connections are used.
	TLS *tls.Config

	// MaxBufferedRecords sets the maximum number of records to buffer.
	// Zero or negative values disable this limit.
	// Default: 0 (no limit on record count).
	MaxBufferedRecords int

	// MaxBufferedBytes sets the maximum bytes of records to buffer.
	// Zero or negative values disable this limit.
	// Default: 0 (no limit on bytes).
	MaxBufferedBytes int

	// RequestTimeout sets the maximum time to wait for broker responses.
	// Zero or negative values mean no timeout.
	// Default: 0 (no timeout).
	RequestTimeout time.Duration

	// CleanupTimeout sets the maximum time to wait for buffered messages
	// to flush on shutdown. Zero or negative values mean no timeout.
	// Default: 0 (no timeout).
	CleanupTimeout time.Duration

	// MaxRetries controls retry behavior on broker failures.
	// <=0: No retries, fail immediately (default).
	// >0: Retry up to this many times.
	// Default: 0 (no retries, fail fast).
	MaxRetries int

	// AllowAutoTopicCreation enables automatic topic creation when publishing to non-existent topics.
	// Default: false (safer for production - prevents typos from creating topics).
	AllowAutoTopicCreation bool

	// InitialDynamicConfig contains the initial values for dynamically updatable
	// configuration. These settings can be changed at runtime via UpdateConfig()
	// without restarting. Required. Must not be empty.
	InitialDynamicConfig DynamicConfig

	// Logger is the logger instance (same interface as franz-go).
	// Optional. If nil, a no-op logger will be used.
	Logger kgo.Logger

	// InitialPublishEventListeners are event listeners registered when Start() is called.
	// These listeners receive PublishEvent notifications for all published messages.
	// For dynamic listener management after Start(), use AddPublishEventListener().
	// Optional.
	InitialPublishEventListeners []func(*PublishEvent)

	// --- INTERNAL FIELDS (not for user configuration) ---

	// logger is for internal use only.
	// The actively used logger instance (never nil, defaults to nopLogger).
	logger kgo.Logger

	// clientFactory is for internal use only (testing hook).
	// Creates Kafka clients, can be overridden for mocking in tests.
	clientFactory clientFactory

	// clientMu is for internal use only.
	// Protects the client field during Start/Stop operations.
	clientMu sync.Mutex

	// client is for internal use only.
	// The Kafka client instance, initialized in Start() and closed in Stop().
	client kafkaClient

	// dynamicConfig is for internal use only.
	// Holds runtime-updatable configuration with compiled pattern matchers.
	// Updated atomically via UpdateConfig() for lock-free reads.
	dynamicConfig atomic.Pointer[DynamicConfig]

	// publishEventListeners is for internal use only.
	// Event broadcaster for PublishEvent notifications.
	publishEventListeners eventor.Eventor[func(*PublishEvent)]

	// registerInitialListenersOnce is for internal use only.
	// Ensures InitialPublishEventListeners are registered exactly once.
	registerInitialListenersOnce sync.Once
}

// AddPublishEventListener adds a listener for when a message has been either
// published or failed to be published.
//
// The listener function receives a PublishEvent containing:
//   - EventType: Event type from WRP message
//   - Topic: Kafka topic (attempted or actual)
//   - TopicShardStrategy: Topic sharding strategy used
//   - Error: nil for success, error for failures
//   - ErrorType: Error classification (empty for success)
//   - Duration: Time from Produce() call to completion
//
// The optional cancel parameter allows the caller to receive a function that
// can be called to remove the listener.
//
// Listeners are called from internal goroutines and must be thread-safe.
// Multiple listeners can be registered by calling this option multiple times.
func (p *Publisher) AddPublishEventListener(fn func(*PublishEvent)) func() {
	return p.publishEventListeners.Add(fn)
}

// Start connects to Kafka and begins operation.
// Must be called before Produce().
// Validates configuration and initializes the dynamic config.
//
// Returns an error if:
//   - Configuration is invalid (missing brokers, invalid TopicMap, etc.)
//   - Cannot connect to brokers
//   - Authentication failure (SASL/TLS)
//   - Already started
func (p *Publisher) Start() error {
	p.clientMu.Lock()
	defer p.clientMu.Unlock()

	if p.client != nil {
		return ErrAlreadyStarted
	}

	// Set default client factory if not configured
	if p.clientFactory == nil {
		p.clientFactory = defaultClientFactory
	}

	// Set default logger if not configured
	logger := p.Logger
	if logger == nil {
		logger = &nopLogger{}
	}
	p.logger = logger

	// Register initial event listeners (only once, even if Start() is called multiple times)
	p.registerInitialListenersOnce.Do(func() {
		for _, listener := range p.InitialPublishEventListeners {
			p.publishEventListeners.Add(listener)
		}
	})

	// Validate configuration
	if err := p.validate(); err != nil {
		return err
	}

	// Initialize dynamic config (compile patterns, validate)
	if err := p.UpdateConfig(p.InitialDynamicConfig); err != nil {
		return err
	}

	// Build franz-go client options from config
	opts := p.toKgoOpts()

	// Create client using factory (allows testing)
	client, err := p.clientFactory(opts...)
	if err != nil {
		return fmt.Errorf("failed to create Kafka client: %w", err)
	}

	p.client = client
	p.logger.Log(kgo.LogLevelInfo, "Publisher started successfully")

	return nil
}

// Stop gracefully shuts down and flushes buffered messages.
// Blocks until messages are sent or timeout occurs.
// Safe to call multiple times (idempotent).
func (p *Publisher) Stop(ctx context.Context) {
	p.clientMu.Lock()
	defer p.clientMu.Unlock()

	if p.client == nil {
		return // Already stopped or never started
	}

	p.logger.Log(kgo.LogLevelInfo, "Stopping publisher, flushing buffered messages")

	// Apply CleanupTimeout only if the context doesn't already have a deadline.
	// This respects caller-provided timeouts while providing a sensible default.
	if p.CleanupTimeout > 0 {
		if _, hasDeadline := ctx.Deadline(); !hasDeadline {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, p.CleanupTimeout)
			defer cancel()
		}
	}

	// Flush all buffered records
	if err := p.client.Flush(ctx); err != nil {
		p.logger.Log(kgo.LogLevelWarn, "flush incomplete during shutdown", "error", err.Error())
	}

	// Close the client
	p.client.Close()
	p.client = nil

	p.logger.Log(kgo.LogLevelInfo, "Publisher stopped successfully")
}

// Produce publishes a WRP message to Kafka with QoS-aware routing.
//
// QoS behavior:
//   - 0-24 (inclusive): Fire-and-forget, returns Attempted immediately (actual result via callback)
//   - 25-74 (inclusive): Async with retries, waits if buffer full, returns Queued
//   - 75-99 (inclusive): Sync with confirmation, blocks for ack, returns Accepted or Failed
//
// Returns:
//   - Outcome: What happened (Accepted, Queued, Attempted, Failed)
//   - error: Non-nil only for QoS 75-99 synchronous failures and pre-flight errors
func (p *Publisher) Produce(ctx context.Context, msg *wrp.Message) (Outcome, error) {
	if ctx.Err() != nil {
		return Failed, ctx.Err()
	}

	startTime := time.Now()

	// Initialize event with what we know early
	event := PublishEvent{
		EventType: eventType(msg),
	}

	// Get client reference while holding lock (brief hold)
	p.clientMu.Lock()
	client := p.client
	p.clientMu.Unlock()

	// Check if client is started
	if client == nil {
		p.dispatchEvent(&event, startTime, ErrNotStarted)
		return Failed, ErrNotStarted
	}

	// Build the Kafka records
	records, shardStrategies, err := p.buildRecords(msg)
	if err != nil {
		p.dispatchEvent(&event, startTime, err)
		return Failed, err
	}

	// If no records were created, return error
	if len(records) == 0 {
		err := errors.New("no records created")
		p.dispatchEvent(&event, startTime, err)
		return Failed, err
	}

	// Set topic and shard strategy from the first record for event reporting (could be multiple topics)
	event.Topic = records[0].Topic
	event.TopicShardStrategy = string(shardStrategies[0])
	qos := msg.QualityOfService

	for i, record := range records {
		// Create a copy of event for each record to track individual outcomes
		recordEvent := event
		recordEvent.Topic = record.Topic
		recordEvent.TopicShardStrategy = string(shardStrategies[i])

		if qos <= 24 {
			// Low QoS: Fire-and-forget with TryProduce
			client.TryProduce(ctx, record, func(r *kgo.Record, err error) {
				p.dispatchEvent(&recordEvent, startTime, err)
			})
			return Attempted, nil
		} else if qos <= 74 {
			// Medium QoS: Async with retry, block if buffer full
			// Provide callback to capture async errors (retries exhausted, timeout, etc.)
			client.Produce(ctx, record, func(r *kgo.Record, err error) {
				if err != nil {
					// Async error occurred - dispatch error event
					// Note: This runs in a different goroutine after the produce attempt
					asyncEvent := PublishEvent{
						EventType:          recordEvent.EventType,
						Topic:              recordEvent.Topic,
						TopicShardStrategy: recordEvent.TopicShardStrategy,
						Error:              err,
						ErrorType:          errorType(err),
						Duration:           time.Since(startTime),
					}
					p.dispatchEvent(&asyncEvent, startTime, err)
				}
			})

			// Optimistically dispatch success event (actual result delivered via callback)
			p.dispatchEvent(&recordEvent, startTime, nil)
			return Queued, nil
		} else {
			// High QoS: Sync with confirmation
			results := client.ProduceSync(ctx, record)

			if len(results) > 0 && results[0].Err != nil {
				// Broker error
				err := fmt.Errorf("broker rejected message: %w", results[0].Err)
				p.dispatchEvent(&recordEvent, startTime, err)
				return Failed, err
			}

			// Success
			p.dispatchEvent(&recordEvent, startTime, nil)
			return Accepted, nil
		}
	}
	// Should not reach here, but return error as a fallback
	err = errors.New("no records created")
	p.dispatchEvent(&event, startTime, err)
	return Failed, err
}

// eventType extracts the event type from a WRP message's Destination field.
// Returns empty string if the destination is not a valid event locator.
// Uses wrp.ParseLocator to properly parse the destination.
func eventType(msg *wrp.Message) string {
	if msg == nil {
		return ""
	}
	locator, err := wrp.ParseLocator(msg.Destination)
	if err != nil || locator.Scheme != "event" {
		return ""
	}
	return locator.Authority
}

// buildRecord builds a list of franz-go kgo.Records from a WRP message.
// Returns the records, topic shard strategies used, and any error.
func (p *Publisher) buildRecords(msg *wrp.Message) ([]*kgo.Record, []TopicShardStrategy, error) {
	dynCfg := p.dynamicConfig.Load()

	topics, shardStrategies, err := dynCfg.matches(msg)
	if err != nil {
		return nil, nil, err
	}

	// Encode message to msgpack format
	encoded, err := msg.EncodeMsgpack(nil)
	if err != nil {
		return nil, nil,
			errors.Join(
				ErrEncoding,
				fmt.Errorf("msgpack encoding failed"),
				err,
			)
	}

	// Use device ID (from WRP Source) as partition key for ordering
	deviceID, err := wrp.ParseDeviceID(msg.Source)
	if err != nil {
		return nil, nil,
			errors.Join(
				ErrValidation,
				fmt.Errorf("invalid device ID in WRP Source `%s`", msg.Source),
				err,
			)
	}

	// Create records for each topic
	records := make([]*kgo.Record, 0, len(topics))
	for _, topic := range topics {
		record := &kgo.Record{
			Topic:   topic,
			Key:     deviceID.Bytes(),
			Value:   encoded,
			Headers: dynCfg.headers(msg),
		}
		records = append(records, record)
	}

	return records, shardStrategies, nil
}

// dispatchEvent dispatches a PublishEvent to all registered listeners.
func (p *Publisher) dispatchEvent(event *PublishEvent, since time.Time, err error) {
	if err != nil {
		event.Error = err
		event.ErrorType = errorType(err)
	}
	event.Duration = time.Since(since)

	p.publishEventListeners.Visit(func(listener func(*PublishEvent)) {
		listener(event)
	})
}

// UpdateConfig atomically updates the runtime configuration.
// Validates configuration before applying.
// In-flight messages use the previous configuration.
// Initializes counters for all multi-topic routes (used for distribution and fallback).
//
// Returns an error if:
//   - TopicMap is empty
//   - Invalid patterns
//   - Invalid TopicRoute combinations
//   - Invalid enum values
func (p *Publisher) UpdateConfig(next DynamicConfig) error {
	// Validate the new configuration
	if err := next.validate(); err != nil {
		return err
	}

	// Initialize counters for all multi-topic routes
	// Counter tracks all messages through the route for distribution
	for i := range next.TopicMap {
		if len(next.TopicMap[i].Topics) > 0 {
			next.TopicMap[i].counter = &atomic.Uint64{}
		}
	}

	// Compile patterns for efficient matching
	err := next.compile()
	if err != nil {
		return fmt.Errorf("compiling config: %w", err)
	}

	// Atomic swap
	p.dynamicConfig.Store(&next)

	return nil
}

// BufferedRecords returns the current and maximum buffer counts and bytes.
// Thread-safe with zero overhead.
// Returns zeros if limits are disabled or client not started.
// O(1) operation - no locking.
//
// Returns:
//   - currentRecords: Number of records currently buffered
//   - maxRecords: Configured MaxBufferedRecords value (0 if disabled)
//   - currentBytes: Bytes currently buffered
//   - maxBytes: Configured MaxBufferedBytes value (0 if disabled)
func (p *Publisher) BufferedRecords() (currentRecords, maxRecords int, currentBytes, maxBytes int64) {
	maxRecords = p.MaxBufferedRecords
	maxBytes = int64(p.MaxBufferedBytes)

	// Get client reference while holding lock (brief hold)
	p.clientMu.Lock()
	client := p.client
	p.clientMu.Unlock()

	if client == nil {
		return 0, 0, 0, 0
	}

	currentRecords = int(client.BufferedProduceRecords())
	currentBytes = client.BufferedProduceBytes()

	return currentRecords, maxRecords, currentBytes, maxBytes
}

// validate validates the Publisher's configuration.
// Called during New() and Start() to ensure fail-fast behavior.
func (p *Publisher) validate() error {
	// Validate static configuration
	if len(p.Brokers) == 0 {
		return errors.Join(ErrValidation, fmt.Errorf("brokers list is required"))
	}

	// Validate each broker address
	for i, broker := range p.Brokers {
		if broker == "" {
			return errors.Join(ErrValidation, fmt.Errorf("broker %d is empty", i))
		}
	}

	// Validate InitialDynamicConfig
	if err := p.InitialDynamicConfig.validate(); err != nil {
		return err
	}

	return nil
}

// toKgoOpts converts the Publisher's configuration to franz-go client options.
// Returns a slice of kgo.Opt that can be passed to kgo.NewClient().
func (p *Publisher) toKgoOpts() []kgo.Opt {
	dynCfg := p.dynamicConfig.Load()

	opts := []kgo.Opt{
		kgo.SeedBrokers(p.Brokers...),
	}

	// Configure franz-go logging
	if p.logger != nil {
		opts = append(opts, kgo.WithLogger(p.logger))
	}

	// Add auto-topic creation if enabled
	if p.AllowAutoTopicCreation {
		opts = append(opts, kgo.AllowAutoTopicCreation())
	}

	// Add SASL config if provided
	if p.SASL != nil {
		opts = append(opts, kgo.SASL(p.SASL))
	}

	// Add TLS config if provided
	if p.TLS != nil {
		opts = append(opts, kgo.DialTLSConfig(p.TLS))
	}

	// Add buffering config (both limits are independent)
	if p.MaxBufferedRecords > 0 {
		opts = append(opts, kgo.MaxBufferedRecords(p.MaxBufferedRecords))
	}

	if p.MaxBufferedBytes > 0 {
		opts = append(opts, kgo.MaxBufferedBytes(p.MaxBufferedBytes))
	}

	// Add request timeout
	if p.RequestTimeout > 0 {
		opts = append(opts, kgo.RequestTimeoutOverhead(p.RequestTimeout))
	}

	// Add retry config (only if > 0)
	// <=0 = no retries (fail fast), N = retry N times
	if p.MaxRetries > 0 {
		opts = append(opts, kgo.RequestRetries(p.MaxRetries))
	}

	// Add linger time
	if dynCfg != nil && dynCfg.Linger > 0 {
		opts = append(opts, kgo.ProducerLinger(dynCfg.Linger))
	}

	// Add acks requirement
	if dynCfg != nil {
		switch dynCfg.Acks {
		case AcksAll:
			opts = append(opts, kgo.RequiredAcks(kgo.AllISRAcks()))
		case AcksLeader:
			opts = append(opts, kgo.RequiredAcks(kgo.LeaderAck()))
		case AcksNone:
			opts = append(opts, kgo.RequiredAcks(kgo.NoAck()))
		}
	}

	// Add compression
	if dynCfg != nil {
		switch dynCfg.CompressionCodec {
		case CompressionSnappy:
			opts = append(opts, kgo.ProducerBatchCompression(kgo.SnappyCompression()))
		case CompressionGzip:
			opts = append(opts, kgo.ProducerBatchCompression(kgo.GzipCompression()))
		case CompressionLz4:
			opts = append(opts, kgo.ProducerBatchCompression(kgo.Lz4Compression()))
		case CompressionZstd:
			opts = append(opts, kgo.ProducerBatchCompression(kgo.ZstdCompression()))
		case CompressionNone, "":
			opts = append(opts, kgo.ProducerBatchCompression(kgo.NoCompression()))
		}
	}

	return opts
}
