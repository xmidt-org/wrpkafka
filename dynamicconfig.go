// SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package wrpkafka

import (
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/xmidt-org/wrp-go/v5"
)

// DynamicConfig is the runtime-updatable configuration subset.
// Can be modified via UpdateConfig() without restart.
type DynamicConfig struct {
	// TopicMap defines routing rules.
	// Required. Must not be empty.
	TopicMap []TopicRoute

	// Headers defines Kafka record headers.
	// Optional. Empty map {} is valid.
	// Multiple values per key are supported (e.g., multiple sources for same header).
	Headers map[string][]string

	// CompressionCodec specifies the compression algorithm.
	// Valid: "snappy", "gzip", "lz4", "zstd", "none".
	CompressionCodec Compression

	// Linger sets the batching delay.
	// Zero or negative values disable lingering.
	Linger time.Duration

	// Acks controls broker acknowledgments.
	// Valid: "all", "leader", "none".
	Acks Acks
}

// match finds the first matching topic for the given WRP message.
// Returns the topic name, sharding strategy, or an error if no match found.
func (dc *DynamicConfig) match(msg *wrp.Message) (string, TopicShardStrategy, error) {

	for _, route := range dc.TopicMap {
		if dc.routeMatches(msg, &route) {
			// Match found
			topic := route.selectTopic(msg)
			if topic == "" {
				return "", route.TopicShardStrategy,
					errors.New("no topic selected for message")
			}

			// Success - return first match
			return topic, route.TopicShardStrategy, nil
		}
	}

	return "", TopicShardNone, errors.Join(
		ErrNoTopicMatch,
		fmt.Errorf("no topic route matched for message"),
	)
}

func (dc *DynamicConfig) matches(msg *wrp.Message) ([]string, []TopicShardStrategy, error) {
	var topics []string
	var shardStrategy []TopicShardStrategy

	for _, route := range dc.TopicMap {
		if dc.routeMatches(msg, &route) {
			// Match found
			topic := route.selectTopic(msg)
			if topic == "" {
				return nil, nil, errors.New("no topic selected for message")
			}

			// Success
			topics = append(topics, topic)
			shardStrategy = append(shardStrategy, route.TopicShardStrategy)
		}
	}

	if len(topics) == 0 {
		return nil, nil, errors.Join(
			ErrNoTopicMatch,
			fmt.Errorf("no topic route matched for message"),
		)
	}

	return topics, shardStrategy, nil
}

// routeMatches checks if a TopicRoute matches the given WRP message.
// Uses the route's patterns matching
// All RegexFields must match for the route to match.
func (dc *DynamicConfig) routeMatches(msg *wrp.Message, route *TopicRoute) bool {

	// Check all RegexFields - all must match for the route to match
	for _, fieldName := range route.Patterns.RegexFields {
		fieldValue := dc.extractFieldValue(msg, fieldName)
		if fieldValue == "" {
			return false // Field not found or empty
		}

		// Check if any pattern matches this field value
		fieldMatches := false
		for _, matcher := range route.matchers {
			if matcher.matches(fieldValue) {
				fieldMatches = true
				break
			}
		}

		if !fieldMatches {
			return false // This required field didn't match any pattern
		}
	}

	return true // All required fields matched
}

// extractFieldValue extracts a field value from a WRP message based on field name.
// Returns empty string if field not found or parsing fails.
func (dc *DynamicConfig) extractFieldValue(msg *wrp.Message, fieldName string) string {
	switch fieldName {
	case "destination.authority":
		if locator, err := wrp.ParseLocator(msg.Destination); err == nil {
			return locator.Authority
		}
	case "source.authority":
		if locator, err := wrp.ParseLocator(msg.Source); err == nil {
			return locator.Authority
		}
	case "destination": // Shorthand for destination.authority
		if locator, err := wrp.ParseLocator(msg.Destination); err == nil {
			return locator.Authority
		}
	case "source": // Shorthand for source.authority
		if locator, err := wrp.ParseLocator(msg.Source); err == nil {
			return locator.Authority
		}
	}

	return ""
}

// headers builds the Kafka record headers from the DynamicConfig and WRP message.
// Headers can be literal values or wrp.* references to WRP message fields.
// For multi-valued WRP fields (like PartnerIDs), creates multiple headers with the same key.
// Multiple values per key are supported (e.g., multiple sources for same header).
// Returns nil-safe slice of kgo.RecordHeader.
func (dc *DynamicConfig) headers(msg *wrp.Message) []kgo.RecordHeader {
	// Estimate 2 values per key on average
	headers := make([]kgo.RecordHeader, 0, len(dc.Headers)*2)

	for key, values := range dc.Headers {
		// Process each value for this header key
		for _, value := range values {
			// Check if value is a WRP field reference (starts with "wrp.")
			if len(value) > 4 && value[:4] == "wrp." {
				fieldName := value[4:] // Remove "wrp." prefix

				// Extract field values (single-valued fields return slice with one element)
				fieldValues := extractWRPField(msg, fieldName)
				for _, v := range fieldValues {
					if v != "" {
						headers = append(headers, kgo.RecordHeader{
							Key:   key,
							Value: []byte(v),
						})
					}
				}
			} else {
				// Literal value
				headers = append(headers, kgo.RecordHeader{
					Key:   key,
					Value: []byte(value),
				})
			}
		}
	}

	return headers
}

// compileConfig pre-compiles all pattern matchers for a DynamicConfig.
// The matcher is stored directly in each TopicRoute for efficient access.
// This optimization ensures patterns are parsed once, not on every message.
// Returns an error if compilation fails.
func (dc *DynamicConfig) compile() error {
	for i := range dc.TopicMap {
		if err := dc.TopicMap[i].compile(); err != nil {
			return err
		}
	}
	return nil
}

// validate validates the DynamicConfig according to the specification.
func (dc *DynamicConfig) validate() error {
	// Validate TopicMap
	if len(dc.TopicMap) == 0 {
		return errors.Join(ErrValidation, fmt.Errorf("topic map must not be empty"))
	}

	// Validate each TopicRoute
	for i, route := range dc.TopicMap {
		if err := route.validate(); err != nil {
			return fmt.Errorf("topic route %d: %w", i, err)
		}
	}

	// Validate CompressionCodec
	if err := validateCompression(dc.CompressionCodec); err != nil {
		return errors.Join(ErrValidation, err)
	}

	// Validate Acks
	if err := validateAcks(dc.Acks); err != nil {
		return errors.Join(ErrValidation, err)
	}

	// Validate Headers
	for key, values := range dc.Headers {
		if key == "" {
			return errors.Join(ErrValidation, fmt.Errorf("header key must not be empty"))
		}
		if len(values) == 0 {
			return errors.Join(ErrValidation, fmt.Errorf("header %q must have at least one value", key))
		}
		// Validate each header value for wrp.* field references
		for _, value := range values {
			if !isValidWRPFieldReference(value) {
				return errors.Join(ErrValidation, fmt.Errorf("header %q has invalid WRP field reference %q", key, value))
			}
		}
	}

	return nil
}
