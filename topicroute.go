// SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package wrpkafka

import (
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/xmidt-org/wrp-go/v5"
)

// TopicRoute defines a single routing rule that maps event type patterns to Kafka topics.
type TopicRoute struct {
	// Pattern is the glob pattern to match event types (case-sensitive by default).
	// Supports: exact match, "*" (catch-all), "prefix-*", "foo\*" (escaped).
	Pattern Pattern

	// CaseInsensitive enables case-insensitive pattern matching.
	CaseInsensitive bool

	// Topic is the single target topic (mutually exclusive with Topics).
	Topic string

	// Topics is the list of target topics for sharding (mutually exclusive with Topic).
	Topics []string

	// TopicShardStrategy specifies the sharding strategy.
	// Valid values: TopicShardNone (""), TopicShardRoundRobin, TopicShardDeviceID, or "metadata:<field>".
	// Empty string for single-topic routes (Topic field used).
	TopicShardStrategy TopicShardStrategy

	// counter is for internal use only - do not set manually.
	// Tracks messages flowing through this route for round-robin distribution.
	// Automatically initialized for all multi-topic routes during validation.
	counter *atomic.Uint64

	// matcher is for internal use only - do not set manually.
	// Compiled pattern matcher initialized during configuration validation.
	matcher *patternMatcher
}

func (route *TopicRoute) compile() error {
	if err := route.validate(); err != nil {
		return err
	}

	m, _ := route.Pattern.compile(route.CaseInsensitive)

	route.matcher = m
	return nil
}

// validate validates a single TopicRoute.
func (route *TopicRoute) validate() error {
	if err := route.Pattern.validate(); err != nil {
		return err
	}

	// Handle the single topic case first.
	if route.Topic != "" {
		if len(route.Topics) != 0 {
			return errors.Join(
				ErrValidation,
				fmt.Errorf("topic and topics are mutually exclusive"),
			)
		}

		if route.TopicShardStrategy != TopicShardNone {
			return errors.Join(
				ErrValidation,
				fmt.Errorf("topic shard strategy must be empty/none for single-topic routes"),
			)
		}
		return nil
	}

	// Deal with no topic specified.
	if len(route.Topics) == 0 {
		return errors.Join(
			ErrValidation,
			fmt.Errorf("either Topic or Topics must be set"),
		)
	}

	// Multi-topic route validation.
	if route.TopicShardStrategy == TopicShardNone {
		return errors.Join(
			ErrValidation,
			fmt.Errorf("topic shard strategy is required for multi-topic routes"),
		)
	}

	if err := validateTopicShardStrategy(route.TopicShardStrategy); err != nil {
		return errors.Join(ErrValidation, err)
	}

	return nil
}

// selectTopic selects the appropriate Kafka topic for a message based on the routing rule.
// Supports single-topic routes and multi-topic sharding strategies.
func (r *TopicRoute) selectTopic(msg *wrp.Message) string {
	// Single topic route (TopicShardNone)
	if r.Topic != "" {
		return r.Topic
	}

	// Multi-topic sharding - delegate to strategy-specific methods
	switch r.TopicShardStrategy {
	case TopicShardRoundRobin:
		return r.selectRoundRobin()

	case TopicShardDeviceID:
		return r.selectByDeviceID(msg)

	default:
		// Check if it's a metadata strategy
		if isMetadata, fieldName := r.TopicShardStrategy.IsMetadataStrategy(); isMetadata {
			return r.selectByMetadata(msg, fieldName)
		}
	}

	return ""
}

// selectRoundRobin selects a topic using round-robin distribution.
// Counter is guaranteed to be initialized for all multi-topic routes.
// Returns empty string if no topics are configured.
func (r *TopicRoute) selectRoundRobin() string {
	if len(r.Topics) == 0 {
		return ""
	}

	// Atomic increment and get index
	// Counter is always initialized for multi-topic routes (no nil check needed)
	count := r.counter.Add(1) - 1
	//nolint:gosec // G115: Modulo ensures result fits in int range
	idx := int(count % uint64(len(r.Topics)))
	return r.Topics[idx]
}

// selectByDeviceID selects a topic by hashing the device ID from WRP Source field.
// Falls back to round-robin if the device ID is missing or empty.
// Returns empty string if no topics are configured.
func (r *TopicRoute) selectByDeviceID(msg *wrp.Message) string {
	if len(r.Topics) == 0 {
		return ""
	}

	deviceID := msg.Source
	if deviceID == "" {
		// Fall back to round-robin if device ID is missing
		return r.selectRoundRobin()
	}

	idx := hashString(deviceID, len(r.Topics))
	return r.Topics[idx]
}

// selectByMetadata selects a topic by hashing a metadata field value.
// Falls back to round-robin if the field is missing or empty.
// Returns empty string if no topics are configured.
func (r *TopicRoute) selectByMetadata(msg *wrp.Message, fieldName string) string {
	if len(r.Topics) == 0 {
		return ""
	}

	var fieldValue string
	if msg.Metadata != nil {
		fieldValue = msg.Metadata[fieldName]
	}
	if fieldValue == "" {
		// Fall back to round-robin if field is missing
		return r.selectRoundRobin()
	}

	idx := hashString(fieldValue, len(r.Topics))
	return r.Topics[idx]
}
