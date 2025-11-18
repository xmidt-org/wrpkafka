// SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package wrpkafka

import (
	"errors"
	"fmt"
	"strings"
)

// TopicShardStrategy specifies how messages are distributed across multiple Kafka topics.
type TopicShardStrategy string

const (
	// TopicShardNone indicates single-topic routing (no sharding).
	TopicShardNone TopicShardStrategy = ""

	// TopicShardRoundRobin distributes messages across topics in round-robin fashion.
	TopicShardRoundRobin TopicShardStrategy = "roundrobin"

	// TopicShardDeviceID shards messages by device ID (from WRP Source field).
	TopicShardDeviceID TopicShardStrategy = "deviceid"

	// TopicShardMetadata uses format "metadata:<field>" - parsed at runtime.
	// Example: "metadata:tenant_id" shards by the tenant_id metadata field.
	// Note: This is a prefix pattern, not a constant. Use IsMetadataStrategy() to detect.
)

var topicShardStrategyTypes map[TopicShardStrategy]struct{}
var topicShardStrategyList []string

func init() {
	list := []TopicShardStrategy{
		TopicShardRoundRobin,
		TopicShardDeviceID,
	}

	topicShardStrategyTypes = make(map[TopicShardStrategy]struct{})
	for _, s := range list {
		topicShardStrategyTypes[s] = struct{}{}
		topicShardStrategyList = append(topicShardStrategyList, string(s))
	}
}

// IsMetadataStrategy checks if this strategy uses metadata-based sharding.
// Returns (true, fieldName) if the strategy is "metadata:<field>", otherwise (false, "").
func (s TopicShardStrategy) IsMetadataStrategy() (bool, string) {
	const prefix = "metadata:"
	if len(s) > len(prefix) && string(s[:len(prefix)]) == prefix {
		return true, string(s[len(prefix):])
	}
	return false, ""
}

// validateTopicShardStrategy validates the TopicShardStrategy enum value.
// Valid values: TopicShardRoundRobin, TopicShardDeviceID, or "metadata:<fieldname>".
func validateTopicShardStrategy(strategy TopicShardStrategy) error {
	// Check standard strategies
	_, ok := topicShardStrategyTypes[strategy]
	if ok {
		return nil
	}

	// Check for metadata:<fieldname>
	if isMetadata, fieldName := strategy.IsMetadataStrategy(); isMetadata {
		// Field name must be non-empty and not just whitespace
		if strings.TrimSpace(fieldName) == "" {
			return errors.Join(ErrValidation,
				fmt.Errorf("metadata sharding requires field name (e.g., 'metadata:tenant_id')"))
		}
		return nil
	}

	// Invalid strategy
	list := strings.Join(topicShardStrategyList, "', '")
	list = "'" + list + "'"
	return errors.Join(ErrValidation,
		fmt.Errorf("topic shard strategy '%s' is invalid: must be %s, or 'metadata:<field>'", strategy, list))
}
