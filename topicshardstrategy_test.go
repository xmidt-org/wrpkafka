// SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package wrpkafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestIsMetadataStrategy tests the IsMetadataStrategy method.
func TestIsMetadataStrategy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		strategy      TopicShardStrategy
		wantIsMetadata bool
		wantFieldName string
	}{
		{
			name:          "metadata strategy with field",
			strategy:      "metadata:tenant_id",
			wantIsMetadata: true,
			wantFieldName: "tenant_id",
		},
		{
			name:          "metadata strategy with different field",
			strategy:      "metadata:region",
			wantIsMetadata: true,
			wantFieldName: "region",
		},
		{
			name:          "metadata strategy with empty field",
			strategy:      "metadata:",
			wantIsMetadata: false,
			wantFieldName: "",
		},
		{
			name:          "round robin strategy",
			strategy:      TopicShardRoundRobin,
			wantIsMetadata: false,
			wantFieldName: "",
		},
		{
			name:          "device ID strategy",
			strategy:      TopicShardDeviceID,
			wantIsMetadata: false,
			wantFieldName: "",
		},
		{
			name:          "none strategy",
			strategy:      TopicShardNone,
			wantIsMetadata: false,
			wantFieldName: "",
		},
		{
			name:          "invalid strategy",
			strategy:      "unknown",
			wantIsMetadata: false,
			wantFieldName: "",
		},
		{
			name:          "metadata prefix but invalid",
			strategy:      "metadat",
			wantIsMetadata: false,
			wantFieldName: "",
		},
		{
			name:          "metadata with complex field name",
			strategy:      "metadata:tenant_id.region",
			wantIsMetadata: true,
			wantFieldName: "tenant_id.region",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			gotIsMetadata, gotFieldName := tt.strategy.IsMetadataStrategy()
			assert.Equal(t, tt.wantIsMetadata, gotIsMetadata)
			assert.Equal(t, tt.wantFieldName, gotFieldName)
		})
	}
}

// TestValidateTopicShardStrategy tests the validateTopicShardStrategy function.
func TestValidateTopicShardStrategy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		strategy TopicShardStrategy
		wantErr  bool
	}{
		{
			name:     "valid round robin",
			strategy: TopicShardRoundRobin,
			wantErr:  false,
		},
		{
			name:     "valid device ID",
			strategy: TopicShardDeviceID,
			wantErr:  false,
		},
		{
			name:     "valid metadata with field",
			strategy: "metadata:tenant_id",
			wantErr:  false,
		},
		{
			name:     "valid metadata with different field",
			strategy: "metadata:region",
			wantErr:  false,
		},
		{
			name:     "valid metadata with complex field name",
			strategy: "metadata:tenant.region",
			wantErr:  false,
		},
		{
			name:     "invalid metadata with empty field",
			strategy: "metadata:",
			wantErr:  true,
		},
		{
			name:     "invalid metadata with whitespace only",
			strategy: "metadata:   ",
			wantErr:  true,
		},
		{
			name:     "invalid unknown strategy",
			strategy: "unknown",
			wantErr:  true,
		},
		{
			name:     "invalid empty strategy",
			strategy: "",
			wantErr:  true,
		},
		{
			name:     "invalid metadata prefix typo",
			strategy: "metadat:field",
			wantErr:  true,
		},
		{
			name:     "invalid case sensitivity",
			strategy: "RoundRobin",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateTopicShardStrategy(tt.strategy)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
