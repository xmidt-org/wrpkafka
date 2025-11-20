// SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package wrpkafka

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestErrors tests error types and sentinel errors.
func TestErrors(t *testing.T) {
	t.Parallel()

	t.Run("sentinel errors", func(t *testing.T) {
		t.Parallel()
		// All sentinel errors should be *metricError
		sentinels := []error{
			ErrEncoding,
			ErrNoTopicMatch,
			ErrBufferFull,
			ErrBroker,
			ErrTimeout,
			ErrValidation,
			ErrNotStarted,
		}

		for _, sentinel := range sentinels {
			me, ok := sentinel.(*metricError) // nolint:errorlint
			assert.True(t, ok, "sentinel should be *metricError")
			assert.NotEmpty(t, me.message, "sentinel should have message")
			assert.NotEmpty(t, me.metric, "sentinel should have metric type")
			assert.Equal(t, me.message, me.Error(), "Error() should return message")
			assert.Equal(t, me.metric, me.Metric(), "Metric() should return metric type")
		}
	})

	t.Run("error wrapping with errors.Is", func(t *testing.T) {
		t.Parallel()

		// Wrapped error should match sentinel
		wrapped := errors.Join(ErrEncoding, fmt.Errorf("msgpack failed"))
		assert.True(t, errors.Is(wrapped, ErrEncoding))
		assert.False(t, errors.Is(wrapped, ErrBroker))

		// Multiple wrapping
		doubleWrapped := fmt.Errorf("outer: %w", wrapped)
		assert.True(t, errors.Is(doubleWrapped, ErrEncoding))
	})

	t.Run("error types for metrics", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name     string
			err      error
			expected string
		}{
			{"encoding error", ErrEncoding, "encoding_error"},
			{"no topic match", ErrNoTopicMatch, "no_topic_match"},
			{"buffer full", ErrBufferFull, "buffer_full"},
			{"broker error", ErrBroker, "broker_error"},
			{"timeout", ErrTimeout, "timeout"},
			{"validation", ErrValidation, "validation_error"},
			{"not started", ErrNotStarted, "not_started"},
			{"nil error", nil, ""},
			{"unknown error", fmt.Errorf("random"), "unknown"},
			{"wrapped encoding", errors.Join(ErrEncoding, fmt.Errorf("test")), "encoding_error"},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result := errorType(tt.err)
				assert.Equal(t, tt.expected, result)
			})
		}
	})

	t.Run("Is() method semantics", func(t *testing.T) {
		t.Parallel()

		// Sentinel should match itself
		assert.True(t, errors.Is(ErrEncoding, ErrEncoding))

		// Different sentinels should not match
		assert.False(t, errors.Is(ErrEncoding, ErrBroker))

		// New *metricError with same metric should NOT match sentinel
		// (only pointer equality should work)
		newErr := &metricError{metric: "encoding_error", message: "test"}
		assert.False(t, errors.Is(newErr, ErrEncoding))

		// nil should not match
		assert.False(t, errors.Is(nil, ErrEncoding))
		assert.False(t, errors.Is(ErrEncoding, nil))
	})
}
