// SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package wrpkafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestOutcome_String tests the String() method for all Outcome values.
func TestOutcome_String(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		outcome  Outcome
		expected string
	}{
		{
			name:     "Accepted",
			outcome:  Accepted,
			expected: "Accepted",
		},
		{
			name:     "Queued",
			outcome:  Queued,
			expected: "Queued",
		},
		{
			name:     "Attempted",
			outcome:  Attempted,
			expected: "Attempted",
		},
		{
			name:     "Dropped",
			outcome:  Dropped,
			expected: "Dropped",
		},
		{
			name:     "Failed",
			outcome:  Failed,
			expected: "Failed",
		},
		{
			name:     "Unknown - invalid outcome value",
			outcome:  Outcome(999),
			expected: "Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := tt.outcome.String()
			assert.Equal(t, tt.expected, result, "String() should return correct value")
		})
	}
}
