// SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package wrpkafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestHashString tests the hashString function's edge cases and bounds checking.
// Note: We trust the standard library's hash/fnv implementation for consistency and distribution.
func TestHashString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		s    string
		n    int
		want int
	}{
		{"n=0 returns 0", "any-string", 0, 0},
		{"negative n returns 0", "any-string", -5, 0},
		{"n=1 always returns 0", "any-string", 1, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := hashString(tt.s, tt.n)
			assert.Equal(t, tt.want, got)
		})
	}

	// Test that result is within bounds for normal cases
	t.Run("result within bounds", func(t *testing.T) {
		t.Parallel()

		testCases := []struct {
			s string
			n int
		}{
			{"mac:112233445566", 3},
			{"device-id", 10},
			{"", 5},
		}

		for _, tc := range testCases {
			result := hashString(tc.s, tc.n)
			assert.GreaterOrEqual(t, result, 0)
			assert.Less(t, result, tc.n)
		}
	})
}
