// SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package wrpkafka

import (
	"hash/fnv"
)

// hashString computes FNV-1a hash of a string and returns the index within bounds [0, n).
// Returns 0 if n <= 0.
func hashString(s string, n int) int {
	if n <= 0 {
		return 0
	}

	h := fnv.New32a()
	h.Write([]byte(s))
	hash := h.Sum32()

	//nolint:gosec // G115: Modulo ensures result fits in int range
	return int(hash % uint32(n))
}
