// SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package wrpkafka

import (
	"errors"
	"fmt"
	"strings"

	"github.com/xmidt-org/wrp-go/v5"
)

const (
	// MetadataKeyDeviceID is the metadata key for hardware device ID.
	MetadataKeyDeviceID = "hw-deviceid"
)

// HashKeyType represents the type of hash key to extract from a WRP message.
type HashKeyType int

const (
	// HashKeyDeviceID is the default key.
	HashKeyDeviceID HashKeyType = iota

	// HashKeyNone indicates no hash key should be used (e.g., for non-sharded topics).
	HashKeyNone
)

// String returns the string representation of the HashKeyType.
func (h HashKeyType) String() (string, error) {
	switch h {
	case HashKeyNone:
		return "none", nil
	case HashKeyDeviceID:
		return "deviceid", nil
	default:
		return fmt.Sprintf("unknown(%d)", h), fmt.Errorf("unknown hash key type: %d", h)
	}
}

// ParseHashKeyType converts a string to a HashKeyType.
// The comparison is case-insensitive.
// Defaults to deviceId
func ParseHashKeyType(s string) HashKeyType {
	switch strings.ToLower(s) {
	case "none":
		return HashKeyNone
	case "deviceid":
		return HashKeyDeviceID
	default:
		return HashKeyDeviceID
	}
}

var (
	// ErrEmptyHashKey is returned when the hash key is empty but required.
	ErrEmptyHashKey = errors.New("hash key is empty")
)

// GetHashKey extracts the hash key from a WRP message based on the specified hash key type.
// Returns the hash key string or an error if the hash key is required but empty.
//
// Behavior by hash key type:
//   - HashKeyNone: Returns empty string and nil error.
//   - HashKeyDeviceID: Returns the value from msg.Metadata["hw-deviceid"] or ErrEmptyHashKey if empty.
func GetHashKey(msg *wrp.Message, keyType HashKeyType) (string, error) {
	switch keyType {
	case HashKeyNone:
		return "", nil

	case HashKeyDeviceID:
		if msg.Metadata == nil {
			return "", ErrEmptyHashKey
		}
		hashKey := msg.Metadata[MetadataKeyDeviceID]
		if hashKey == "" {
			return "", ErrEmptyHashKey
		}
		return hashKey, nil

	default:
		return "", fmt.Errorf("unsupported hash key type: %v", keyType)
	}
}
