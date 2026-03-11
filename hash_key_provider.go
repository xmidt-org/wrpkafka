// SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package wrpkafka

import (
	"errors"
	"fmt"
	"strings"

	"github.com/xmidt-org/wrp-go/v5"
)

var (
	// ErrInvalidHashKeyType is returned when an unsupported hash key type is specified.
	ErrInvalidHashKeyType = errors.New("invalid hash key type")

	// ErrEmptyHashKey is returned when the hash key is empty but required.
	ErrEmptyHashKey = errors.New("hash key is empty")
)

const (
	// DefaultMetadataKeyField is the default metadata field used for hash key extraction.
	DefaultMetadataKeyField = "hw-deviceid"
)

// HashKeyType represents the type of hash key to extract from a WRP message.
type HashKeyType string

const (
	// key from any field in metadata
	HashKeyMetadata HashKeyType = "metadata"

	// deviceId parsed from the source field
	HashKeySource HashKeyType = "source"

	// indicates no hash key should be used (e.g., for non-sharded topics).
	HashKeyNone HashKeyType = "none"
)

// ParseHashKeyType converts a string to a HashKeyType.
// The comparison is case-insensitive.
// Defaults to metadata/hw-deviceid when the input string is empty.
// Returns an error if the hash key type is not recognized.
func ParseHashKeyType(s string) (HashKeyType, string, error) {
	if s == "" {
		return HashKeyMetadata, DefaultMetadataKeyField, nil
	}
	tokens := strings.Split(s, "/")
	switch strings.ToLower(tokens[0]) {
	case string(HashKeyNone):
		return HashKeyNone, "", nil
	case string(HashKeyMetadata):
		if (len(tokens) >= 2) && tokens[1] != "" {
			return HashKeyMetadata, tokens[1], nil
		}
		return HashKeyMetadata, DefaultMetadataKeyField, nil
	case string(HashKeySource):
		return HashKeySource, "", nil
	default:
		return "", "", ErrInvalidHashKeyType
	}
}

// GetHashKey extracts the hash key from a WRP message based on the specified hash key type.
// Returns the hash key string or an error if the hash key is required but empty.
// Defaults to metadata with the default field if keyType is empty.
// Returns ErrEmptyHashKey if the extracted hash key value is empty or missing.
func GetHashKey(msg *wrp.Message, keyType HashKeyType, metadataKey string) (string, error) {
	if keyType == "" {
		keyType = HashKeyMetadata
		metadataKey = DefaultMetadataKeyField
	}

	switch keyType {
	case HashKeyNone:
		return "", nil
	case HashKeyMetadata:
		if metadataKey == "" {
			return "", fmt.Errorf("metadata key field cannot be empty: %w", ErrEmptyHashKey)
		}
		if msg.Metadata == nil {
			return "", ErrEmptyHashKey
		}
		hashKey := msg.Metadata[metadataKey]
		if hashKey == "" {
			return "", ErrEmptyHashKey
		}
		return hashKey, nil
	case HashKeySource:
		if msg.Source == "" {
			return "", ErrEmptyHashKey
		}
		deviceID, err := wrp.ParseDeviceID(msg.Source)
		if err != nil {
			return "", fmt.Errorf("failed to parse device ID from source: %w", err)
		}
		return deviceID.ID(), nil

	default:
		return "", fmt.Errorf("unsupported hash key type: %v: %w", keyType, ErrInvalidHashKeyType)
	}
}
