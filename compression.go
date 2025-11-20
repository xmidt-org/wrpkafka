// SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package wrpkafka

import (
	"errors"
	"fmt"
	"strings"
)

// Compression specifies the message compression algorithm.
type Compression string

const (
	// CompressionSnappy uses Snappy compression (good balance, recommended).
	CompressionSnappy Compression = "snappy"

	// CompressionGzip uses Gzip compression.
	CompressionGzip Compression = "gzip"

	// CompressionLz4 uses LZ4 compression.
	CompressionLz4 Compression = "lz4"

	// CompressionZstd uses Zstandard compression.
	CompressionZstd Compression = "zstd"

	// CompressionNone disables compression.
	CompressionNone Compression = "none"
)

var compressionTypes map[Compression]struct{}
var compressionList []string

func init() {
	list := []Compression{
		CompressionSnappy,
		CompressionGzip,
		CompressionLz4,
		CompressionZstd,
		CompressionNone,
	}

	compressionTypes = make(map[Compression]struct{})
	for _, c := range list {
		compressionTypes[c] = struct{}{}
		compressionList = append(compressionList, string(c))
	}
}

// validateCompression validates the Compression enum value.
func validateCompression(codec Compression) error {
	if codec == "" {
		return nil
	}

	_, ok := compressionTypes[codec]
	if ok {
		return nil
	}

	list := strings.Join(compressionList, "', '")
	list = "'" + list + "'"
	return errors.Join(ErrValidation,
		fmt.Errorf("compression codec '%s' is invalid: must be %s or empty", codec, list))
}
