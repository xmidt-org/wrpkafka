// SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package wrpkafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xmidt-org/wrp-go/v5"
)

func TestParseHashKeyType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  HashKeyType
	}{
		{
			name:  "parse none",
			input: "none",
			want:  HashKeyNone,
		},
		{
			name:  "parse deviceid",
			input: "deviceid",
			want:  HashKeyDeviceID,
		},
		{
			name:  "invalid type",
			input: "invalid",
			want:  HashKeyDeviceID,
		},
		{
			name:  "empty string",
			input: "",
			want:  HashKeyDeviceID,
		},
		{
			name:  "case insensitive - uppercase",
			input: "DEVICEID",
			want:  HashKeyDeviceID,
		},
		{
			name:  "case insensitive - mixed case",
			input: "DeviceId",
			want:  HashKeyDeviceID,
		},
		{
			name:  "case insensitive - uppercase none",
			input: "NONE",
			want:  HashKeyNone,
		},
		{
			name:  "case insensitive - mixed case none",
			input: "None",
			want:  HashKeyNone,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := ParseHashKeyType(tt.input)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestHashKeyType_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		keyType   HashKeyType
		wantStr   string
		wantError bool
	}{
		{
			name:      "HashKeyNone",
			keyType:   HashKeyNone,
			wantStr:   "none",
			wantError: false,
		},
		{
			name:      "HashKeyDeviceID",
			keyType:   HashKeyDeviceID,
			wantStr:   "deviceid",
			wantError: false,
		},
		{
			name:      "unknown type",
			keyType:   HashKeyType(999),
			wantStr:   "unknown(999)",
			wantError: true,
		},
		{
			name:      "negative value",
			keyType:   HashKeyType(-1),
			wantStr:   "unknown(-1)",
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := tt.keyType.String()
			assert.Equal(t, tt.wantStr, got)
			if tt.wantError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "unknown hash key type")
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestGetHashKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		msg       *wrp.Message
		keyType   HashKeyType
		want      string
		wantError error
	}{
		{
			name:      "HashKeyNone returns empty string",
			msg:       &wrp.Message{},
			keyType:   HashKeyNone,
			want:      "",
			wantError: nil,
		},
		{
			name: "HashKeyNone with message data still returns empty",
			msg: &wrp.Message{
				Metadata: map[string]string{
					MetadataKeyDeviceID: "device123",
				},
			},
			keyType:   HashKeyNone,
			want:      "",
			wantError: nil,
		},
		{
			name: "HashKeyDeviceID with valid device ID",
			msg: &wrp.Message{
				Metadata: map[string]string{
					MetadataKeyDeviceID: "mac:112233445566",
				},
			},
			keyType:   HashKeyDeviceID,
			want:      "mac:112233445566",
			wantError: nil,
		},
		{
			name: "HashKeyDeviceID with empty device ID",
			msg: &wrp.Message{
				Metadata: map[string]string{
					MetadataKeyDeviceID: "",
				},
			},
			keyType:   HashKeyDeviceID,
			want:      "",
			wantError: ErrEmptyHashKey,
		},
		{
			name: "HashKeyDeviceID with nil metadata",
			msg: &wrp.Message{
				Metadata: nil,
			},
			keyType:   HashKeyDeviceID,
			want:      "",
			wantError: ErrEmptyHashKey,
		},
		{
			name: "HashKeyDeviceID with empty metadata map",
			msg: &wrp.Message{
				Metadata: map[string]string{},
			},
			keyType:   HashKeyDeviceID,
			want:      "",
			wantError: ErrEmptyHashKey,
		},
		{
			name: "HashKeyDeviceID with other metadata but no device ID",
			msg: &wrp.Message{
				Metadata: map[string]string{
					"other-key": "other-value",
				},
			},
			keyType:   HashKeyDeviceID,
			want:      "",
			wantError: ErrEmptyHashKey,
		},
		{
			name: "HashKeyDeviceID with complex device ID",
			msg: &wrp.Message{
				Metadata: map[string]string{
					MetadataKeyDeviceID: "uuid:12345-67890-abcdef",
				},
			},
			keyType:   HashKeyDeviceID,
			want:      "uuid:12345-67890-abcdef",
			wantError: nil,
		},
		{
			name:      "unsupported hash key type",
			msg:       &wrp.Message{},
			keyType:   HashKeyType(999),
			want:      "",
			wantError: nil, // We check for error containing specific text
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := GetHashKey(tt.msg, tt.keyType)
			assert.Equal(t, tt.want, got)

			if tt.wantError != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantError)
			} else if tt.name == "unsupported hash key type" {
				// Special case: check for unsupported type error
				require.Error(t, err)
				assert.Contains(t, err.Error(), "unsupported hash key type")
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestGetHashKey_MetadataKeyConstant(t *testing.T) {
	t.Parallel()

	// Verify that the constant is correctly defined
	assert.Equal(t, "hw-deviceid", MetadataKeyDeviceID, "MetadataKeyDeviceID constant should match expected value")
}

func TestHashKeyType_RoundTrip(t *testing.T) {
	t.Parallel()

	// Test that parsing and converting back to string works correctly
	tests := []struct {
		name    string
		keyType HashKeyType
	}{
		{"none", HashKeyNone},
		{"deviceid", HashKeyDeviceID},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Convert to string
			str, err := tt.keyType.String()
			require.NoError(t, err)

			// Parse back
			parsed := ParseHashKeyType(str)

			// Should match original
			assert.Equal(t, tt.keyType, parsed)
		})
	}
}
