// SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package wrpkafka

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/xmidt-org/wrp-go/v5"
)

type HashKeyProviderSuite struct {
	suite.Suite
}

// ParseHashKey tests
func (s *HashKeyProviderSuite) TestParseHashKey_Empty() {
	hashKey, err := ParseHashKey("")
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), HashKeyMetadata, hashKey.Name)
	assert.Equal(s.T(), DefaultMetadataKeyField, hashKey.MetadataField)
}

func (s *HashKeyProviderSuite) TestParseHashKey_None() {
	hashKey, err := ParseHashKey("none")
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), HashKeyNone, hashKey.Name)
	assert.Equal(s.T(), "", hashKey.MetadataField)
}

func (s *HashKeyProviderSuite) TestParseHashKey_None_CaseInsensitive() {
	hashKey, err := ParseHashKey("NONE")
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), HashKeyNone, hashKey.Name)
	assert.Equal(s.T(), "", hashKey.MetadataField)
}

func (s *HashKeyProviderSuite) TestParseHashKey_Metadata_Default() {
	hashKey, err := ParseHashKey("metadata")
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), HashKeyMetadata, hashKey.Name)
	assert.Equal(s.T(), DefaultMetadataKeyField, hashKey.MetadataField)
}

func (s *HashKeyProviderSuite) TestParseHashKey_Metadata_CustomField() {
	hashKey, err := ParseHashKey("metadata/custom-field")
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), HashKeyMetadata, hashKey.Name)
	assert.Equal(s.T(), "custom-field", hashKey.MetadataField)
}

func (s *HashKeyProviderSuite) TestParseHashKey_Metadata_EmptyField() {
	hashKey, err := ParseHashKey("metadata/")
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), HashKeyMetadata, hashKey.Name)
	assert.Equal(s.T(), DefaultMetadataKeyField, hashKey.MetadataField)
}

func (s *HashKeyProviderSuite) TestParseHashKey_Metadata_CaseInsensitive() {
	hashKey, err := ParseHashKey("METADATA/my-field")
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), HashKeyMetadata, hashKey.Name)
	assert.Equal(s.T(), "my-field", hashKey.MetadataField)
}

func (s *HashKeyProviderSuite) TestParseHashKey_Source() {
	hashKey, err := ParseHashKey("source")
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), HashKeySource, hashKey.Name)
	assert.Equal(s.T(), "", hashKey.MetadataField)
}

func (s *HashKeyProviderSuite) TestParseHashKey_Source_CaseInsensitive() {
	hashKey, err := ParseHashKey("SOURCE")
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), HashKeySource, hashKey.Name)
	assert.Equal(s.T(), "", hashKey.MetadataField)
}

func (s *HashKeyProviderSuite) TestParseHashKey_Invalid() {
	hashKey, err := ParseHashKey("invalid")
	assert.Error(s.T(), err)
	assert.True(s.T(), errors.Is(err, ErrInvalidHashKeyType))
	assert.Equal(s.T(), HashKey{}, hashKey)
}

// GetHashKey tests
func (s *HashKeyProviderSuite) TestGetHashKey_None() {
	msg := &wrp.Message{
		Metadata: map[string]string{"hw-deviceid": "mac:112233445566"},
	}
	hashKey := HashKey{Name: HashKeyNone}
	key, err := hashKey.GetHashKey(msg)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), "", key)
}

func (s *HashKeyProviderSuite) TestGetHashKey_Metadata_Valid() {
	msg := &wrp.Message{
		Metadata: map[string]string{"hw-deviceid": "mac:112233445566"},
	}
	hashKey := HashKey{Name: HashKeyMetadata, MetadataField: "hw-deviceid"}
	key, err := hashKey.GetHashKey(msg)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), "mac:112233445566", key)
}

func (s *HashKeyProviderSuite) TestGetHashKey_Metadata_CustomField() {
	msg := &wrp.Message{
		Metadata: map[string]string{"custom-id": "some-value"},
	}
	hashKey := HashKey{Name: HashKeyMetadata, MetadataField: "custom-id"}
	key, err := hashKey.GetHashKey(msg)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), "some-value", key)
}

func (s *HashKeyProviderSuite) TestGetHashKey_Metadata_DefaultWhenEmpty() {
	msg := &wrp.Message{
		Metadata: map[string]string{"hw-deviceid": "mac:112233445566"},
	}
	hashKey := HashKey{} // Empty struct should default to metadata/hw-deviceid
	key, err := hashKey.GetHashKey(msg)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), "mac:112233445566", key)
}

func (s *HashKeyProviderSuite) TestGetHashKey_Metadata_NilMetadata() {
	msg := &wrp.Message{
		Metadata: nil,
	}
	hashKey := HashKey{Name: HashKeyMetadata, MetadataField: "hw-deviceid"}
	key, err := hashKey.GetHashKey(msg)
	assert.Error(s.T(), err)
	assert.True(s.T(), errors.Is(err, ErrEmptyHashKey))
	assert.Equal(s.T(), "", key)
}

func (s *HashKeyProviderSuite) TestGetHashKey_Metadata_EmptyValue() {
	msg := &wrp.Message{
		Metadata: map[string]string{"hw-deviceid": ""},
	}
	hashKey := HashKey{Name: HashKeyMetadata, MetadataField: "hw-deviceid"}
	key, err := hashKey.GetHashKey(msg)
	assert.Error(s.T(), err)
	assert.True(s.T(), errors.Is(err, ErrEmptyHashKey))
	assert.Equal(s.T(), "", key)
}

func (s *HashKeyProviderSuite) TestGetHashKey_Metadata_MissingField() {
	msg := &wrp.Message{
		Metadata: map[string]string{"other-field": "value"},
	}
	hashKey := HashKey{Name: HashKeyMetadata, MetadataField: "hw-deviceid"}
	key, err := hashKey.GetHashKey(msg)
	assert.Error(s.T(), err)
	assert.True(s.T(), errors.Is(err, ErrEmptyHashKey))
	assert.Equal(s.T(), "", key)
}

func (s *HashKeyProviderSuite) TestGetHashKey_Metadata_Valid_LeadingSlash() {
	msg := &wrp.Message{
		Metadata: map[string]string{"/hw-deviceid": "mac:112233445566"},
	}
	hashKey := HashKey{Name: HashKeyMetadata, MetadataField: "hw-deviceid"}
	key, err := hashKey.GetHashKey(msg)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), "mac:112233445566", key)
}

func (s *HashKeyProviderSuite) TestGetHashKey_Metadata_Empty_LeadingSlash() {
	msg := &wrp.Message{
		Metadata: map[string]string{"/hw-deviceid": ""},
	}
	hashKey := HashKey{Name: HashKeyMetadata, MetadataField: "hw-deviceid"}
	key, err := hashKey.GetHashKey(msg)
	assert.Error(s.T(), err)
	assert.True(s.T(), errors.Is(err, ErrEmptyHashKey))
	assert.Equal(s.T(), "", key)
}

func (s *HashKeyProviderSuite) TestGetHashKey_Metadata_EmptyFieldName() {
	msg := &wrp.Message{
		Metadata: map[string]string{"hw-deviceid": "mac:112233445566"},
	}
	hashKey := HashKey{Name: HashKeyMetadata, MetadataField: ""}
	key, err := hashKey.GetHashKey(msg)
	assert.Error(s.T(), err)
	assert.Contains(s.T(), err.Error(), "metadata key field cannot be empty")
	assert.Equal(s.T(), "", key)
}

func (s *HashKeyProviderSuite) TestGetHashKey_Source_Valid() {
	msg := &wrp.Message{
		Source: "mac:112233445566/service",
	}
	hashKey := HashKey{Name: HashKeySource}
	key, err := hashKey.GetHashKey(msg)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), "112233445566", key)
}

func (s *HashKeyProviderSuite) TestGetHashKey_Source_EmptySource() {
	msg := &wrp.Message{
		Source: "",
	}
	hashKey := HashKey{Name: HashKeySource}
	key, err := hashKey.GetHashKey(msg)
	assert.Error(s.T(), err)
	assert.True(s.T(), errors.Is(err, ErrEmptyHashKey))
	assert.Equal(s.T(), "", key)
}

func (s *HashKeyProviderSuite) TestGetHashKey_Source_InvalidFormat() {
	msg := &wrp.Message{
		Source: "invalid-format",
	}
	hashKey := HashKey{Name: HashKeySource}
	key, err := hashKey.GetHashKey(msg)
	assert.Error(s.T(), err)
	assert.Contains(s.T(), err.Error(), "failed to parse device ID from source")
	assert.Equal(s.T(), "", key)
}

func (s *HashKeyProviderSuite) TestGetHashKey_InvalidKeyType() {
	msg := &wrp.Message{
		Metadata: map[string]string{"hw-deviceid": "mac:112233445566"},
	}
	hashKey := HashKey{Name: HashKeyType("invalid"), MetadataField: "hw-deviceid"}
	key, err := hashKey.GetHashKey(msg)
	assert.Error(s.T(), err)
	assert.True(s.T(), errors.Is(err, ErrInvalidHashKeyType))
	assert.Equal(s.T(), "", key)
}

// Integration tests - combining ParseHashKey and GetHashKey
func (s *HashKeyProviderSuite) TestIntegration_ParseAndExtract_Metadata() {
	hashKey, err := ParseHashKey("metadata/hw-deviceid")
	assert.NoError(s.T(), err)

	msg := &wrp.Message{
		Metadata: map[string]string{"hw-deviceid": "mac:112233445566"},
	}
	key, err := hashKey.GetHashKey(msg)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), "mac:112233445566", key)
}

func (s *HashKeyProviderSuite) TestIntegration_ParseAndExtract_Source() {
	hashKey, err := ParseHashKey("source")
	assert.NoError(s.T(), err)

	msg := &wrp.Message{
		Source: "mac:112233445566/service",
	}
	key, err := hashKey.GetHashKey(msg)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), "112233445566", key)
}

func (s *HashKeyProviderSuite) TestIntegration_ParseAndExtract_None() {
	hashKey, err := ParseHashKey("none")
	assert.NoError(s.T(), err)

	msg := &wrp.Message{
		Source: "mac:112233445566/service",
	}
	key, err := hashKey.GetHashKey(msg)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), "", key)
}

func (s *HashKeyProviderSuite) TestIntegration_ParseAndExtract_Default() {
	hashKey, err := ParseHashKey("")
	assert.NoError(s.T(), err)

	msg := &wrp.Message{
		Metadata: map[string]string{"hw-deviceid": "mac:112233445566"},
	}
	key, err := hashKey.GetHashKey(msg)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), "mac:112233445566", key)
}
func (s *HashKeyProviderSuite) TestIntegration_ParseAndExtract_MetadataWithSlash() {
	hashKey, err := ParseHashKey("metadata/custom-field")
	assert.NoError(s.T(), err)

	msg := &wrp.Message{
		Metadata: map[string]string{"/custom-field": "slash-prefixed-value"},
	}
	key, err := hashKey.GetHashKey(msg)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), "slash-prefixed-value", key)
}

func TestHashKeyProviderSuite(t *testing.T) {
	suite.Run(t, new(HashKeyProviderSuite))
}
