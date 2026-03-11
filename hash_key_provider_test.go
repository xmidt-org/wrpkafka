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

func (s *HashKeyProviderSuite) TestParseHashKeyType_Empty() {
	keyType, field, err := ParseHashKeyType("")
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), HashKeyMetadata, keyType)
	assert.Equal(s.T(), DefaultMetadataKeyField, field)
}

func (s *HashKeyProviderSuite) TestParseHashKeyType_None() {
	keyType, field, err := ParseHashKeyType("none")
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), HashKeyNone, keyType)
	assert.Equal(s.T(), "", field)
}

func (s *HashKeyProviderSuite) TestParseHashKeyType_None_CaseInsensitive() {
	keyType, field, err := ParseHashKeyType("NONE")
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), HashKeyNone, keyType)
	assert.Equal(s.T(), "", field)
}

func (s *HashKeyProviderSuite) TestParseHashKeyType_Metadata_Default() {
	keyType, field, err := ParseHashKeyType("metadata")
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), HashKeyMetadata, keyType)
	assert.Equal(s.T(), DefaultMetadataKeyField, field)
}

func (s *HashKeyProviderSuite) TestParseHashKeyType_Metadata_CustomField() {
	keyType, field, err := ParseHashKeyType("metadata/custom-field")
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), HashKeyMetadata, keyType)
	assert.Equal(s.T(), "custom-field", field)
}

func (s *HashKeyProviderSuite) TestParseHashKeyType_Metadata_EmptyField() {
	keyType, field, err := ParseHashKeyType("metadata/")
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), HashKeyMetadata, keyType)
	assert.Equal(s.T(), DefaultMetadataKeyField, field)
}

func (s *HashKeyProviderSuite) TestParseHashKeyType_Metadata_CaseInsensitive() {
	keyType, field, err := ParseHashKeyType("METADATA/my-field")
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), HashKeyMetadata, keyType)
	assert.Equal(s.T(), "my-field", field)
}

func (s *HashKeyProviderSuite) TestParseHashKeyType_Source() {
	keyType, field, err := ParseHashKeyType("source")
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), HashKeySource, keyType)
	assert.Equal(s.T(), "", field)
}

func (s *HashKeyProviderSuite) TestParseHashKeyType_Source_CaseInsensitive() {
	keyType, field, err := ParseHashKeyType("SOURCE")
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), HashKeySource, keyType)
	assert.Equal(s.T(), "", field)
}

func (s *HashKeyProviderSuite) TestParseHashKeyType_Invalid() {
	keyType, field, err := ParseHashKeyType("invalid")
	assert.Error(s.T(), err)
	assert.True(s.T(), errors.Is(err, ErrInvalidHashKeyType))
	assert.Equal(s.T(), HashKeyType(""), keyType)
	assert.Equal(s.T(), "", field)
}

func (s *HashKeyProviderSuite) TestGetHashKey_None() {
	msg := &wrp.Message{
		Metadata: map[string]string{"hw-deviceid": "mac:112233445566"},
	}
	key, err := GetHashKey(msg, HashKeyNone, "")
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), "", key)
}

func (s *HashKeyProviderSuite) TestGetHashKey_Metadata_Valid() {
	msg := &wrp.Message{
		Metadata: map[string]string{"hw-deviceid": "mac:112233445566"},
	}
	key, err := GetHashKey(msg, HashKeyMetadata, "hw-deviceid")
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), "mac:112233445566", key)
}

func (s *HashKeyProviderSuite) TestGetHashKey_Metadata_CustomField() {
	msg := &wrp.Message{
		Metadata: map[string]string{"custom-id": "some-value"},
	}
	key, err := GetHashKey(msg, HashKeyMetadata, "custom-id")
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), "some-value", key)
}

func (s *HashKeyProviderSuite) TestGetHashKey_Metadata_NilMetadata() {
	msg := &wrp.Message{
		Metadata: nil,
	}
	key, err := GetHashKey(msg, HashKeyMetadata, "hw-deviceid")
	assert.Error(s.T(), err)
	assert.True(s.T(), errors.Is(err, ErrEmptyHashKey))
	assert.Equal(s.T(), "", key)
}

func (s *HashKeyProviderSuite) TestGetHashKey_Metadata_EmptyValue() {
	msg := &wrp.Message{
		Metadata: map[string]string{"hw-deviceid": ""},
	}
	key, err := GetHashKey(msg, HashKeyMetadata, "hw-deviceid")
	assert.Error(s.T(), err)
	assert.True(s.T(), errors.Is(err, ErrEmptyHashKey))
	assert.Equal(s.T(), "", key)
}

func (s *HashKeyProviderSuite) TestGetHashKey_Metadata_MissingField() {
	msg := &wrp.Message{
		Metadata: map[string]string{"other-field": "value"},
	}
	key, err := GetHashKey(msg, HashKeyMetadata, "hw-deviceid")
	assert.Error(s.T(), err)
	assert.True(s.T(), errors.Is(err, ErrEmptyHashKey))
	assert.Equal(s.T(), "", key)
}

func (s *HashKeyProviderSuite) TestGetHashKey_Source_Valid() {
	msg := &wrp.Message{
		Source: "mac:112233445566/service",
	}
	key, err := GetHashKey(msg, HashKeySource, "")
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), "112233445566", key)
}

func (s *HashKeyProviderSuite) TestGetHashKey_Source_EmptySource() {
	msg := &wrp.Message{
		Source: "",
	}
	key, err := GetHashKey(msg, HashKeySource, "")
	assert.Error(s.T(), err)
	assert.True(s.T(), errors.Is(err, ErrEmptyHashKey))
	assert.Equal(s.T(), "", key)
}

func (s *HashKeyProviderSuite) TestGetHashKey_Source_InvalidFormat() {
	msg := &wrp.Message{
		Source: "invalid-format",
	}
	key, err := GetHashKey(msg, HashKeySource, "")
	assert.Error(s.T(), err)
	assert.Equal(s.T(), "", key)
}

func (s *HashKeyProviderSuite) TestGetHashKey_InvalidKeyType() {
	msg := &wrp.Message{
		Metadata: map[string]string{"hw-deviceid": "mac:112233445566"},
	}
	key, err := GetHashKey(msg, HashKeyType("invalid"), "hw-deviceid")
	assert.Error(s.T(), err)
	assert.True(s.T(), errors.Is(err, ErrInvalidHashKeyType))
	assert.Equal(s.T(), "", key)
}

func TestHashKeyProviderSuite(t *testing.T) {
	suite.Run(t, new(HashKeyProviderSuite))
}
