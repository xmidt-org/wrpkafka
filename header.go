// SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package wrpkafka

import (
	"strconv"
	"strings"

	"github.com/xmidt-org/wrp-go/v5"
)

// extractWRPField extracts field values from a WRP message for use in Kafka record headers.
// Supports three categories of field extraction:
//
//  1. Standard WRP fields: Direct field names like "Type", "Source", "Destination", etc.
//     Returns a single-element slice for scalar fields, multi-element for arrays.
//
//  2. HTTP Headers: Syntax "Header.X-Custom-Name" extracts specific HTTP headers.
//     Performs case-insensitive matching and returns all matching values.
//     Example: "Header.Content-Type" extracts the Content-Type header.
//
//  3. Metadata: Syntax "Metadata.fieldName" extracts specific metadata map entries.
//     Returns the value if the key exists and is non-empty.
//     Example: "Metadata.tenant_id" extracts msg.Metadata["tenant_id"].
//
// Returns nil if:
//   - msg is nil
//   - fieldName is not recognized
//   - fieldName matches a pattern (Header./Metadata.) but the value doesn't exist
//   - The field exists but is empty/nil
//
// Returns empty slice for fields that exist but have no values.
func extractWRPField(msg *wrp.Message, fieldName string) []string {
	if msg == nil {
		return nil
	}

	if got, ok := extractStandardFields(msg, fieldName); ok {
		return got
	}

	if got, ok := extractHeaderFields(msg, fieldName); ok {
		return got
	}

	if got, ok := extractMetadataFields(msg, fieldName); ok {
		return got
	}

	return nil
}

// extractStandardFields extracts standard WRP message field values by field name.
// Provides direct access to built-in WRP message fields without requiring reflection.
//
// Supported field names (case-sensitive):
//   - "Type": Message type (SimpleRequest, SimpleEvent, etc.)
//   - "Source": Source device/service identifier
//   - "DeviceID": Device ID extracted from Source field (e.g., "mac:112233445566" → "112233445566")
//   - "Destination": Target device/service identifier
//   - "TransactionUUID": Unique transaction identifier for request/response correlation
//   - "ContentType": Content type of the payload
//   - "Accept": Acceptable response content types
//   - "Status": HTTP-style status code (pointer field, nil if not set)
//   - "RequestDeliveryResponse": Delivery response request flag (pointer field, nil if not set)
//   - "Headers": HTTP-style headers as array of "Key: Value" strings
//   - "Path": Service path/endpoint
//   - "ServiceName": Target service name
//   - "URL": Full URL for the request
//   - "PartnerIDs": Array of partner identifiers
//   - "SessionID": Session identifier for stateful interactions
//   - "QualityOfService": QoS level (0-99) for delivery guarantees
//
// Return value handling:
//   - Scalar fields: Returns single-element slice (e.g., []string{"value"})
//   - Array fields: Returns the array directly (e.g., PartnerIDs, Headers)
//   - Pointer fields: Returns nil if pointer is nil (Status, RequestDeliveryResponse)
//   - Numeric fields: Converted to string representation
//
// Returns:
//   - ([]string{value}, true) if fieldName matches a standard field and has a value
//   - (nil, true) if fieldName matches a standard field but value is nil/empty
//   - (nil, false) if fieldName doesn't match any standard field
//
// Examples:
//   - "Source" → []string{"mac:112233445566"}
//   - "Type" → []string{"SimpleEvent"}
//   - "PartnerIDs" → []string{"partner1", "partner2"}
//   - "Status" (when nil) → nil (but returns true for "ok" flag)
func extractStandardFields(msg *wrp.Message, fieldName string) ([]string, bool) {
	switch fieldName {
	case "Type":
		return []string{msg.Type.String()}, true
	case "Source":
		return []string{msg.Source}, true
	case "DeviceID":
		deviceID, err := wrp.ParseDeviceID(msg.Source)
		if err != nil {
			return nil, true
		}
		return []string{deviceID.ID()}, true
	case "Destination":
		return []string{msg.Destination}, true
	case "TransactionUUID":
		return []string{msg.TransactionUUID}, true
	case "ContentType":
		return []string{msg.ContentType}, true
	case "Accept":
		return []string{msg.Accept}, true
	case "Status":
		if msg.Status != nil {
			return []string{strconv.FormatInt(*msg.Status, 10)}, true
		}
		return nil, true
	case "RequestDeliveryResponse":
		if msg.RequestDeliveryResponse != nil {
			return []string{strconv.FormatInt(*msg.RequestDeliveryResponse, 10)}, true
		}
		return nil, true
	case "Headers":
		return msg.Headers, true
	case "Path":
		return []string{msg.Path}, true
	case "ServiceName":
		return []string{msg.ServiceName}, true
	case "URL":
		return []string{msg.URL}, true
	case "PartnerIDs":
		return msg.PartnerIDs, true
	case "SessionID":
		return []string{msg.SessionID}, true
	case "QualityOfService":
		return []string{strconv.Itoa(int(msg.QualityOfService))}, true
	}

	return nil, false
}

// extractHeaderFields extracts HTTP header values from a WRP message using "Header.X" syntax.
// Implements case-insensitive header name matching per HTTP specification.
//
// Syntax: "Header.X-Custom-Name" where X-Custom-Name is the target HTTP header.
//
// The WRP message stores headers as strings in "Key: Value" format. This function:
//   - Parses each header string to extract key and value
//   - Performs case-insensitive comparison of header names
//   - Collects all values for matching header names (supports multi-valued headers)
//   - Trims whitespace from both keys and values
//
// Examples:
//   - "Header.Content-Type" → extracts Content-Type header value(s)
//   - "Header.X-Custom" → extracts X-Custom header value(s)
//   - Case-insensitive: "Header.content-type" matches "Content-Type"
//
// Returns:
//   - (values, true) if fieldName matches "Header." prefix (even if no matching headers found)
//   - (nil, false) if fieldName doesn't match "Header." prefix
//
// Skips malformed headers (missing colon separator).
func extractHeaderFields(msg *wrp.Message, fieldName string) ([]string, bool) {
	// Support "Header.X-Custom-Name" syntax for extracting specific HTTP headers
	if strings.HasPrefix(fieldName, "Header.") {
		rv := make([]string, 0, len(msg.Headers))

		targetHeader := strings.TrimSpace(fieldName[7:]) // Extract header name after "Header."

		// HTTP headers are case-insensitive
		targetHeaderLower := strings.ToLower(targetHeader)

		// msg.Headers contains strings in "Key: Value" format
		for _, header := range msg.Headers {
			// Split on first colon to separate key from value
			key, value, found := strings.Cut(header, ":")
			if !found {
				continue // Skip malformed headers
			}

			// HTTP header names are case-insensitive, trim whitespace
			key = strings.TrimSpace(key)
			if strings.ToLower(key) == targetHeaderLower {
				rv = append(rv, strings.TrimSpace(value))
			}
		}

		return rv, true
	}

	return nil, false
}

// extractMetadataFields extracts metadata field values from a WRP message using "Metadata.X" syntax.
// Provides access to arbitrary key-value pairs stored in the WRP message's Metadata map.
//
// Syntax: "Metadata.fieldName" where fieldName is the key in the msg.Metadata map.
//
// The WRP message Metadata field is a map[string]string for storing arbitrary key-value pairs.
// This function:
//   - Extracts the target key name after the "Metadata." prefix
//   - Looks up the key in msg.Metadata (if map is not nil)
//   - Returns the value if it exists and is non-empty
//   - Trims whitespace from the key name
//
// Examples:
//   - "Metadata.tenant_id" → extracts msg.Metadata["tenant_id"]
//   - "Metadata.device_type" → extracts msg.Metadata["device_type"]
//   - "Metadata.region" → extracts msg.Metadata["region"]
//
// Returns:
//   - ([]string{value}, true) if fieldName matches "Metadata." prefix and value exists/non-empty
//   - (nil, true) if fieldName matches "Metadata." prefix but value doesn't exist or is empty
//   - (nil, false) if fieldName doesn't match "Metadata." prefix
//
// Note: Unlike extractHeaderFields which can return multiple values, metadata extraction
// always returns a single-element slice (metadata values are unique per key).
func extractMetadataFields(msg *wrp.Message, fieldName string) ([]string, bool) {
	// Support "Metadata.X" syntax for extracting specific metadata fields
	if strings.HasPrefix(fieldName, "Metadata.") {
		if msg.Metadata != nil {
			targetKey := strings.TrimSpace(fieldName[9:]) // Extract key after "Metadata."

			if value, ok := msg.Metadata[targetKey]; ok && value != "" {
				return []string{value}, true
			}
		}
		return nil, true
	}

	return nil, false
}

// validWRPFieldNames contains all valid WRP field names for header extraction.
// This is used for validation to catch typos and invalid field references early.
var validWRPFieldNames = map[string]struct{}{
	"Type":                    {},
	"Source":                  {},
	"DeviceID":                {},
	"Destination":             {},
	"TransactionUUID":         {},
	"ContentType":             {},
	"Accept":                  {},
	"Status":                  {},
	"RequestDeliveryResponse": {},
	"Headers":                 {},
	"Path":                    {},
	"ServiceName":             {},
	"URL":                     {},
	"PartnerIDs":              {},
	"SessionID":               {},
	"QualityOfService":        {},
}

// isValidWRPFieldReference validates a wrp.* header field reference.
// Returns true if the reference is valid (standard field, Header.*, or Metadata.*).
// Returns false for invalid field names (catches typos early).
func isValidWRPFieldReference(value string) bool {
	// Not a WRP field reference
	if len(value) <= 4 || value[:4] != "wrp." {
		return true
	}

	fieldName := value[4:] // Remove "wrp." prefix

	// Check if it's a Header.* or Metadata.* reference (always valid)
	if strings.HasPrefix(fieldName, "Header.") || strings.HasPrefix(fieldName, "Metadata.") {
		return true
	}

	// Check if it's a valid standard WRP field
	_, ok := validWRPFieldNames[fieldName]
	return ok
}
