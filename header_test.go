// SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package wrpkafka

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/xmidt-org/wrp-go/v5"
)

// TestBuildHeaders tests header extraction and building.
func TestBuildHeaders(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		headerConfig map[string][]string
		msg          *wrp.Message
		wantCount    int
		checkHeaders func(t *testing.T, headers []kgo.RecordHeader)
	}{
		{
			name: "literal values only",
			headerConfig: map[string][]string{
				"X-Service": {"my-service"},
				"X-Region":  {"us-west-2"},
			},
			msg:       &wrp.Message{},
			wantCount: 2,
			checkHeaders: func(t *testing.T, headers []kgo.RecordHeader) {
				hasHeader(t, headers, "X-Service", "my-service")
				hasHeader(t, headers, "X-Region", "us-west-2")
			},
		},
		{
			name: "wrp.* field references",
			headerConfig: map[string][]string{
				"X-Source":      {"wrp.Source"},
				"X-Destination": {"wrp.Destination"},
				"X-TxnUUID":     {"wrp.TransactionUUID"},
			},
			msg: &wrp.Message{
				Source:          "mac:112233445566",
				Destination:     "event:online",
				TransactionUUID: "12345-67890",
			},
			wantCount: 3,
			checkHeaders: func(t *testing.T, headers []kgo.RecordHeader) {
				hasHeader(t, headers, "X-Source", "mac:112233445566")
				hasHeader(t, headers, "X-Destination", "event:online")
				hasHeader(t, headers, "X-TxnUUID", "12345-67890")
			},
		},
		{
			name: "mixed literal and wrp.* references",
			headerConfig: map[string][]string{
				"X-Service": {"my-service"},
				"X-Source":  {"wrp.Source"},
			},
			msg: &wrp.Message{
				Source: "mac:112233445566",
			},
			wantCount: 2,
			checkHeaders: func(t *testing.T, headers []kgo.RecordHeader) {
				hasHeader(t, headers, "X-Service", "my-service")
				hasHeader(t, headers, "X-Source", "mac:112233445566")
			},
		},
		{
			name: "empty wrp.* field skipped",
			headerConfig: map[string][]string{
				"X-Source": {"wrp.Source"},
				"X-TxnID":  {"wrp.TransactionUUID"},
			},
			msg: &wrp.Message{
				Source: "mac:112233445566",
				// TransactionUUID is empty
			},
			wantCount: 1, // Only X-Source should be present
			checkHeaders: func(t *testing.T, headers []kgo.RecordHeader) {
				hasHeader(t, headers, "X-Source", "mac:112233445566")
				if hasHeaderKey(headers, "X-TxnID") {
					t.Error("X-TxnID should not be present (empty field)")
				}
			},
		},
		{
			name:         "nil headerConfig",
			headerConfig: nil,
			msg:          &wrp.Message{},
			wantCount:    0,
		},
		{
			name:         "empty headerConfig",
			headerConfig: map[string][]string{},
			msg:          &wrp.Message{},
			wantCount:    0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dc := &DynamicConfig{
				Headers: tt.headerConfig,
			}
			headers := dc.headers(tt.msg)

			if len(headers) != tt.wantCount {
				t.Errorf("headers() returned %d headers, want %d", len(headers), tt.wantCount)
			}

			if tt.checkHeaders != nil {
				tt.checkHeaders(t, headers)
			}
		})
	}
}

// TestExtractWRPField tests WRP field extraction by name.
func TestExtractWRPField(t *testing.T) {
	t.Parallel()
	status := int64(200)
	rdr := int64(42)
	msg := &wrp.Message{
		Type:                    wrp.SimpleEventMessageType,
		Source:                  "mac:112233445566",
		Destination:             "event:online",
		TransactionUUID:         "12345-67890",
		ContentType:             "application/json",
		Accept:                  "application/msgpack",
		Status:                  &status,
		RequestDeliveryResponse: &rdr,
		Path:                    "/api/v2/device",
		ServiceName:             "talaria",
		URL:                     "https://example.com/webhook",
		SessionID:               "session-123",
		Headers:                 []string{"X-Custom: value1", "X-Another: value2"},
		PartnerIDs:              []string{"partner1", "partner2"},
		QualityOfService:        wrp.QOSValue(75),
		Metadata: map[string]string{
			"region": "us-west",
			"env":    "prod",
		},
	}

	tests := []struct {
		name      string
		fieldName string
		want      []string
	}{
		{"Type", "Type", []string{"SimpleEventMessageType"}},
		{"Source", "Source", []string{"mac:112233445566"}},
		{"Destination", "Destination", []string{"event:online"}},
		{"TransactionUUID", "TransactionUUID", []string{"12345-67890"}},
		{"ContentType", "ContentType", []string{"application/json"}},
		{"Accept", "Accept", []string{"application/msgpack"}},
		{"Status", "Status", []string{"200"}},
		{"RequestDeliveryResponse", "RequestDeliveryResponse", []string{"42"}},
		{"Path", "Path", []string{"/api/v2/device"}},
		{"ServiceName", "ServiceName", []string{"talaria"}},
		{"URL", "URL", []string{"https://example.com/webhook"}},
		{"SessionID", "SessionID", []string{"session-123"}},
		{"Headers", "Headers", []string{"X-Custom: value1", "X-Another: value2"}},
		{"PartnerIDs", "PartnerIDs", []string{"partner1", "partner2"}},
		{"QualityOfService", "QualityOfService", []string{"75"}},
		{"metadata - region", "Metadata.region", []string{"us-west"}},
		{"metadata - env", "Metadata.env", []string{"prod"}},
		{"Status - nil", "Status", nil},
		{"RequestDeliveryResponse - nil", "RequestDeliveryResponse", nil},
		{"unknown field", "unknown", nil},
		{"nil message", "Source", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var testMsg *wrp.Message
			switch tt.name {
			case "nil message":
				testMsg = nil
			case "Status - nil", "RequestDeliveryResponse - nil":
				// Create message without Status/RequestDeliveryResponse pointers
				testMsg = &wrp.Message{
					Source: "mac:112233445566",
				}
			default:
				testMsg = msg
			}

			got := extractWRPField(testMsg, tt.fieldName)
			if len(got) != len(tt.want) {
				t.Errorf("extractWRPField(%q) = %v, want %v", tt.fieldName, got, tt.want)
				return
			}

			// Check all elements for multi-valued fields
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("extractWRPField(%q)[%d] = %q, want %q", tt.fieldName, i, got[i], tt.want[i])
				}
			}
		})
	}
}

// TestExtractWRPField_HTTPHeaders tests extracting individual HTTP headers using "Header.X" syntax.
func TestExtractWRPField_HTTPHeaders(t *testing.T) {
	t.Parallel()
	msg := &wrp.Message{
		Headers: []string{
			"Content-Type: application/json",
			"X-Custom-Header: value1",
			"X-Custom-Header: value2", // HTTP allows multiple values for same header
			"X-Device-ID: mac:112233445566",
			"Authorization: Bearer token123",
			"accept-encoding: gzip, deflate",    // Test case-insensitive matching
			"MalformedHeaderNoColon",            // Should be skipped
			"X-Empty:",                          // Empty value
			"  X-Whitespace  :  spaced value  ", // Test whitespace trimming
		},
	}

	tests := []struct {
		name      string
		fieldName string
		want      []string
	}{
		{
			name:      "single header value",
			fieldName: "Header.Content-Type",
			want:      []string{"application/json"},
		},
		{
			name:      "multiple values for same header",
			fieldName: "Header.X-Custom-Header",
			want:      []string{"value1", "value2"},
		},
		{
			name:      "case-insensitive header name",
			fieldName: "Header.ACCEPT-ENCODING",
			want:      []string{"gzip, deflate"},
		},
		{
			name:      "case-insensitive header name (lowercase query)",
			fieldName: "Header.authorization",
			want:      []string{"Bearer token123"},
		},
		{
			name:      "header not found",
			fieldName: "Header.X-Missing",
			want:      nil,
		},
		{
			name:      "empty header value",
			fieldName: "Header.X-Empty",
			want:      []string{""},
		},
		{
			name:      "whitespace trimming",
			fieldName: "Header.X-Whitespace",
			want:      []string{"spaced value"},
		},
		{
			name:      "malformed header skipped",
			fieldName: "Header.MalformedHeaderNoColon",
			want:      nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := extractWRPField(msg, tt.fieldName)

			// Compare lengths first
			if len(got) != len(tt.want) {
				t.Errorf("extractWRPField(%q) returned %d values, want %d\ngot:  %v\nwant: %v",
					tt.fieldName, len(got), len(tt.want), got, tt.want)
				return
			}

			// Compare each value
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("extractWRPField(%q)[%d] = %q, want %q",
						tt.fieldName, i, got[i], tt.want[i])
				}
			}
		})
	}
}

// TestExtractWRPField_Metadata tests extracting metadata fields using "Metadata.X" syntax.
func TestExtractWRPField_Metadata(t *testing.T) {
	t.Parallel()
	msg := &wrp.Message{
		Metadata: map[string]string{
			"region":      "us-west-2",
			"environment": "production",
			"tenant":      "acme-corp",
			"version":     "1.2.3",
			"empty":       "", // Empty value
		},
	}

	tests := []struct {
		name      string
		fieldName string
		want      []string
	}{
		{
			name:      "extract region metadata",
			fieldName: "Metadata.region",
			want:      []string{"us-west-2"},
		},
		{
			name:      "extract environment metadata",
			fieldName: "Metadata.environment",
			want:      []string{"production"},
		},
		{
			name:      "extract tenant metadata",
			fieldName: "Metadata.tenant",
			want:      []string{"acme-corp"},
		},
		{
			name:      "extract version metadata",
			fieldName: "Metadata.version",
			want:      []string{"1.2.3"},
		},
		{
			name:      "metadata key not found",
			fieldName: "Metadata.missing-key",
			want:      nil,
		},
		{
			name:      "empty metadata value returns nil",
			fieldName: "Metadata.empty",
			want:      nil,
		},
		{
			name:      "nil metadata map",
			fieldName: "Metadata.anything",
			want:      nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			testMsg := msg
			if tt.name == "nil metadata map" {
				testMsg = &wrp.Message{} // No metadata
			}

			got := extractWRPField(testMsg, tt.fieldName)

			// Compare lengths first
			if len(got) != len(tt.want) {
				t.Errorf("extractWRPField(%q) returned %d values, want %d\ngot:  %v\nwant: %v",
					tt.fieldName, len(got), len(tt.want), got, tt.want)
				return
			}

			// Compare each value
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("extractWRPField(%q)[%d] = %q, want %q",
						tt.fieldName, i, got[i], tt.want[i])
				}
			}
		})
	}
}

// TestBuildHeaders_WithMetadata tests that metadata fields can be extracted and used in Kafka headers.
func TestBuildHeaders_WithMetadata(t *testing.T) {
	t.Parallel()
	msg := &wrp.Message{
		Source: "mac:112233445566",
		Metadata: map[string]string{
			"region":      "us-west-2",
			"environment": "production",
			"tenant":      "acme-corp",
		},
	}

	tests := []struct {
		name         string
		headerConfig map[string][]string
		wantHeaders  map[string][]string
	}{
		{
			name: "extract single metadata field",
			headerConfig: map[string][]string{
				"x-region": {"wrp.Metadata.region"},
			},
			wantHeaders: map[string][]string{
				"x-region": {"us-west-2"},
			},
		},
		{
			name: "extract multiple metadata fields",
			headerConfig: map[string][]string{
				"x-region":      {"wrp.Metadata.region"},
				"x-environment": {"wrp.Metadata.environment"},
				"x-tenant":      {"wrp.Metadata.tenant"},
			},
			wantHeaders: map[string][]string{
				"x-region":      {"us-west-2"},
				"x-environment": {"production"},
				"x-tenant":      {"acme-corp"},
			},
		},
		{
			name: "mix metadata with WRP fields",
			headerConfig: map[string][]string{
				"source":   {"wrp.Source"},
				"x-region": {"wrp.Metadata.region"},
			},
			wantHeaders: map[string][]string{
				"source":   {"mac:112233445566"},
				"x-region": {"us-west-2"},
			},
		},
		{
			name: "metadata key not found - no header created",
			headerConfig: map[string][]string{
				"x-missing": {"wrp.Metadata.does-not-exist"},
			},
			wantHeaders: map[string][]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dc := &DynamicConfig{
				Headers: tt.headerConfig,
			}
			headers := dc.headers(msg)

			// Build a map of actual header values for easier comparison
			actualHeaders := make(map[string][]string)
			for _, h := range headers {
				actualHeaders[h.Key] = append(actualHeaders[h.Key], string(h.Value))
			}

			// Check each expected header
			for key, wantValues := range tt.wantHeaders {
				gotValues, ok := actualHeaders[key]
				if !ok {
					t.Errorf("header key %q not found in result", key)
					continue
				}

				if len(gotValues) != len(wantValues) {
					t.Errorf("header %q has %d values, want %d\ngot:  %v\nwant: %v",
						key, len(gotValues), len(wantValues), gotValues, wantValues)
					continue
				}

				for i, wantVal := range wantValues {
					if gotValues[i] != wantVal {
						t.Errorf("header %q[%d] = %q, want %q", key, i, gotValues[i], wantVal)
					}
				}
			}

			// Ensure no unexpected headers were created
			if len(actualHeaders) != len(tt.wantHeaders) {
				t.Errorf("got %d headers, want %d", len(actualHeaders), len(tt.wantHeaders))
			}
		})
	}
}

// TestBuildHeaders_WithHTTPHeaders tests that HTTP headers can be extracted and used in Kafka headers.
func TestBuildHeaders_WithHTTPHeaders(t *testing.T) {
	t.Parallel()
	msg := &wrp.Message{
		Source: "mac:112233445566",
		Headers: []string{
			"Content-Type: application/json",
			"X-Partner-ID: partner1",
			"X-Partner-ID: partner2",
		},
	}

	tests := []struct {
		name         string
		headerConfig map[string][]string
		wantHeaders  map[string][]string // Map of key to expected values
	}{
		{
			name: "extract single HTTP header",
			headerConfig: map[string][]string{
				"kafka-content-type": {"wrp.Header.Content-Type"},
			},
			wantHeaders: map[string][]string{
				"kafka-content-type": {"application/json"},
			},
		},
		{
			name: "extract multi-valued HTTP header",
			headerConfig: map[string][]string{
				"partner-id": {"wrp.Header.X-Partner-ID"},
			},
			wantHeaders: map[string][]string{
				"partner-id": {"partner1", "partner2"},
			},
		},
		{
			name: "mix HTTP header and WRP field",
			headerConfig: map[string][]string{
				"source":       {"wrp.Source"},
				"content-type": {"wrp.Header.Content-Type"},
			},
			wantHeaders: map[string][]string{
				"source":       {"mac:112233445566"},
				"content-type": {"application/json"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dc := &DynamicConfig{
				Headers: tt.headerConfig,
			}
			headers := dc.headers(msg)

			// Build a map of actual header values for easier comparison
			actualHeaders := make(map[string][]string)
			for _, h := range headers {
				actualHeaders[h.Key] = append(actualHeaders[h.Key], string(h.Value))
			}

			// Check each expected header
			for key, wantValues := range tt.wantHeaders {
				gotValues, ok := actualHeaders[key]
				if !ok {
					t.Errorf("header key %q not found in result", key)
					continue
				}

				if len(gotValues) != len(wantValues) {
					t.Errorf("header %q has %d values, want %d\ngot:  %v\nwant: %v",
						key, len(gotValues), len(wantValues), gotValues, wantValues)
					continue
				}

				for i, wantVal := range wantValues {
					if gotValues[i] != wantVal {
						t.Errorf("header %q[%d] = %q, want %q", key, i, gotValues[i], wantVal)
					}
				}
			}
		})
	}
}

// TestBuildHeaders_MixedSources tests combining WRP fields, HTTP headers, and metadata.
func TestBuildHeaders_MixedSources(t *testing.T) {
	t.Parallel()
	msg := &wrp.Message{
		Source:      "mac:112233445566",
		Destination: "event:device-status/online",
		Headers: []string{
			"Content-Type: application/json",
			"X-Partner-ID: partner1",
			"X-Partner-ID: partner2",
		},
		Metadata: map[string]string{
			"region":      "us-west-2",
			"environment": "production",
			"tenant":      "acme-corp",
		},
	}

	tests := []struct {
		name         string
		headerConfig map[string][]string
		wantHeaders  map[string][]string
	}{
		{
			name: "combine all three sources",
			headerConfig: map[string][]string{
				"device-id":     {"wrp.Source"},              // WRP field
				"content-type":  {"wrp.Header.Content-Type"}, // HTTP header
				"region":        {"wrp.Metadata.region"},     // Metadata
				"literal-value": {"static-content"},          // Literal
			},
			wantHeaders: map[string][]string{
				"device-id":     {"mac:112233445566"},
				"content-type":  {"application/json"},
				"region":        {"us-west-2"},
				"literal-value": {"static-content"},
			},
		},
		{
			name: "multi-valued HTTP header with single-valued WRP and metadata",
			headerConfig: map[string][]string{
				"partner-id":  {"wrp.Header.X-Partner-ID"},  // Multi-valued
				"source":      {"wrp.Source"},               // Single-valued WRP field
				"environment": {"wrp.Metadata.environment"}, // Single-valued metadata
			},
			wantHeaders: map[string][]string{
				"partner-id":  {"partner1", "partner2"},
				"source":      {"mac:112233445566"},
				"environment": {"production"},
			},
		},
		{
			name: "all extraction types in one config",
			headerConfig: map[string][]string{
				"x-source":      {"wrp.Source"},
				"x-destination": {"wrp.Destination"},
				"x-content":     {"wrp.Header.Content-Type"},
				"x-partner":     {"wrp.Header.X-Partner-ID"},
				"x-region":      {"wrp.Metadata.region"},
				"x-tenant":      {"wrp.Metadata.tenant"},
				"x-service":     {"my-service"},
			},
			wantHeaders: map[string][]string{
				"x-source":      {"mac:112233445566"},
				"x-destination": {"event:device-status/online"},
				"x-content":     {"application/json"},
				"x-partner":     {"partner1", "partner2"},
				"x-region":      {"us-west-2"},
				"x-tenant":      {"acme-corp"},
				"x-service":     {"my-service"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dc := &DynamicConfig{
				Headers: tt.headerConfig,
			}
			headers := dc.headers(msg)

			// Build a map of actual header values for easier comparison
			actualHeaders := make(map[string][]string)
			for _, h := range headers {
				actualHeaders[h.Key] = append(actualHeaders[h.Key], string(h.Value))
			}

			// Check each expected header
			for key, wantValues := range tt.wantHeaders {
				gotValues, ok := actualHeaders[key]
				if !ok {
					t.Errorf("header key %q not found in result", key)
					continue
				}

				if len(gotValues) != len(wantValues) {
					t.Errorf("header %q has %d values, want %d\ngot:  %v\nwant: %v",
						key, len(gotValues), len(wantValues), gotValues, wantValues)
					continue
				}

				for i, wantVal := range wantValues {
					if gotValues[i] != wantVal {
						t.Errorf("header %q[%d] = %q, want %q", key, i, gotValues[i], wantVal)
					}
				}
			}

			// Ensure no unexpected headers were created
			if len(actualHeaders) != len(tt.wantHeaders) {
				t.Errorf("got %d headers, want %d", len(actualHeaders), len(tt.wantHeaders))
			}
		})
	}
}

// Helper to check if a header with key/value exists.
func hasHeader(t *testing.T, headers []kgo.RecordHeader, key, value string) {
	t.Helper()
	for _, h := range headers {
		if h.Key == key && string(h.Value) == value {
			return
		}
	}
	t.Errorf("header %q=%q not found", key, value)
}

// Helper to check if a header key exists.
func hasHeaderKey(headers []kgo.RecordHeader, key string) bool {
	for _, h := range headers {
		if h.Key == key {
			return true
		}
	}
	return false
}
