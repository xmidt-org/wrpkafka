// SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package wrpkafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestValidate tests the validate method.
// Note: splitWildcard is thoroughly tested in TestSplitWildcard.
func TestValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		pattern Pattern
		wantErr bool
	}{
		{"empty pattern", "", true},
		{"valid single wildcard", "device-*", false},
		{"multiple wildcards fails", "foo-*-bar-*", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.pattern.validate()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestCompile tests that compile creates the correct matcher configuration.
// Note: splitWildcard is tested in TestSplitWildcard.
func TestCompile(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		pattern    Pattern
		caseInsen  bool
		wantAll    bool
		wantExact  string
		wantPrefix string
		wantSuffix string
		wantErr    bool
	}{
		{
			name:    "catch-all pattern",
			pattern: "*",
			wantAll: true,
		},
		{
			name:      "exact match",
			pattern:   "device-status",
			wantExact: "device-status",
		},
		{
			name:       "prefix pattern",
			pattern:    "device-*",
			wantPrefix: "device-",
		},
		{
			name:       "suffix pattern",
			pattern:    "*-online",
			wantSuffix: "-online",
		},
		{
			name:       "contains pattern",
			pattern:    "device-*-online",
			wantPrefix: "device-",
			wantSuffix: "-online",
		},
		{
			name:      "escaped asterisk becomes exact",
			pattern:   `star\*rating`,
			wantExact: "star*rating",
		},
		{
			name:      "case insensitive exact",
			pattern:   "Device-Status",
			caseInsen: true,
			wantExact: "Device-Status", // Not lowercased - uses EqualFold in matches()
		},
		{
			name:       "case insensitive prefix",
			pattern:    "Device-*",
			caseInsen:  true,
			wantPrefix: "Device-", // Not lowercased - uses EqualFold in matches()
		},
		{
			name:    "empty pattern fails",
			pattern: "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m, err := tt.pattern.compile(tt.caseInsen)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantAll, m.all)
			assert.Equal(t, tt.caseInsen, m.caseInsensitive)
			assert.Equal(t, tt.wantExact, m.exact)
			assert.Equal(t, tt.wantPrefix, m.prefix)
			assert.Equal(t, tt.wantSuffix, m.suffix)
		})
	}
}

// TestMatches tests the matches method for different matcher configurations.
func TestMatches(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		matcher   *patternMatcher
		eventType string
		want      bool
	}{
		// Catch-all (all=true)
		{
			name:      "catch-all matches anything",
			matcher:   &patternMatcher{all: true},
			eventType: "device-status",
			want:      true,
		},
		{
			name:      "catch-all matches empty",
			matcher:   &patternMatcher{all: true},
			eventType: "",
			want:      true,
		},

		// Exact match (exact != "")
		{
			name:      "exact match success",
			matcher:   &patternMatcher{exact: "device-status"},
			eventType: "device-status",
			want:      true,
		},
		{
			name:      "exact match fail",
			matcher:   &patternMatcher{exact: "device-status"},
			eventType: "device-online",
			want:      false,
		},
		{
			name:      "exact case insensitive match",
			matcher:   &patternMatcher{exact: "device-status", caseInsensitive: true},
			eventType: "DEVICE-STATUS",
			want:      true,
		},

		// Prefix match (prefix != "", suffix == "")
		{
			name:      "prefix match success",
			matcher:   &patternMatcher{prefix: "device-"},
			eventType: "device-status",
			want:      true,
		},
		{
			name:      "prefix match fail",
			matcher:   &patternMatcher{prefix: "device-"},
			eventType: "sensor-status",
			want:      false,
		},
		{
			name:      "prefix match empty suffix",
			matcher:   &patternMatcher{prefix: "device-"},
			eventType: "device-",
			want:      true,
		},
		{
			name:      "prefix case insensitive",
			matcher:   &patternMatcher{prefix: "device-", caseInsensitive: true},
			eventType: "DEVICE-STATUS",
			want:      true,
		},

		// Suffix match (prefix == "", suffix != "")
		{
			name:      "suffix match success",
			matcher:   &patternMatcher{suffix: "-online"},
			eventType: "device-online",
			want:      true,
		},
		{
			name:      "suffix match fail",
			matcher:   &patternMatcher{suffix: "-online"},
			eventType: "device-offline",
			want:      false,
		},
		{
			name:      "suffix match empty prefix",
			matcher:   &patternMatcher{suffix: "-online"},
			eventType: "-online",
			want:      true,
		},
		{
			name:      "suffix case insensitive",
			matcher:   &patternMatcher{suffix: "-online", caseInsensitive: true},
			eventType: "DEVICE-ONLINE",
			want:      true,
		},

		// Contains match (prefix != "", suffix != "")
		{
			name:      "contains match success",
			matcher:   &patternMatcher{prefix: "device-", suffix: "-online"},
			eventType: "device-status-online",
			want:      true,
		},
		{
			name:      "contains match zero chars",
			matcher:   &patternMatcher{prefix: "device-", suffix: "-online"},
			eventType: "device--online",
			want:      true,
		},
		{
			name:      "contains match fail prefix",
			matcher:   &patternMatcher{prefix: "device-", suffix: "-online"},
			eventType: "sensor-status-online",
			want:      false,
		},
		{
			name:      "contains match fail suffix",
			matcher:   &patternMatcher{prefix: "device-", suffix: "-online"},
			eventType: "device-status-offline",
			want:      false,
		},
		{
			name:      "contains too short",
			matcher:   &patternMatcher{prefix: "device-", suffix: "-online"},
			eventType: "device-online",
			want:      false,
		},
		{
			name:      "contains case insensitive",
			matcher:   &patternMatcher{prefix: "device-", suffix: "-online", caseInsensitive: true},
			eventType: "DEVICE-STATUS-ONLINE",
			want:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := tt.matcher.matches(tt.eventType)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestSplitWildcard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    string
		wantPre  string
		wantStar string
		wantPost string
		wantOk   bool
	}{
		{
			input:   "device-status",
			wantPre: "device-status",
			wantOk:  true,
		}, {
			input:    "device-*",
			wantPre:  "device-",
			wantStar: "*",
			wantPost: "",
			wantOk:   true,
		}, {
			input:    "device-*-online",
			wantPre:  "device-",
			wantStar: "*",
			wantPost: "-online",
			wantOk:   true,
		}, {
			input:    `star\*rating`,
			wantPre:  "star*rating",
			wantStar: "",
			wantPost: "",
			wantOk:   true,
		}, {
			input:    "foo-*--*-bar",
			wantPre:  "",
			wantStar: "",
			wantPost: "",
			wantOk:   false,
		}, {
			input:    `foo\*-bar-*`,
			wantPre:  "foo*-bar-",
			wantStar: "*",
			wantPost: "",
			wantOk:   true,
		}, {
			input:    `foo\\\*bar`,
			wantPre:  `foo\*bar`,
			wantStar: "",
			wantPost: "",
			wantOk:   true,
		}, {
			input:    `foo\\\*\*\\bar`,
			wantPre:  `foo\**\bar`,
			wantStar: "",
			wantPost: "",
			wantOk:   true,
		}, {
			input:    `foo\\\*\*\\bar`,
			wantPre:  `foo\**\bar`,
			wantStar: "",
			wantPost: "",
			wantOk:   true,
		}, {
			input:    `f\\o*o\\\*\*\\bar`,
			wantPre:  `f\o`,
			wantStar: "*",
			wantPost: `o\**\bar`,
			wantOk:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()

			gotPre, gotStar, gotPost, gotOk := splitWildcard(tt.input)
			assert.Equal(t, tt.wantPre, gotPre)
			assert.Equal(t, tt.wantStar, gotStar)
			assert.Equal(t, tt.wantPost, gotPost)
			assert.Equal(t, tt.wantOk, gotOk)
		})
	}
}
