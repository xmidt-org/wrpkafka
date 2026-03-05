// SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package wrpkafka

import (
	"errors"
	"fmt"
	"strings"
)

// Pattern is a simplified glob for event type matching.
//
// Supported patterns:
//
//	"*"              - Matches any event type
//	"exact"          - Matches "exact" only
//	"device-*"       - Matches "device-" followed by 1+ characters
//	"*-online"       - Matches 1+ characters followed by "-online"
//	"device-*-online" - Matches "device-", 1+ chars, then "-online"
//
// Escaping asterisks:
//
//	Use backslash to match literal asterisk character.
//
//	In JSON/YAML:  "star\\*ratings"  → matches "star*ratings"
//	In Go code:    "star\\*ratings"  → matches "star*ratings"
//
//	(The double backslash in source becomes single backslash in the pattern)
//
// Rules:
//   - At most one unescaped * per pattern
//   - * matches zero or more characters
//   - Case-sensitive unless CaseInsensitive flag is set
//
// Note: This is not filepath.Match. The pattern syntax is intentionally simplified
// for performance and clarity.
type Pattern string

type Patterns struct {
	Patterns    []Pattern
	RegexFields []string
}

// compile parses and optimizes the pattern for efficient repeated matching.
// This method performs all pattern analysis once, returning a matcher that
// can quickly match event types without re-parsing.
func (p Pattern) compile(caseInsensitive bool) (*patternMatcher, error) {
	// Validate first
	if err := p.validate(); err != nil {
		return nil, err
	}

	pattern := string(p)

	// Catch-all pattern - matches everything
	if pattern == "*" {
		return &patternMatcher{
			all: true,
		}, nil
	}

	before, wildcard, after, _ := splitWildcard(pattern)

	if wildcard == "" {
		return &patternMatcher{
			caseInsensitive: caseInsensitive,
			exact:           before,
		}, nil
	}

	rv := patternMatcher{
		caseInsensitive: caseInsensitive,
		prefix:          before,
		suffix:          after,
	}

	return &rv, nil
}

// patternMatcher is a compiled, optimized pattern matcher.
// It pre-processes the pattern once for efficient repeated matching.
type patternMatcher struct {
	all             bool
	caseInsensitive bool   // Whether to lowercase eventType during matching
	exact           string // For exact match: unescaped, optionally lowercased pattern
	prefix          string // For prefix match: prefix without trailing *, optionally lowercased
	suffix          string // For suffix match: suffix without leading *, optionally lowercased
}

// matches checks if an event type matches this compiled pattern.
// This is optimized for performance - all pattern parsing was done during Compile().
func (pm *patternMatcher) matches(eventType string) bool {
	if pm.all {
		return true
	}

	if pm.exact != "" {
		if pm.caseInsensitive {
			return strings.EqualFold(eventType, pm.exact)
		}
		return eventType == pm.exact
	}

	if len(pm.prefix) > len(eventType) {
		return false
	}
	prefix := eventType[:len(pm.prefix)]

	if len(pm.suffix) > len(eventType) {
		return false
	}
	suffix := eventType[len(eventType)-len(pm.suffix):]

	if len(eventType) < len(pm.prefix)+len(pm.suffix) {
		return false
	}

	if !pm.caseInsensitive {
		return prefix == pm.prefix && suffix == pm.suffix
	}

	return strings.EqualFold(prefix, pm.prefix) && strings.EqualFold(suffix, pm.suffix)
}

// validate validates the pattern syntax according to simplified glob rules.
// Supported patterns:
// - "*" (catch-all)
// - "exact" (exact match)
// - "prefix-*" (prefix match - only trailing asterisk)
// - "foo\*" (escaped asterisk - matches literal "*")
func (p Pattern) validate() error {
	pattern := string(p)
	if pattern == "" {
		return errors.Join(ErrValidation, fmt.Errorf("pattern must not be empty"))
	}

	if _, _, _, ok := splitWildcard(pattern); !ok {
		return errors.Join(ErrValidation, fmt.Errorf("pattern '%s' is invalid: at most one unescaped '*' is allowed", pattern))
	}

	return nil
}

func (ps Patterns) validate() error {
	if len(ps.Patterns) == 0 {
		return errors.Join(ErrValidation, fmt.Errorf("patterns must not be empty"))
	}

	for i, p := range ps.Patterns {
		if err := p.validate(); err != nil {
			return fmt.Errorf("pattern %d: %w", i, err)
		}
	}
	return nil
}

// splitWildcard returns before, wildcard, after, ok according to rules:
// - One unescaped '*' → split at that '*' with ok=true.
// - Zero unescaped '*' → return (s, "", "") with ok=true.
// - More than one unescaped '*' → ("","","", false).
// Unescaped means the '*' has an even count of preceding '\' characters.
func splitWildcard(s string) (before, wildcard, after string, ok bool) {
	star := -1
	for i := 0; i < len(s); i++ {
		if s[i] != '*' {
			continue
		}
		// Count preceding backslashes
		bs := 0
		for j := i - 1; j >= 0 && s[j] == '\\'; j-- {
			bs++
		}
		// Even # of '\' => unescaped '*'
		if bs%2 == 0 {
			if star >= 0 {
				// Second unescaped '*': invalid
				return "", "", "", false
			}
			star = i
		}
	}

	if star < 0 {
		// No unescaped '*'
		before = s
	} else {
		before = s[:star]
		after = s[star+1:]
		wildcard = "*"
	}

	// Exactly one unescaped '*'
	before = strings.ReplaceAll(before, `\*`, `*`)
	before = strings.ReplaceAll(before, `\\`, `\`)
	after = strings.ReplaceAll(after, `\*`, `*`)
	after = strings.ReplaceAll(after, `\\`, `\`)
	return before, wildcard, after, true
}
