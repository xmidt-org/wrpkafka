// SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package wrpkafka

import (
	"errors"
	"fmt"
	"strings"
)

// Acks specifies the broker acknowledgment requirements.
type Acks string

const (
	// AcksAll requires all ISR replicas to acknowledge (strongest durability).
	AcksAll Acks = "all"

	// AcksLeader requires only the leader replica to acknowledge.
	AcksLeader Acks = "leader"

	// AcksNone requires no acknowledgment (fire-and-forget).
	AcksNone Acks = "none"
)

var acksTypes map[Acks]struct{}
var acksList []string

func init() {
	list := []Acks{
		AcksAll,
		AcksLeader,
		AcksNone,
	}

	acksTypes = make(map[Acks]struct{})
	for _, a := range list {
		acksTypes[a] = struct{}{}
		acksList = append(acksList, string(a))
	}
}

// validateAcks validates the Acks enum value.
func validateAcks(acks Acks) error {
	if acks == "" {
		return nil
	}

	_, ok := acksTypes[acks]
	if ok {
		return nil
	}

	list := strings.Join(acksList, "', '")
	list = "'" + list + "'"
	return errors.Join(ErrValidation,
		fmt.Errorf("acks '%s' is invalid: must be %s or empty", acks, list))
}
