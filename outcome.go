// SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package wrpkafka

// Outcome represents the result of a Produce() operation.
type Outcome int

const (
	// Accepted indicates the message was delivered AND confirmed by Kafka.
	// Only returned for QoS 75-99 synchronous operations.
	Accepted Outcome = iota

	// Queued indicates the message was locally buffered but NOT confirmed with
	// the target Kafka broker.
	// For QoS 25-74: Async retry (callback on result).
	Queued

	// Attempted indicates the message was attempted to be sent to Kafka.  The
	// outcome at the time of return is unknown.  An async callback will provide
	// the final result, but there is no retry.
	// For QoS 0-24: Fire-and-forget (may fail async).
	Attempted

	// Dropped indicates the message was dropped due to buffer full.
	// Only for QoS 0-24 when buffer at capacity.
	Dropped

	// Failed indicates synchronous delivery failed.
	// Only for QoS 75-99 when Kafka rejects or times out.
	Failed
)

// String returns the string representation of the Outcome.
func (o Outcome) String() string {
	switch o {
	case Accepted:
		return "Accepted"
	case Queued:
		return "Queued"
	case Attempted:
		return "Attempted"
	case Dropped:
		return "Dropped"
	case Failed:
		return "Failed"
	default:
		return "Unknown"
	}
}
