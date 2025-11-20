// SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package wrpkafka

import "errors"

var (
	// ErrEncoding indicates msgpack encoding failed.
	ErrEncoding = &metricError{
		metric:  "encoding_error",
		message: "encoding failed",
	}

	// ErrNoTopicMatch indicates no routing rule matched the event type.
	ErrNoTopicMatch = &metricError{
		metric:  "no_topic_match",
		message: "no topic matched",
	}

	// ErrBufferFull indicates buffer capacity exceeded (QoS 0-24 only).
	ErrBufferFull = &metricError{
		metric:  "buffer_full",
		message: "buffer full",
	}

	// ErrBroker indicates Kafka broker rejected the message.
	ErrBroker = &metricError{
		metric:  "broker_error",
		message: "broker error",
	}

	// ErrTimeout indicates request timeout exceeded.
	ErrTimeout = &metricError{
		metric:  "timeout",
		message: "timeout",
	}

	// ErrMissingMetadata indicates metadata field for sharding was not found.
	ErrMissingMetadata = &metricError{
		metric:  "missing_metadata_field",
		message: "missing metadata field",
	}

	// ErrValidation indicates configuration validation failed.
	ErrValidation = &metricError{
		metric:  "validation_error",
		message: "validation error",
	}

	// ErrNotStarted indicates the publisher has not been started.
	ErrNotStarted = &metricError{
		metric:  "not_started",
		message: "publisher not started",
	}

	// ErrAlreadyStarted indicates the publisher has already been started.
	ErrAlreadyStarted = &metricError{
		metric:  "already_started",
		message: "publisher already started",
	}
)

// metricError is an internal error type that wraps errors with a type classification
// for metrics and observability. The errorType field provides a string label for grouping
// errors in metrics systems.
type metricError struct {
	metric  string // Type classification for metrics (e.g., "encoding_error", "validation_error")
	message string // Human-readable message
}

// Error implements the error interface.
func (e *metricError) Error() string {
	return e.message
}

func (e *metricError) Metric() string {
	return e.metric
}

func (e *metricError) Is(target error) bool {
	if t, ok := target.(*metricError); ok {
		return e.message == t.message
	}
	return false
}

// errorType extracts the error type string for metrics classification.
// Walks the error chain to find metricError types.
func errorType(err error) string {
	if err == nil {
		return ""
	}

	// Walk the error chain to find a metricError
	var me *metricError
	if errors.As(err, &me) {
		return me.Metric()
	}

	return "unknown"
}
