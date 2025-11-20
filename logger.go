// SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package wrpkafka

import "github.com/twmb/franz-go/pkg/kgo"

// nopLogger, the default logger, drops everything.
type nopLogger struct{}

func (*nopLogger) Level() kgo.LogLevel { return kgo.LogLevelNone }
func (*nopLogger) Log(kgo.LogLevel, string, ...any) {
}
