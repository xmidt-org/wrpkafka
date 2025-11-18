<!-- SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC -->
<!-- SPDX-License-Identifier: Apache-2.0 -->

# wrpkafka

A high-performance Go library for publishing [WRP](https://github.com/xmidt-org/wrp-go) messages to Apache Kafka.

[![Build Status](https://github.com/xmidt-org/wrpkafka/actions/workflows/ci.yml/badge.svg)](https://github.com/xmidt-org/wrpkafka/actions/workflows/ci.yml)
[![codecov.io](http://codecov.io/github/xmidt-org/wrpkafka/coverage.svg?branch=main)](http://codecov.io/github/xmidt-org/wrpkafka?branch=main)
[![Go Report Card](https://goreportcard.com/badge/github.com/xmidt-org/wrpkafka)](https://goreportcard.com/report/github.com/xmidt-org/wrpkafka)
[![Apache V2 License](https://img.shields.io/badge/license-Apache%20V2-blue.svg)](https://github.com/xmidt-org/wrpkafka/blob/main/LICENSE)
[![GitHub Release](https://img.shields.io/github/release/xmidt-org/wrpkafka.svg)](https://github.com/xmidt-org/wrpkafka/releases)
[![Go Reference](https://pkg.go.dev/badge/github.com/xmidt-org/wrpkafka.svg)](https://pkg.go.dev/github.com/xmidt-org/wrpkafka)

## Summary

wrpkafka provides a production-ready publisher for routing [WRP (Web Routing Protocol)](https://github.com/xmidt-org/wrp-go) messages to Apache Kafka with flexible topic routing, multiple sharding strategies, and quality-of-service guarantees. Built on [franz-go](https://github.com/twmb/franz-go), it offers runtime configuration updates, framework-agnostic observability, and high throughput with minimal allocations.

## Table of Contents

- [Features](#features)
- [Install](#install)
- [Quick Start](#quick-start)
- [Code of Conduct](#code-of-conduct)
- [Contributing](#contributing)

## Features

- **QoS-Aware Routing** - Three quality-of-service levels (0-24: fire-and-forget, 25-74: async retry, 75-99: sync confirmation)
- **Flexible Topic Routing** - Pattern-based routing with exact match, prefix match, and catch-all support
- **Multiple Sharding Strategies** - Round-robin, device ID-based, and metadata field-based distribution across topics
- **Runtime Configuration Updates** - Hot-reload topic routes, compression, and headers without restart
- **Framework-Agnostic Observability** - Event-based listeners compatible with any metrics system
- **Production-Ready** - 95%+ test coverage with comprehensive unit and integration tests
- **High Performance** - Lock-free configuration reads, zero-allocation steady state, 10,000+ msg/sec throughput

## Install

```bash
go get github.com/xmidt-org/wrpkafka
```

Requires Go 1.21+ (for generics support in `atomic.Pointer[T]`).

## Quick Start

```go
package main

import (
    "context"
    "log"

    "github.com/xmidt-org/wrp-go/v5"
    "github.com/xmidt-org/wrpkafka"
)

func main() {
    // Configure the publisher
    config := wrpkafka.Config{
        Brokers: []string{"localhost:9092"},
        InitialDynamicConfig: wrpkafka.DynamicConfig{
            TopicMap: []wrpkafka.TopicRoute{
                {Pattern: "device-status-*", Topic: "device-status"},
                {Pattern: "*", Topic: "device-events"}, // catch-all
            },
        },
    }

    // Create and start publisher
    publisher, err := wrpkafka.New(config)
    if err != nil {
        log.Fatal(err)
    }
    defer publisher.Stop(context.Background())

    if err := publisher.Start(); err != nil {
        log.Fatal(err)
    }

    // Publish a WRP message
    msg := &wrp.Message{
        Type:             wrp.SimpleEventMessageType,
        Source:           "mac:112233445566",
        Destination:      "event:device-status/mac:112233445566",
        QualityOfService: 50, // Medium priority
        Payload:          []byte(`{"status":"online"}`),
    }

    outcome, err := publisher.Produce(context.Background(), msg)
    if err != nil {
        log.Fatalf("Publish failed: %v", err)
    }

    log.Printf("Published with outcome: %v", outcome)
}
```

See the [package examples](https://pkg.go.dev/github.com/xmidt-org/wrpkafka#pkg-examples) for topic routing, sharding strategies, dynamic configuration, headers, observability, and error handling.

Additional documentation:
- [API Reference](https://pkg.go.dev/github.com/xmidt-org/wrpkafka) - Complete GoDoc
- [TESTING.md](TESTING.md) - Testing guide with unit and integration tests
- [Design Proposal](docs/proposal.md) - Architecture and design decisions

## Code of Conduct

This project and everyone participating in it are governed by the [XMidt Code Of Conduct](https://xmidt.io/code_of_conduct/).
By participating, you agree to this Code.

## Contributing

Refer to [CONTRIBUTING.md](CONTRIBUTING.md).
