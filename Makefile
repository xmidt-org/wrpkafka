# SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC
# SPDX-License-Identifier: Apache-2.0
# Makefile for wrpkafka

.PHONY: test test-unit test-integration test-integration-coverage test-all clean coverage help

# Default target
.DEFAULT_GOAL := help

## test: Run unit tests only (fast)
test: test-unit

## test-unit: Run unit tests with coverage
test-unit:
	@echo "Running unit tests..."
	@go test -v -race -cover ./...

## test-integration: Run integration tests (requires Docker/Podman)
test-integration:
	@echo "Running integration tests (requires Docker/Podman)..."
	@DOCKER_HOST=unix:///run/user/$(shell id -u)/podman/podman.sock \
	TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=unix:///run/user/$(shell id -u)/podman/podman.sock \
	TESTCONTAINERS_RYUK_DISABLED=true \
	go test -v -race -tags=integration -timeout 10m ./...

## test-integration-coverage: Run integration tests with coverage report
test-integration-coverage:
	@echo "Running integration tests with coverage (requires Docker/Podman)..."
	@DOCKER_HOST=unix:///run/user/$(shell id -u)/podman/podman.sock \
	TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=unix:///run/user/$(shell id -u)/podman/podman.sock \
	TESTCONTAINERS_RYUK_DISABLED=true \
	go test -race -tags=integration -timeout 10m -coverprofile=coverage-integration.out -covermode=atomic ./...
	@echo ""
	@echo "=== Integration Test Coverage ==="
	@go tool cover -func=coverage-integration.out | tail -1
	@echo ""
	@go tool cover -html=coverage-integration.out -o coverage-integration.html
	@echo "Detailed HTML coverage report: coverage-integration.html"

## test-all: Run both unit and integration tests
test-all: test-unit test-integration

## coverage: Generate coverage report
coverage:
	@echo "Generating coverage report..."
	@go test -race -coverprofile=coverage.out ./...
	@go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

## coverage-integration: Generate coverage for integration tests
coverage-integration:
	@echo "Generating integration test coverage..."
	@DOCKER_HOST=unix:///run/user/$(shell id -u)/podman/podman.sock \
	TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=unix:///run/user/$(shell id -u)/podman/podman.sock \
	TESTCONTAINERS_RYUK_DISABLED=true \
	go test -race -tags=integration -coverprofile=coverage-integration.out ./...
	@go tool cover -html=coverage-integration.out -o coverage-integration.html
	@echo "Integration coverage report: coverage-integration.html"

## clean: Clean up generated files
clean:
	@echo "Cleaning up..."
	@rm -f coverage.out coverage.html coverage-integration.out coverage-integration.html

## fmt: Format code
fmt:
	@echo "Formatting code..."
	@go fmt ./...

## vet: Run go vet
vet:
	@echo "Running go vet..."
	@go vet ./...

## lint: Run golangci-lint (if installed)
lint:
	@echo "Running linters..."
	@golangci-lint run || echo "golangci-lint not installed"

## tidy: Tidy go modules
tidy:
	@echo "Tidying go modules..."
	@go mod tidy

## help: Show this help message
help:
	@echo "Available targets:"
	@sed -n 's/^##//p' ${MAKEFILE_LIST} | column -t -s ':' | sed -e 's/^/ /'
