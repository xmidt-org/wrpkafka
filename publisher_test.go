// SPDX-FileCopyrightText: 2025 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package wrpkafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/xmidt-org/wrp-go/v5"
)

// newTestPublisher creates a Publisher with the given dynamic config for testing.
func newTestPublisher(dynamicConfig DynamicConfig) *Publisher {
	p := &Publisher{
		Brokers:              []string{"localhost:9092"},
		InitialDynamicConfig: dynamicConfig,
	}

	if err := p.UpdateConfig(dynamicConfig); err != nil {
		panic(fmt.Sprintf("test setup failed updating dynamic config: %v", err))
	}
	return p
}

// TestPublisherLifecycle tests Start and Stop behavior.
func TestPublisherLifecycle(t *testing.T) {
	t.Parallel()
	t.Run("start validates config", func(t *testing.T) {
		t.Parallel()
		p := &Publisher{
			Brokers:              []string{}, // invalid - empty
			InitialDynamicConfig: DynamicConfig{TopicMap: []TopicRoute{{Pattern: "*", Topic: "t"}}},
		}
		err := p.Start()
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrValidation)
	})

	t.Run("start fails if already started", func(t *testing.T) {
		t.Parallel()
		p := newTestPublisher(DynamicConfig{TopicMap: []TopicRoute{{Pattern: "*", Topic: "t"}}})
		p.clientMu.Lock()
		p.client = &mockKafkaClient{} // simulate already started
		p.clientMu.Unlock()

		err := p.Start()
		assert.Error(t, err)
	})

	t.Run("stop flushes and closes client", func(t *testing.T) {
		t.Parallel()
		mockClient := &mockKafkaClient{}
		mockClient.On("Flush", mock.Anything).Return(nil)
		mockClient.On("Close").Return()

		p := newTestPublisher(DynamicConfig{TopicMap: []TopicRoute{{Pattern: "*", Topic: "t"}}})
		p.clientMu.Lock()
		p.client = mockClient
		p.clientMu.Unlock()
		p.logger = &nopLogger{}

		p.Stop(context.Background())
		mockClient.AssertExpectations(t)
	})

	t.Run("stop is idempotent", func(t *testing.T) {
		t.Parallel()
		p := newTestPublisher(DynamicConfig{TopicMap: []TopicRoute{{Pattern: "*", Topic: "t"}}})

		p.Stop(context.Background())
		p.Stop(context.Background()) // Should not panic or error
	})

	t.Run("stop safe when never started", func(t *testing.T) {
		t.Parallel()
		p := newTestPublisher(DynamicConfig{TopicMap: []TopicRoute{{Pattern: "*", Topic: "t"}}})
		p.Stop(context.Background()) // Should not panic
	})
}

// TestProduceQoS tests QoS-aware routing behavior.
func TestProduceQoS(t *testing.T) {
	t.Parallel()
	t.Run("low QoS returns Attempted", func(t *testing.T) {
		t.Parallel()
		mockClient := &mockKafkaClient{}
		mockClient.On("TryProduce", mock.Anything, mock.Anything, mock.Anything).Return()

		p := newTestPublisher(DynamicConfig{TopicMap: []TopicRoute{{Pattern: "*", Topic: "t"}}})
		p.clientMu.Lock()
		p.client = mockClient
		p.clientMu.Unlock()

		msg := &wrp.Message{
			Type:             wrp.SimpleEventMessageType,
			Source:           "mac:112233445566",
			Destination:      "event:test",
			QualityOfService: 10, // low QoS
		}

		outcome, err := p.Produce(context.Background(), msg)
		assert.NoError(t, err)
		assert.Equal(t, Attempted, outcome)
	})

	t.Run("medium QoS returns Queued", func(t *testing.T) {
		t.Parallel()
		mockClient := &mockKafkaClient{}
		mockClient.On("Produce", mock.Anything, mock.Anything, mock.Anything).Return()

		p := newTestPublisher(DynamicConfig{TopicMap: []TopicRoute{{Pattern: "*", Topic: "t"}}})
		p.clientMu.Lock()
		p.client = mockClient
		p.clientMu.Unlock()

		msg := &wrp.Message{
			Type:             wrp.SimpleEventMessageType,
			Source:           "mac:112233445566",
			Destination:      "event:test",
			QualityOfService: 50, // medium QoS
		}

		outcome, err := p.Produce(context.Background(), msg)
		assert.NoError(t, err)
		assert.Equal(t, Queued, outcome)
	})

	t.Run("QoS boundaries", func(t *testing.T) {
		t.Parallel()
		tests := []struct {
			qos            int
			expectedMethod string
		}{
			{0, "TryProduce"},
			{24, "TryProduce"},
			{25, "Produce"},
			{74, "Produce"},
		}

		for _, tt := range tests {
			t.Run(fmt.Sprintf("QoS=%d", tt.qos), func(t *testing.T) {
				t.Parallel()
				mockClient := &mockKafkaClient{}
				mockClient.On("TryProduce", mock.Anything, mock.Anything, mock.Anything).Return()
				mockClient.On("Produce", mock.Anything, mock.Anything, mock.Anything).Return()

				p := newTestPublisher(DynamicConfig{TopicMap: []TopicRoute{{Pattern: "*", Topic: "t"}}})
				p.clientMu.Lock()
				p.client = mockClient
				p.clientMu.Unlock()

				msg := &wrp.Message{
					Type:             wrp.SimpleEventMessageType,
					Source:           "mac:112233445566",
					Destination:      "event:test",
					QualityOfService: wrp.QOSValue(tt.qos),
				}

				_, _ = p.Produce(context.Background(), msg)
				mockClient.AssertCalled(t, tt.expectedMethod, mock.Anything, mock.Anything, mock.Anything)
			})
		}
	})
}

// TestProduceErrors tests error handling.
func TestProduceErrors(t *testing.T) {
	t.Parallel()
	t.Run("not started", func(t *testing.T) {
		t.Parallel()
		p := newTestPublisher(DynamicConfig{TopicMap: []TopicRoute{{Pattern: "*", Topic: "t"}}})
		msg := &wrp.Message{Destination: "event:test"}

		outcome, err := p.Produce(context.Background(), msg)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrNotStarted)
		assert.Equal(t, Failed, outcome)
	})

	t.Run("no topic match", func(t *testing.T) {
		t.Parallel()
		p := newTestPublisher(DynamicConfig{TopicMap: []TopicRoute{{Pattern: "exact", Topic: "t"}}})
		p.clientMu.Lock()
		p.client = &mockKafkaClient{}
		p.clientMu.Unlock()

		msg := &wrp.Message{Destination: "event:nomatch"}

		outcome, err := p.Produce(context.Background(), msg)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrNoTopicMatch)
		assert.Equal(t, Failed, outcome)
	})

	t.Run("context canceled", func(t *testing.T) {
		t.Parallel()
		p := newTestPublisher(DynamicConfig{TopicMap: []TopicRoute{{Pattern: "*", Topic: "t"}}})
		p.clientMu.Lock()
		p.client = &mockKafkaClient{}
		p.clientMu.Unlock()

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // cancel immediately

		msg := &wrp.Message{Destination: "event:test"}

		outcome, err := p.Produce(ctx, msg)
		assert.Error(t, err)
		assert.Equal(t, Failed, outcome)
	})

	t.Run("no records", func(t *testing.T) {
		t.Parallel()
		p := newTestPublisher(DynamicConfig{TopicMap: []TopicRoute{{Pattern: "*", Topic: "t"}}})
		p.clientMu.Lock()
		p.client = &mockKafkaClient{}
		p.clientMu.Unlock()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		msg := &wrp.Message{Destination: "event:test"}

		outcome, err := p.Produce(ctx, msg)
		assert.Error(t, err)
		assert.Equal(t, Failed, outcome)
	})
}

// TestUpdateConfig tests runtime configuration updates.
func TestUpdateConfig(t *testing.T) {
	t.Parallel()
	t.Run("updates config atomically", func(t *testing.T) {
		t.Parallel()
		p := newTestPublisher(DynamicConfig{TopicMap: []TopicRoute{{Pattern: "*", Topic: "old"}}})

		newConfig := DynamicConfig{
			TopicMap: []TopicRoute{{Pattern: "*", Topic: "new"}},
		}

		err := p.UpdateConfig(newConfig)
		assert.NoError(t, err)

		// Verify new config is active
		loaded := p.dynamicConfig.Load()
		assert.Equal(t, "new", loaded.TopicMap[0].Topic)
	})

	t.Run("validates new config", func(t *testing.T) {
		t.Parallel()
		p := newTestPublisher(DynamicConfig{TopicMap: []TopicRoute{{Pattern: "*", Topic: "t"}}})

		invalidConfig := DynamicConfig{TopicMap: []TopicRoute{}} // empty topic map

		err := p.UpdateConfig(invalidConfig)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrValidation)

		// Old config should still be active
		loaded := p.dynamicConfig.Load()
		assert.Equal(t, "t", loaded.TopicMap[0].Topic)
	})
}

// TestEventListeners tests publish event listeners.
func TestEventListeners(t *testing.T) {
	t.Parallel()
	t.Run("listeners are called on success", func(t *testing.T) {
		t.Parallel()
		mockClient := &mockKafkaClient{}
		mockClient.On("TryProduce", mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
			cb := args.Get(2).(func(*kgo.Record, error))
			cb(args.Get(1).(*kgo.Record), nil) // success
		})

		p := newTestPublisher(DynamicConfig{TopicMap: []TopicRoute{{Pattern: "*", Topic: "t"}}})
		p.clientMu.Lock()
		p.client = mockClient
		p.clientMu.Unlock()

		var events []PublishEvent
		cancel := p.AddPublishEventListener(func(e *PublishEvent) {
			events = append(events, *e)
		})
		defer cancel()

		msg := &wrp.Message{
			Type:             wrp.SimpleEventMessageType,
			Source:           "mac:112233445566",
			Destination:      "event:test",
			QualityOfService: 10,
		}

		_, _ = p.Produce(context.Background(), msg)

		// Wait a bit for async callback
		time.Sleep(10 * time.Millisecond)

		assert.NotEmpty(t, events)
		assert.Nil(t, events[0].Error)
	})

	t.Run("initial listeners are registered on Start", func(t *testing.T) {
		t.Parallel()
		called := atomic.Bool{}

		p := &Publisher{
			Brokers: []string{"localhost:9092"},
			InitialDynamicConfig: DynamicConfig{
				TopicMap: []TopicRoute{{Pattern: "*", Topic: "t"}},
			},
			InitialPublishEventListeners: []func(*PublishEvent){
				func(e *PublishEvent) {
					called.Store(true)
				},
			},
		}

		// Set mock client factory
		mockClient := &mockKafkaClient{}
		mockClient.On("TryProduce", mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
			cb := args.Get(2).(func(*kgo.Record, error))
			cb(args.Get(1).(*kgo.Record), nil)
		})
		mockClient.On("Flush", mock.Anything).Return(nil)
		mockClient.On("Close").Return()
		p.clientFactory = func(opts ...kgo.Opt) (kafkaClient, error) {
			return mockClient, nil
		}

		// Start will register the initial listeners
		err := p.Start()
		require.NoError(t, err)
		defer p.Stop(context.Background())

		msg := &wrp.Message{
			Type:             wrp.SimpleEventMessageType,
			Source:           "mac:112233445566",
			Destination:      "event:test",
			QualityOfService: 10,
		}

		p.Produce(context.Background(), msg)
		time.Sleep(10 * time.Millisecond)

		assert.True(t, called.Load())
	})

	t.Run("cancel removes listener", func(t *testing.T) {
		t.Parallel()
		p := newTestPublisher(DynamicConfig{TopicMap: []TopicRoute{{Pattern: "*", Topic: "t"}}})

		callCount := atomic.Int32{}
		cancel := p.AddPublishEventListener(func(e *PublishEvent) {
			callCount.Add(1)
		})

		p.dispatchEvent(&PublishEvent{}, time.Now(), nil)
		assert.Equal(t, int32(1), callCount.Load())

		cancel() // remove listener

		p.dispatchEvent(&PublishEvent{}, time.Now(), nil)
		assert.Equal(t, int32(1), callCount.Load()) // should not increment
	})
}

// TestBufferedRecords tests buffer monitoring.
func TestBufferedRecords(t *testing.T) {
	t.Parallel()
	mockClient := &mockKafkaClient{}
	mockClient.On("BufferedProduceRecords").Return(int64(42))
	mockClient.On("BufferedProduceBytes").Return(int64(1024))

	p := newTestPublisher(DynamicConfig{TopicMap: []TopicRoute{{Pattern: "*", Topic: "t"}}})
	p.MaxBufferedRecords = 100
	p.MaxBufferedBytes = 2048
	p.clientMu.Lock()
	p.client = mockClient
	p.clientMu.Unlock()

	currentRecords, maxRecords, currentBytes, maxBytes := p.BufferedRecords()
	assert.Equal(t, 42, currentRecords)
	assert.Equal(t, 100, maxRecords)
	assert.Equal(t, int64(1024), currentBytes)
	assert.Equal(t, int64(2048), maxBytes)
}

// TestConfigConcurrency tests concurrent config access.
func TestConfigConcurrency(t *testing.T) {
	t.Parallel()
	p := newTestPublisher(DynamicConfig{TopicMap: []TopicRoute{{Pattern: "*", Topic: "t"}}})

	var wg sync.WaitGroup
	const goroutines = 10
	const iterations = 100

	// Concurrent reads
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				cfg := p.dynamicConfig.Load()
				assert.NotNil(t, cfg)
			}
		}()
	}

	// Concurrent writes
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				newConfig := DynamicConfig{
					TopicMap: []TopicRoute{{Pattern: "*", Topic: fmt.Sprintf("topic-%d", idx)}},
				}
				p.UpdateConfig(newConfig)
			}
		}(i)
	}

	wg.Wait()

	// Final config should be valid
	cfg := p.dynamicConfig.Load()
	assert.NotNil(t, cfg)
	assert.NotEmpty(t, cfg.TopicMap)
}

// TestProduceContextCancellation tests context cancellation behavior
func TestProduceContextCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	p := newTestPublisher(DynamicConfig{
		TopicMap: []TopicRoute{{Pattern: "*", Topic: "topic1"}},
	})

	msg := &wrp.Message{
		Type:             wrp.SimpleEventMessageType,
		Source:           "mac:112233445566",
		Destination:      "event:test",
		QualityOfService: 50,
	}

	result, err := p.Produce(ctx, msg)

	assert.Equal(t, Failed, result)
	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)
}

// TestProduceNotStarted tests behavior when client is not started
func TestProduceNotStarted(t *testing.T) {
	t.Parallel()

	p := newTestPublisher(DynamicConfig{
		TopicMap: []TopicRoute{{Pattern: "*", Topic: "topic1"}},
	})
	// Don't set client - simulates not started

	msg := &wrp.Message{
		Type:             wrp.SimpleEventMessageType,
		Source:           "mac:112233445566",
		Destination:      "event:test",
		QualityOfService: 50,
	}

	result, err := p.Produce(context.Background(), msg)

	assert.Equal(t, Failed, result)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrNotStarted)
}

// TestProduce provides table-driven tests for the Produce function
func TestProduce(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		msg            *wrp.Message
		dynamicConfig  DynamicConfig
		mockSetup      func(m *mockKafkaClient)
		expectedResult Outcome
		expectedError  string
		expectCalls    map[string]int // method name -> call count
	}{
		{
			name: "low QoS single topic success",
			msg: &wrp.Message{
				Type:             wrp.SimpleEventMessageType,
				Source:           "mac:112233445566",
				Destination:      "event:test",
				QualityOfService: 10,
			},
			dynamicConfig: DynamicConfig{
				TopicMap: []TopicRoute{{Pattern: "*", Topic: "topic1"}},
			},
			mockSetup: func(m *mockKafkaClient) {
				m.On("TryProduce", mock.Anything, mock.Anything, mock.Anything).Return()
			},
			expectedResult: Attempted,
			expectedError:  "",
			expectCalls:    map[string]int{"TryProduce": 1},
		},
		{
			name: "low QoS multiple topics success",
			msg: &wrp.Message{
				Type:             wrp.SimpleEventMessageType,
				Source:           "mac:112233445566",
				Destination:      "event:test",
				QualityOfService: 0,
			},
			dynamicConfig: DynamicConfig{
				TopicMap: []TopicRoute{
					{Pattern: "test", Topic: "topic1"},
					{Pattern: "test", Topic: "topic2"},
				},
			},
			mockSetup: func(m *mockKafkaClient) {
				m.On("TryProduce", mock.Anything, mock.Anything, mock.Anything).Return()
			},
			expectedResult: Attempted,
			expectedError:  "",
			expectCalls:    map[string]int{"TryProduce": 2},
		},
		{
			name: "medium QoS single topic success",
			msg: &wrp.Message{
				Type:             wrp.SimpleEventMessageType,
				Source:           "mac:112233445566",
				Destination:      "event:test",
				QualityOfService: 50,
			},
			dynamicConfig: DynamicConfig{
				TopicMap: []TopicRoute{{Pattern: "*", Topic: "topic1"}},
			},
			mockSetup: func(m *mockKafkaClient) {
				m.On("Produce", mock.Anything, mock.Anything, mock.Anything).Return()
			},
			expectedResult: Queued,
			expectedError:  "",
			expectCalls:    map[string]int{"Produce": 1},
		},
		{
			name: "medium QoS multiple topics success",
			msg: &wrp.Message{
				Type:             wrp.SimpleEventMessageType,
				Source:           "mac:112233445566",
				Destination:      "event:test",
				QualityOfService: 74,
			},
			dynamicConfig: DynamicConfig{
				TopicMap: []TopicRoute{
					{Pattern: "test", Topic: "topic1"},
					{Pattern: "test", Topic: "topic2"},
					{Pattern: "test", Topic: "topic3"},
				},
			},
			mockSetup: func(m *mockKafkaClient) {
				m.On("Produce", mock.Anything, mock.Anything, mock.Anything).Return()
			},
			expectedResult: Queued,
			expectedError:  "",
			expectCalls:    map[string]int{"Produce": 3},
		},
		{
			name: "high QoS single topic success",
			msg: &wrp.Message{
				Type:             wrp.SimpleEventMessageType,
				Source:           "mac:112233445566",
				Destination:      "event:test",
				QualityOfService: 99,
			},
			dynamicConfig: DynamicConfig{
				TopicMap: []TopicRoute{{Pattern: "*", Topic: "topic1"}},
			},
			mockSetup: func(m *mockKafkaClient) {
				m.On("ProduceSync", mock.Anything, mock.Anything).Return(kgo.ProduceResults{
					{Record: &kgo.Record{Topic: "topic1"}, Err: nil},
				})
			},
			expectedResult: Accepted,
			expectedError:  "",
			expectCalls:    map[string]int{"ProduceSync": 1},
		},
		{
			name: "high QoS multiple topics success",
			msg: &wrp.Message{
				Type:             wrp.SimpleEventMessageType,
				Source:           "mac:112233445566",
				Destination:      "event:test",
				QualityOfService: 75,
			},
			dynamicConfig: DynamicConfig{
				TopicMap: []TopicRoute{
					{Pattern: "test", Topic: "topic1"},
					{Pattern: "test", Topic: "topic2"},
				},
			},
			mockSetup: func(m *mockKafkaClient) {
				m.On("ProduceSync", mock.Anything, mock.Anything).Return(kgo.ProduceResults{
					{Record: &kgo.Record{Topic: "topic1"}, Err: nil},
				}).Times(2)
			},
			expectedResult: Accepted,
			expectedError:  "",
			expectCalls:    map[string]int{"ProduceSync": 2},
		},
		{
			name: "high QoS sync failure",
			msg: &wrp.Message{
				Type:             wrp.SimpleEventMessageType,
				Source:           "mac:112233445566",
				Destination:      "event:test",
				QualityOfService: 80,
			},
			dynamicConfig: DynamicConfig{
				TopicMap: []TopicRoute{{Pattern: "*", Topic: "topic1"}},
			},
			mockSetup: func(m *mockKafkaClient) {
				m.On("ProduceSync", mock.Anything, mock.Anything).Return(kgo.ProduceResults{
					{Record: &kgo.Record{Topic: "topic1"}, Err: assert.AnError},
				})
			},
			expectedResult: Failed,
			expectedError:  "broker rejected message",
			expectCalls:    map[string]int{"ProduceSync": 1},
		},
		{
			name: "QoS boundary values",
			msg: &wrp.Message{
				Type:             wrp.SimpleEventMessageType,
				Source:           "mac:112233445566",
				Destination:      "event:test",
				QualityOfService: 24, // boundary between low and medium
			},
			dynamicConfig: DynamicConfig{
				TopicMap: []TopicRoute{{Pattern: "*", Topic: "topic1"}},
			},
			mockSetup: func(m *mockKafkaClient) {
				m.On("TryProduce", mock.Anything, mock.Anything, mock.Anything).Return()
			},
			expectedResult: Attempted,
			expectedError:  "",
			expectCalls:    map[string]int{"TryProduce": 1},
		},
		{
			name: "QoS boundary values - 25",
			msg: &wrp.Message{
				Type:             wrp.SimpleEventMessageType,
				Source:           "mac:112233445566",
				Destination:      "event:test",
				QualityOfService: 25, // boundary between low and medium
			},
			dynamicConfig: DynamicConfig{
				TopicMap: []TopicRoute{{Pattern: "*", Topic: "topic1"}},
			},
			mockSetup: func(m *mockKafkaClient) {
				m.On("Produce", mock.Anything, mock.Anything, mock.Anything).Return()
			},
			expectedResult: Queued,
			expectedError:  "",
			expectCalls:    map[string]int{"Produce": 1},
		},
		{
			name: "no topic match",
			msg: &wrp.Message{
				Type:             wrp.SimpleEventMessageType,
				Source:           "mac:112233445566",
				Destination:      "event:nomatch",
				QualityOfService: 50,
			},
			dynamicConfig: DynamicConfig{
				TopicMap: []TopicRoute{{Pattern: "different", Topic: "topic1"}},
			},
			mockSetup: func(m *mockKafkaClient) {
				// No calls expected
			},
			expectedResult: Failed,
			expectedError:  "no topic route matched",
			expectCalls:    map[string]int{},
		},
		{
			name: "invalid device ID",
			msg: &wrp.Message{
				Type:             wrp.SimpleEventMessageType,
				Source:           "invalid-device-id",
				Destination:      "event:test",
				QualityOfService: 50,
			},
			dynamicConfig: DynamicConfig{
				TopicMap: []TopicRoute{{Pattern: "*", Topic: "topic1"}},
			},
			mockSetup: func(m *mockKafkaClient) {
				// No calls expected
			},
			expectedResult: Failed,
			expectedError:  "invalid device ID",
			expectCalls:    map[string]int{},
		},
		{
			name: "invalid destination",
			msg: &wrp.Message{
				Type:             wrp.SimpleEventMessageType,
				Source:           "mac:112233445566",
				Destination:      "invalid-destination",
				QualityOfService: 50,
			},
			dynamicConfig: DynamicConfig{
				TopicMap: []TopicRoute{{Pattern: "*", Topic: "topic1"}},
			},
			mockSetup: func(m *mockKafkaClient) {
				// No calls expected
			},
			expectedResult: Failed,
			expectedError:  "invalid locator",
			expectCalls:    map[string]int{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Setup
			mockClient := &mockKafkaClient{}
			tt.mockSetup(mockClient)

			p := newTestPublisher(tt.dynamicConfig)
			p.clientMu.Lock()
			p.client = mockClient
			p.clientMu.Unlock()

			// Execute
			result, err := p.Produce(context.Background(), tt.msg)

			// Assert
			assert.Equal(t, tt.expectedResult, result)
			if tt.expectedError != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
			} else {
				assert.NoError(t, err)
			}

			// Verify method calls
			for method, expectedCount := range tt.expectCalls {
				mockClient.AssertNumberOfCalls(t, method, expectedCount)
			}

			mockClient.AssertExpectations(t)
		})
	}
}

// TestProduceAsyncErrors provides table-driven tests for async error handling
// in the Produce function
func TestProduceAsyncErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                  string
		msg                   *wrp.Message
		dynamicConfig         DynamicConfig
		simulatedError        error
		expectedResult        Outcome
		expectedError         string
		expectedEventCalls    int // Number of events expected to be dispatched
		expectedSuccessEvents int // Number of success events
		expectedErrorEvents   int // Number of error events
	}{
		{
			name: "medium QoS single topic - async retry exhausted error",
			msg: &wrp.Message{
				Type:             wrp.SimpleEventMessageType,
				Source:           "mac:112233445566",
				Destination:      "event:test",
				QualityOfService: 50,
			},
			dynamicConfig: DynamicConfig{
				TopicMap: []TopicRoute{{Pattern: "*", Topic: "topic1"}},
			},
			simulatedError:        errors.New("retries exhausted"),
			expectedResult:        Queued,
			expectedError:         "",
			expectedEventCalls:    2, // 1 success + 1 async error
			expectedSuccessEvents: 1,
			expectedErrorEvents:   1,
		},
		{
			name: "medium QoS single topic - async timeout error",
			msg: &wrp.Message{
				Type:             wrp.SimpleEventMessageType,
				Source:           "mac:112233445566",
				Destination:      "event:test",
				QualityOfService: 60,
			},
			dynamicConfig: DynamicConfig{
				TopicMap: []TopicRoute{{Pattern: "*", Topic: "topic1"}},
			},
			simulatedError:        context.DeadlineExceeded,
			expectedResult:        Queued,
			expectedError:         "",
			expectedEventCalls:    2, // 1 success + 1 async error
			expectedSuccessEvents: 1,
			expectedErrorEvents:   1,
		},
		{
			name: "medium QoS single topic - async success (no error)",
			msg: &wrp.Message{
				Type:             wrp.SimpleEventMessageType,
				Source:           "mac:112233445566",
				Destination:      "event:test",
				QualityOfService: 25,
			},
			dynamicConfig: DynamicConfig{
				TopicMap: []TopicRoute{{Pattern: "*", Topic: "topic1"}},
			},
			simulatedError:        nil, // No error - success case
			expectedResult:        Queued,
			expectedError:         "",
			expectedEventCalls:    1, // 1 success only
			expectedSuccessEvents: 1,
			expectedErrorEvents:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Setup event tracking
			var eventMu sync.Mutex
			var capturedEvents []PublishEvent
			var successEventCount, errorEventCount int

			eventListener := func(event *PublishEvent) {
				eventMu.Lock()
				defer eventMu.Unlock()
				capturedEvents = append(capturedEvents, *event)
				if event.Error == nil {
					successEventCount++
				} else {
					errorEventCount++
				}
			}

			// Setup mock client
			mockClient := &mockKafkaClient{}
			mockClient.On("Produce", mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
				// Get the callback function and call it with the simulated error
				callback := args.Get(2).(func(*kgo.Record, error))
				record := args.Get(1).(*kgo.Record)

				// Simulate async callback
				go func() {
					time.Sleep(1 * time.Millisecond) // Simulate async
					callback(record, tt.simulatedError)
				}()
			}).Return()

			// Setup publisher
			p := newTestPublisher(tt.dynamicConfig)
			p.clientMu.Lock()
			p.client = mockClient
			p.clientMu.Unlock()

			// Add event listener
			cancel := p.AddPublishEventListener(eventListener)
			defer cancel()

			// Execute
			result, err := p.Produce(context.Background(), tt.msg)

			// Assert immediate results
			assert.Equal(t, tt.expectedResult, result)
			if tt.expectedError != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
			} else {
				assert.NoError(t, err)
			}

			// Wait for async callbacks to complete
			assert.Eventually(t, func() bool {
				eventMu.Lock()
				defer eventMu.Unlock()
				return len(capturedEvents) == tt.expectedEventCalls
			}, time.Second, 10*time.Millisecond, "Expected %d events, got %d", tt.expectedEventCalls, len(capturedEvents))

			// Verify event counts
			eventMu.Lock()
			assert.Equal(t, tt.expectedSuccessEvents, successEventCount, "Success event count mismatch")
			assert.Equal(t, tt.expectedErrorEvents, errorEventCount, "Error event count mismatch")

			// Verify error events
			if tt.expectedErrorEvents > 0 && tt.simulatedError != nil {
				for _, event := range capturedEvents {
					if event.Error != nil {
						assert.Equal(t, tt.simulatedError, event.Error, "Error event should contain the simulated error")
						assert.NotEmpty(t, event.ErrorType, "Error event should have ErrorType set")
						assert.True(t, event.Duration > 0, "Error event should have Duration > 0")
					}
				}
			}

			// Verify success events
			if tt.expectedSuccessEvents > 0 {
				successFound := false
				for _, event := range capturedEvents {
					if event.Error == nil {
						successFound = true
						assert.Empty(t, event.ErrorType, "Success event should have empty ErrorType")
						assert.True(t, event.Duration > 0, "Success event should have Duration > 0")
						break
					}
				}
				assert.True(t, successFound, "At least one success event should be found")
			}
			eventMu.Unlock()

			// Verify mock expectations
			mockClient.AssertExpectations(t)
		})
	}
}
