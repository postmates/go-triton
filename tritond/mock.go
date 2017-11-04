package tritond

import (
	"context"
	"sync"
)

const defaultMockWriteBuffer = 100

// NewMockClient returns a new MockClient
func NewMockClient() *MockClient {
	return &MockClient{
		lock:           new(sync.Mutex),
		PartitionCount: make(map[string]int),
		StreamData:     make(map[string]([](map[string]interface{}))),
		WriteSignal:    make(chan bool, defaultMockWriteBuffer),
	}
}

// MockClient implements a client that stores the messages in memory
type MockClient struct {
	StreamData     map[string]([](map[string]interface{}))
	PartitionCount map[string]int
	WriteSignal    chan bool
	lock           *sync.Mutex
}

// Put implements the client interface
func (c *MockClient) Put(ctx context.Context, stream, partition string, data map[string]interface{}) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	messages, _ := c.StreamData[stream]
	c.StreamData[stream] = append(messages, data)
	c.PartitionCount[partition]++

	select {
	case c.WriteSignal <- true:
	default:
		// If buffer is full, don't block.
		// User should set the buffer to an appropriate size if they care about this signal
	}

	return nil
}

// Close is a noop for a mock client. Meets `Client` inteface
func (c *MockClient) Close(ctx context.Context) error {
	return nil
}

// Reset resets MockClient to the initial state
func (c *MockClient) Reset() {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.StreamData = make(map[string]([](map[string]interface{})))
	c.PartitionCount = make(map[string]int)
	c.WriteSignal = make(chan bool, cap(c.WriteSignal))
}

type noopClient struct{}

// NewNoopClient creates a `Client` that performs no operation and allways returns successfully.
func NewNoopClient() Client {
	return &noopClient{}
}

// Put meets the `Client` interface.
func (nc *noopClient) Put(ctx context.Context, stream, partition string, data map[string]interface{}) error {
	return nil
}

// Close meets the `Client` interface
func (nc *noopClient) Close(ctx context.Context) error {
	return nil
}
