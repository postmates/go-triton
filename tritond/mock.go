package tritond

import (
	"context"
	"runtime"
	"sync"
)

// NewMockClient returns a new MockClient
func NewMockClient() *MockClient {
	return &MockClient{
		lock:           new(sync.Mutex),
		PartitionCount: make(map[string]int),
		StreamData:     make(map[string]([](map[string]interface{}))),
	}
}

// MockClient implements a client that stores the messages in memory
type MockClient struct {
	StreamData     map[string]([](map[string]interface{}))
	PartitionCount map[string]int
	lock           *sync.Mutex
}

// Put implements the client interface
func (c *MockClient) Put(ctx context.Context, stream, partition string, data map[string]interface{}) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	messages, _ := c.StreamData[stream]
	c.StreamData[stream] = append(messages, data)
	c.PartitionCount[partition]++

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
}

func (c *MockClient) unlock() {
	c.lock.Unlock()
}

// PrepareForAssert prepares MockClient to be asserted when it was used in async code.
// Test code *must* defer on result of this method
func (c *MockClient) PrepareForAssert() func() {
	runtime.Gosched()
	c.lock.Lock()
	return c.unlock
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
