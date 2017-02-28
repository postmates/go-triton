package tritond

import (
	"context"
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

	lock *sync.Mutex
}

// Put implements the client interface
func (c *MockClient) Put(ctx context.Context, stream, partition string, data map[string]interface{}) error {
	c.lock.Lock()
	messages, _ := c.StreamData[stream]
	c.StreamData[stream] = append(messages, data)
	c.PartitionCount[partition]++
	c.lock.Unlock()
	return nil
}

// Close is a noop for a mock client. Meets `Client` inteface
func (c *MockClient) Close(ctx context.Context) error {
	return nil
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
