package tritond

import "sync"

// NewMockClient returns a new MockClient
func NewMockClient() *MockClient {
	return &MockClient{
		lock:           new(sync.Mutex),
		PartitionCount: make(map[string]int),
	}
}

// MockClient implements a client that stores the messages in memory
type MockClient struct {
	StreamMessage  map[string]([]Message)
	PartitionCount map[string]int

	lock *sync.Mutex
}

// Put implements the client interface
func (c *MockClient) Put(stream string, msg Message) error {
	c.lock.Lock()
	messages, _ := c.StreamMessage[stream]
	c.StreamMessage[stream] = append(messages, msg)
	c.PartitionCount[msg.PartitionKey()]++
	c.lock.Unlock()
	return nil
}
