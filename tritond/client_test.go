package tritond

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPut(t *testing.T) {
	n := 10
	dataChan := make(chan map[string]interface{}, n)
	gConsumer.callback = func(stream, partition string, data map[string]interface{}) {
		dataChan <- data
	}
	c, _ := NewClient()

	data := map[string]interface{}{
		"object_type":   "delivery",
		"delivery_uuid": "example-delivery-uuid",
		"ts":            time.Now().Truncate(0),
		"version":       int64(1),
		"data": map[string]interface{}{
			"couriers": []interface{}{"a", "b", "c"},
		},
	}

	for i := 0; i < n; i++ {
		err := c.Put(context.Background(), "delivery", "example-delivery-uuid", data)
		if err != nil {
			fmt.Println(err)
		}
	}
	c.Close(context.Background())

	for i := 0; i < n; i++ {
		recievedData := <-dataChan
		assert.EqualValues(t, data, recievedData)
	}
}

func TestPutConcurrent(t *testing.T) {
	n := 50
	dataChan := make(chan map[string]interface{}, n)
	gConsumer.callback = func(stream, partition string, data map[string]interface{}) {
		dataChan <- data
	}
	c, _ := NewClient(WithNumIdleConns(2))

	data := map[string]interface{}{
		"object_type":   "delivery",
		"delivery_uuid": "example-delivery-uuid",
		"ts":            time.Now().Truncate(0),
		"version":       int64(1),
		"data": map[string]interface{}{
			"couriers": []interface{}{"a", "b", "c"},
		},
	}

	wg := new(sync.WaitGroup)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := c.Put(context.Background(), "delivery", "example-delivery-uuid", data)
			assert.NoError(t, err)
		}()
	}

	wg.Wait()

	for i := 0; i < n; i++ {
		recievedData := <-dataChan
		assert.EqualValues(t, data, recievedData)
	}
	c.Close(context.Background())
}

func TestPutWithBadEndpoint(t *testing.T) {
	c, _ := NewClient()
	c.Close(context.Background())
	err := c.Put(context.Background(), "test", "teset", nil)
	assert.Equal(t, err, ErrClientClosed)
}

func TestPutClosed(t *testing.T) {
	c, _ := NewClient(WithZMQEndpoint("random://random.random"))
	err := c.Put(context.Background(), "test", "teset", nil)
	assert.Error(t, err)
}
