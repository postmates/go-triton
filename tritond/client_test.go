package tritond

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/pebbe/zmq4"
	"github.com/tinylib/msgp/msgp"
)

type consumer struct {
	callback func(stream, partition string, data map[string]interface{})
	close    chan bool
}

func (c *consumer) Start() error {
	s, err := zmq4.NewSocket(zmq4.PULL)
	if err != nil {
		return err
	}

	err = s.Bind("tcp://127.0.0.1:3515")
	go func() {
		for {
			select {
			case <-c.close:
				return
			default:
				if msg, err := s.RecvMessageBytes(0); err == nil {
					header := make(map[string]string)
					if jsonErr := json.Unmarshal(msg[0], &header); jsonErr != nil {
						log.Print(jsonErr)
					}

					body := map[string]interface{}{}
					bodyData := bytes.NewBuffer(msg[1])
					reader := msgp.NewReader(bodyData)
					if msgErr := reader.ReadMapStrIntf(body); msgErr != nil {
						log.Print(msgErr)
					}

					stream, _ := header["stream_name"]
					partition, _ := header["partition_key"]
					c.callback(stream, partition, body)

				} else {
					log.Print(msg, err)
				}
			}
		}
	}()

	return nil
}

func (c *consumer) Stop() error {
	close(c.close)
	return nil
}

func newConsumer(fn func(stream, partition string, data map[string]interface{})) *consumer {
	return &consumer{
		callback: fn,
		close:    make(chan bool),
	}
}

func TestPut(t *testing.T) {
	consumer := newConsumer(func(stream, partition string, data map[string]interface{}) {
		fmt.Println("Consumer", stream, partition, data)
	})
	consumer.Start()
	c, _ := NewClient()

	data := ObjectMessage{
		"object_type":   "delivery",
		"delivery_uuid": "example-delivery-uuid",
		"ts":            time.Now(),
		"version":       1,
		"data": map[string]interface{}{
			"couriers": []string{"a", "b", "c"},
		},
	}

	for i := 0; i < 10; i++ {
		err := c.Put(context.Background(), "delivery", data)
		if err != nil {
			fmt.Println(err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	c.Close(ctx)
	cancel()

	time.Sleep(3 * time.Second)
	consumer.Stop()
}
