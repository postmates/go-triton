package tritond

import (
	"bytes"
	"encoding/json"
	"flag"
	"log"
	"os"
	"testing"

	"sync"

	"github.com/pebbe/zmq4"
	"github.com/tinylib/msgp/msgp"
)

// Define a global zeromq `consumer` that can be used within the tests.
var gConsumer *consumer

// Setup the testing enviroment
func TestMain(m *testing.M) {
	flag.Parse()

	gConsumer = &consumer{
		close: make(chan bool),
	}
	gConsumer.Start()

	// Run tests
	retval := m.Run()

	gConsumer.Stop()

	os.Exit(retval)
}

// Implement a zeromq consumer for testing
type consumer struct {
	callback func(stream, partition string, data map[string]interface{})
	close    chan bool
	socket   *zmq4.Socket
}

func (c *consumer) Start() error {
	wg := new(sync.WaitGroup)
	wg.Add(1)
	var err error

	go func() {

		c.socket, err = zmq4.NewSocket(zmq4.PULL)
		if err != nil {
			return
		}

		if err = c.socket.Bind("tcp://127.0.0.1:3515"); err != nil {
			panic(err)
		}
		wg.Done()
		for {
			select {
			case <-c.close:
				c.socket.Close()

				return
			default:
				if msg, err := c.socket.RecvMessageBytes(0); err == nil {
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
	wg.Wait()
	if err != nil {
		return err
	}
	return nil
}

func (c *consumer) Stop() {
	close(c.close)
}
