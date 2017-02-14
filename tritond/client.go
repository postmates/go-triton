package tritond

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/pebbe/zmq4"
	"github.com/tinylib/msgp/msgp"
)

const (
	// DefaultZMQHWM configures the high water mark for the push socket.
	// More info: http://api.zeromq.org/4-1:zmq-setsockopt#toc39
	DefaultZMQHWM = 4000
)

var ErrClientClosed = errors.New("Client Closed")

// Message defines the data format for tritond
type Message interface {
	Data() map[string]interface{}
	PartitionKey() string
}

// ObjectMessage defines a particular map input with required keys
// `object_type`: type of object (ex. delivery, customer)
// `{object_type}_uuid`: uuid of object (ex. delivery_uuid: "some-uuid")
type ObjectMessage map[string]interface{}

// Data returns the entire message
func (om ObjectMessage) Data() map[string]interface{} { return om }

// PartitionKey returns the `{object_type}_uuid`
func (om ObjectMessage) PartitionKey() string {
	objectType := om["object_type"].(string)
	objectUUID := om[objectType+"_uuid"].(string)
	return objectUUID
}

// Client defines the interface of a tritond client
type Client interface {
	Put(ctx context.Context, stream string, msg Message) error
	Close(ctx context.Context) error
}

// Option defines a function that can be used to configure a client
type Option func(c *zeromqClient) error

// WithHWM sets the high water mark for the zeromq sockets.
func WithHWM(hwm int) Option {
	return Option(func(c *zeromqClient) error {
		c.highWaterMark = hwm
		return nil
	})
}

// WithZMQEndpoint sets the endpoint for zeromq
func WithZMQEndpoint(endpoint string) Option {
	return Option(func(c *zeromqClient) error {
		c.zmqEndpoint = endpoint
		return nil
	})
}

// NewClient creates a Client with the given configuration options
func NewClient(opts ...Option) (Client, error) {
	zmqCtx, err := zmq4.NewContext()
	if err != nil {
		return nil, err
	}

	client := &zeromqClient{
		zmqEndpoint:   "tcp://127.0.0.1:3515",
		highWaterMark: DefaultZMQHWM,
		zmqCtx:        zmqCtx,
		done:          make(chan struct{}),
		sockets:       make(chan *zmq4.Socket, 10),
	}
	for _, opt := range opts {
		opt(client)
	}
	return client, nil
}

type zeromqClient struct {
	zmqEndpoint   string
	highWaterMark int

	zmqCtx  *zmq4.Context
	done    chan struct{}
	sockets chan *zmq4.Socket
	wg      sync.WaitGroup
}

func (c *zeromqClient) Put(ctx context.Context, stream string, msg Message) error {
	s, socketErr := c.getSocket(ctx)
	if socketErr != nil {
		return socketErr
	}
	defer c.putSocket(s)

	header := struct {
		StreamName   string `json:"stream_name"`
		PartitionKey string `json:"partition_key"`
	}{
		StreamName:   stream,
		PartitionKey: msg.PartitionKey(),
	}

	// Form header and body
	headerData, err := json.Marshal(header)
	if err != nil {
		return err
	}

	var buf bytes.Buffer
	w := msgp.NewWriter(&buf)
	if err = w.WriteMapStrIntf(msg.Data()); err != nil {
		return err
	}
	w.Flush()

	_, err = s.SendMessageDontwait(headerData, buf.Bytes())
	return err
}

func (c *zeromqClient) Close(ctx context.Context) error {
	close(c.done)
	close(c.sockets)

	// Close out idle sockets
	for s := range c.sockets {
		s.Close()
	}

	termFinished := make(chan error)
	go func() {
		termFinished <- c.zmqCtx.Term()
	}()

	// Wait for term up until context deadline
	select {
	case err := <-termFinished:
		return err
	case <-ctx.Done():
		return context.DeadlineExceeded
	}
}

func (c *zeromqClient) getSocket(ctx context.Context) (*zmq4.Socket, error) {
	select {
	case <-c.done:
		return nil, ErrClientClosed
	case s, ok := <-c.sockets:
		if !ok {
			return nil, ErrClientClosed
		}
		return s, nil
	default:
		fmt.Println("New Socket")
		// Create a new socket
		s, err := c.zmqCtx.NewSocket(zmq4.PUSH)
		if err != nil {
			return nil, err
		}

		// Configure
		deadline, _ := ctx.Deadline()
		if !deadline.IsZero() {
			s.SetConnectTimeout(deadline.Sub(time.Now()))
		}

		s.SetSndhwm(c.highWaterMark)
		s.SetLinger(3 * time.Second)

		return s, s.Connect(c.zmqEndpoint)
	}
}

func (c *zeromqClient) putSocket(s *zmq4.Socket) {
	select {
	case <-c.done:
		s.Close() // Close this socket
	default:
		// fallthrough
	}

	select {
	case c.sockets <- s:
	default:
		s.Close() // Disgard socket -- over max idle
	}
}
