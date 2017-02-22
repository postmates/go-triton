package tritond

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/pebbe/zmq4"
	"github.com/tinylib/msgp/msgp"
)

const (
	// DefaultZMQHWM configures the high water mark for the push socket.
	// More info: http://api.zeromq.org/4-1:zmq-setsockopt#toc39
	DefaultZMQHWM = 4000

	// DefaultNumIdleConns configures the number of idle sockets to maintain for zmq.
	DefaultNumIdleConns = 10
)

// ErrClientClosed indicates that the client has been closed and is not longer usable.
var ErrClientClosed = errors.New("Client Closed")

// Client defines the interface of a tritond client.
type Client interface {
	// Put sends `data` to a given partition of a stream asynchronously.
	Put(ctx context.Context, stream, partition string, data map[string]interface{}) error

	// Close flushes all in-flight `Put`s and blocks new `Put`s, effectively closing the client.
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

// WithNumIdleConns sets the maximum number of idle zmq sockets
func WithNumIdleConns(numIdle int) Option {
	return Option(func(c *zeromqClient) error {
		c.numIdleConn = numIdle
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
		numIdleConn:   DefaultNumIdleConns,
		zmqEndpoint:   "tcp://127.0.0.1:3515",
		highWaterMark: DefaultZMQHWM,
		zmqCtx:        zmqCtx,
		done:          make(chan struct{}),
	}
	for _, opt := range opts {
		opt(client)
	}
	client.sockets = make(chan *zmq4.Socket, client.numIdleConn)

	return client, nil
}

type zeromqClient struct {
	zmqEndpoint   string
	highWaterMark int
	numIdleConn   int

	zmqCtx  *zmq4.Context
	done    chan struct{}
	sockets chan *zmq4.Socket
	wg      sync.WaitGroup
}

// Put writes `data` to a stream partition using a zeromq socket to the TritonD daemon.
//
// 	This function is intended to be both non-blocking and thread-safe. As ZeroMQ sockets
// 	are inherently serial, they are neither so this function will create a new socket
// 	whenever there is not an idle one avalilable. A client stores a configurable number
// 	of idle connections that should allow this function to be very quick in the average case.
func (c *zeromqClient) Put(ctx context.Context, stream, partition string,
	data map[string]interface{}) error {

	// Get socket from pool
	s, socketErr := c.getSocket(ctx)
	if socketErr != nil {
		return socketErr
	}
	defer c.putSocket(s)

	// Form header
	header := struct {
		StreamName   string `json:"stream_name"`
		PartitionKey string `json:"partition_key"`
	}{
		StreamName:   stream,
		PartitionKey: partition,
	}
	headerData, err := json.Marshal(header)
	if err != nil {
		return err
	}

	// Form body
	var body bytes.Buffer
	w := msgp.NewWriter(&body)
	if err = w.WriteMapStrIntf(data); err != nil {
		return err
	}
	w.Flush()

	_, err = s.SendMessageDontwait(headerData, body.Bytes())
	return err
}

// Close attempts to gracefully shutdown a client by both
//	preventing new `Put`s and waiting for in-flight `Put`s
//	terminate.
func (c *zeromqClient) Close(ctx context.Context) error {
	// * Prevent new sockets (getSocket)
	// * Prevent returning in-flight sockets (putSocket)
	close(c.done)
	close(c.sockets) // cap idle (since we have already prevented writes)

	// Close out idle sockets
	for s := range c.sockets {
		s.Close()
	}

	// Try and terminate the zeromq context (wait for in-flight sockets)
	termFinished := make(chan error)
	go func() {
		termFinished <- c.zmqCtx.Term()
	}()

	// Wait for term up until context deadline
	//	- in-flight connection may take forever to finish.
	//		Caller can decide how long they are willing to wait.
	select {
	case err := <-termFinished:
		return err
	case <-ctx.Done():
		return context.DeadlineExceeded
	}
}

//
// Socket pool
//

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
	case c.sockets <- s: // Attempt to reuse socket
	default:
		s.Close() // Disgard socket -- over max idle
	}
}
