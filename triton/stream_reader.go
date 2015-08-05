package triton

import (
	"fmt"
	"io"
	"log"
	"sync"

	"github.com/tinylib/msgp/msgp"
)

// A StreamReader is a higher-level interface for reading data from a live Triton stream.
//
// By implementing a Reader interface, we can delivery processed triton data to the client.
// In addition, we provide checkpointing service.
type StreamReader interface {
	Reader
	Checkpoint() error
	Stop()
}

type multiShardStreamReader struct {
	readers   []*ShardStreamReader
	recStream chan map[string]interface{}
	allWg     sync.WaitGroup
	done      chan struct{}
	quit      chan struct{}
}

func (msr *multiShardStreamReader) Checkpoint() (err error) {
	// NOTE: Need to sort out what the last point is, because the record we
	// pull from the underlying stream may never have been delivered to the
	// consumer.
	return fmt.Errorf("not implemented")
}

func (msr *multiShardStreamReader) ReadRecord() (rec map[string]interface{}, err error) {
	select {
	case rec = <-msr.recStream:
		return rec, nil
	case <-msr.done:
		return nil, io.EOF
	}
}

func (msr *multiShardStreamReader) Stop() {
	msr.quit <- struct{}{}
	log.Println("Triggered stop, waiting to complete")
	msr.allWg.Wait()
}

const maxShards int = 100

func NewStreamReader(svc KinesisService, streamName string, c Checkpointer) (sr StreamReader, err error) {
	msr := multiShardStreamReader{
		make([]*ShardStreamReader, 0),
		make(chan map[string]interface{}),
		sync.WaitGroup{},
		make(chan struct{}),
		make(chan struct{}, maxShards),
	}

	shards, err := ListShards(svc, streamName)
	if err != nil {
		return
	}

	if len(shards) == 0 {
		return nil, fmt.Errorf("No shards found")
	}

	sr = &msr

	if len(shards) > maxShards {
		// We reserve some data structures. That's a lot of shards
		panic("Too many shards")
	}

	for _, sid := range shards {
		go func(sid ShardID) {
			shardStream := NewShardStreamReader(svc, streamName, sid)

			msr.allWg.Add(1)
			defer msr.allWg.Done()

			log.Printf("Starting stream processing for %s:%s", streamName, sid)
			processStreamToChan(shardStream, msr.recStream, msr.done)

			msr.quit <- struct{}{}
		}(sid)
	}

	go func() {
		<-msr.quit
		log.Println("Stop triggered, shutdown starting.")

		// Closing the done channel will cause all the worker routines to shutdown.
		// But we can't close a channel more than once, so we'll control access
		// to it via the quit channel.
		close(msr.done)
	}()

	return
}

func processStreamToChan(r *ShardStreamReader, recChan chan map[string]interface{}, done chan struct{}) {
	for {
		select {
		case <-done:
			return
		default:
		}

		log.Println("Get")
		kRec, err := r.Get()
		if err != nil {
			log.Println("Error reading record", err)
			return
		}

		// this indicates there were no more records. Rather than block
		// forever, the ShardStreamReader graciously gives us the opportunity
		// to change our minds.
		if kRec == nil {
			continue
		}

		rec, eb, err := msgp.ReadMapStrIntfBytes(kRec.Data, nil)
		if err != nil {
			// This will end the stream. If this ever happens, we might need
			// some way to repair the stream.
			log.Println("Failed to decode record from stream", err)
			return
		}
		if len(eb) > 0 {
			log.Println("Extra bytes in stream record", len(eb))
			return
		}

		select {
		case recChan <- rec:
		case <-done:
			return
		}
	}
}

// TODO: An interface to choose shards
