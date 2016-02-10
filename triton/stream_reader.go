package triton

import (
	"fmt"
	"io"
	"log"
	"sync"

	"github.com/getsentry/raven-go"
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
	checkpointer Checkpointer
	readers      []*ShardStreamReader
	recStream    chan map[string]interface{}
	allWg        sync.WaitGroup
	done         chan struct{}
	quit         chan struct{}
}

func (msr *multiShardStreamReader) Checkpoint() (err error) {
	for _, r := range msr.readers {
		if r.LastSequenceNumber != nil {
			err = msr.checkpointer.Checkpoint(r.ShardID, *r.LastSequenceNumber)
		}
	}
	return
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
		c,
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
		sn, err := c.LastSequenceNumber(sid)
		if err != nil {
			return nil, err
		}
		var shardStream *ShardStreamReader
		if sn == "" {
			shardStream = NewShardStreamReader(svc, streamName, sid)
		} else {
			shardStream = NewShardStreamReaderFromSequence(svc, streamName, sid, sn)
		}

		msr.readers = append(msr.readers, shardStream)

		go func(shardStream *ShardStreamReader) {
			msr.allWg.Add(1)
			defer msr.allWg.Done()

			log.Printf("Starting stream processing for %s:%s", shardStream.StreamName, shardStream.ShardID)
			processStreamToChan(shardStream, msr.recStream, msr.done)

			msr.quit <- struct{}{}
		}(shardStream)
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

		kRec, err := r.Get()
		if err != nil {
			detailed_error := fmt.Sprintf("Error reading record: %v", err)
			log.Println(detailed_error)
			raven.CaptureError(err,
				map[string]string{
					"stream":        r.StreamName,
					"error_message": detailed_error})
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
			// Log bad data and move on
			detailed_error := fmt.Sprintf("Failed to decode record from stream: %v", err)
			log.Println(detailed_error)
			raven.CaptureError(err,
				map[string]string{
					"stream":        r.StreamName,
					"data":          string(kRec.Data),
					"error_message": detailed_error})
			continue
		}
		if len(eb) > 0 {
			// Log bad data and move on
			detailed_error := fmt.Errorf("Extra bytes in stream record: %d", len(eb))
			log.Println(detailed_error)
			raven.CaptureError(detailed_error,
				map[string]string{
					"stream": r.StreamName,
					"data":   string(kRec.Data)})
			continue
		}

		select {
		case recChan <- rec:
		case <-done:
			return
		}
	}
}

// TODO: An interface to choose shards
