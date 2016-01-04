package triton

// Module for a record reader which consumes from a streamf ro a each shard

import (
	"io"
	"sync"
	"time"
)

// NewMultiShardReaderParams are the parameters to NewMultiShardReader
type NewMultiShardReaderParams struct {
	KinesisService        KinesisService        // The Kinesis service
	Stream                string                // The stream name like "courier_activity"
	ShardToSequenceNumber ShardToSequenceNumber // A mapping from shard ID to sequence number, to start from a shard after a sequence number
	ReloadShardsInterval  time.Duration         // Reload the list of shards
	// and iterators this interval to handle the case of shards changing while
	// reading records.
}

const defaultReloadShardsInterval = time.Minute * 5 // By default for new shards every five minutes

// NewMultiShardReader creates a new MultiShardReader
func NewMultiShardReader(params *NewMultiShardReaderParams) (result *MultiShardReader) {
	// Passing in a null kinesis service is a programming error
	if params.KinesisService == nil {
		panic("expecting a KinesisService")
	}

	// Not specifying a stream is a programming error
	if params.Stream == "" {
		panic("expecting a stream")
	}

	if params.ReloadShardsInterval == 0 {
		params.ReloadShardsInterval = defaultReloadShardsInterval
	}

	var shardToSequenceNumber ShardToSequenceNumber
	if params.ShardToSequenceNumber != nil {
		shardToSequenceNumber = params.ShardToSequenceNumber
	} else {
		shardToSequenceNumber = make(ShardToSequenceNumber)
	}

	result = &MultiShardReader{
		records:               make(chan *ShardRecord),
		closeCh:               make(chan bool, 1),
		shardToSequenceNumber: shardToSequenceNumber,
		kinesisService:        params.KinesisService,
		stream:                params.Stream,
		errors:                make(chan error),
		resetTimer:            time.NewTicker(params.ReloadShardsInterval),
	}
	go result.resetShardReaders()
	return
}

// MultiShardReader looks up the available shards for a stream in Kinesis and reads from them.  Records are available from ReadShardRecord()
type MultiShardReader struct {
	inited                bool
	readers               []*shardReader
	records               chan *ShardRecord
	closeCh               chan bool
	mutex                 sync.Mutex
	closed                bool
	shardToSequenceNumber ShardToSequenceNumber
	kinesisService        KinesisService
	stream                string
	errors                chan error
	disconnects           []chan bool
	resetTimer            *time.Ticker
}

// Stop stops the reader
func (msr *MultiShardReader) Stop() {
	msr.mutex.Lock()
	defer msr.mutex.Unlock()
	msr.closed = true
	msr.closeCh <- true
}

// Reset and disconnect child shard readers
func (msr *MultiShardReader) resetShardReaders() {
	msr.mutex.Lock()
	defer msr.mutex.Unlock()
	// Send each reader the close signal so that it stops trying to write
	for _, sr := range msr.readers {
		sr.close <- true
	}
	// Reset the slice of readers
	msr.readers = nil

	// Disconnect the shard readers channels from our channels
	for _, disconnect := range msr.disconnects {
		disconnect <- true
	}
	msr.disconnects = nil

	// Start new shard readers:
	shardIDs, err := ListShards(msr.kinesisService, msr.stream)
	if err != nil {
		msr.errors <- err
		return
	}
	for _, shardID := range shardIDs {
		// Create a new shard reader
		sr := newShardReader(&newShardReaderParams{
			stream:                   msr.stream,
			shardID:                  shardID,
			startAfterSequenceNumber: msr.shardToSequenceNumber[shardID],
			kinesisService:           msr.kinesisService,
		})
		// Connect the shard reader channels to our channels
		msr.disconnects = append(msr.disconnects,
			connectShardRecordChannel(msr.records, sr.records),
			connectErrorChannel(msr.errors, sr.errors))
	}
	return
}

// ReadShardRecord returns a ShardRecord or an error if an record was unable to be decoded or if there was an error from Kinesis
func (msr *MultiShardReader) ReadShardRecord() (result *ShardRecord, err error) {
	if msr.closed {
		// Return an EOF when we are closed
		err = io.EOF
		return
	}
	for {
		select {

		case <-msr.resetTimer.C:
			// Whenever resetTimer fires, reset the shard readers.  This allows for
			// the case where the list of shards changes due to merges/splits
			msr.resetShardReaders()
		case err = <-msr.errors:
			// Return an error
			return
		case result = <-msr.records:
			// Keep track of shardID => sequence #
			msr.shardToSequenceNumber[result.ShardID] = result.SequenceNumber
			return
		// If we're closed, quit
		case <-msr.closeCh:
			err = io.EOF
			return
		}
	}
	return
}

// connectShardRecordChannel connects a chan *ShardRecord to another,
// piping each record from src to dst.
//
// Returns a channel which removes the connection on receiving any bool value
func connectShardRecordChannel(dst chan *ShardRecord, src chan *ShardRecord) (stop chan bool) {
	stop = make(chan bool, 1)
	go func() {
		for {
			var rec *ShardRecord
			select {
			case rec = <-src:
			case <-stop:
				return
			}
			select {
			case dst <- rec:
			case <-stop:
				return
			}
		}
	}()
	return
}

// connectErrorChannel connects a chan error to another,
// piping each record from src to dst.
//
// Returns a channel which removes the connection on receiving any bool value
func connectErrorChannel(dst chan error, src chan error) (stop chan bool) {
	stop = make(chan bool, 1)
	go func() {
		for {
			var err error
			select {
			case err = <-src:
			case <-stop:
				return
			}
			select {
			case dst <- err:
			case <-stop:
				return
			}
		}
	}()
	return
}
