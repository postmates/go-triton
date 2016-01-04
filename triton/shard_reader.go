package triton

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/tinylib/msgp/msgp"
	"log"
	"time"
)

const (
	// Read the latest records
	shardIteratorTypeLatest = "LATEST"
	// Read the records after this sequence number
	shardIteratorTypeAfterSequenceNumber = "AFTER_SEQUENCE_NUMBER"
	// The small interval to poll shards
	minShardPollInterval = time.Second
	// The number of records to process at once
	recordLimit = 10000
)

type ShardReader interface {
	ReadShardRecord() (rec *ShardRecord, err error)
	Stop()
}

// newShardReaderParams are params for starting and returning a new shardReader
type newShardReaderParams struct {
	stream                   string
	shardID                  ShardID
	startAfterSequenceNumber SequenceNumber
	kinesisService           KinesisService
}

// newShardReader creates a new shardReader struct and starts the loop
func newShardReader(params *newShardReaderParams) (sr *shardReader) {
	sr = &shardReader{
		newShardReaderParams: *params,
		records:              make(chan *ShardRecord),
		close:                make(chan bool, 1),
	}
	go sr.loop()
	return
}

// shardRecord is a struct for the shardReader state
type shardReader struct {
	newShardReaderParams
	nextIterator string
	records      chan *ShardRecord
	errors       chan error
	close        chan bool
	closed       bool
}

// loop runs the shard reader loop
func (sr *shardReader) loop() {
	defer close(sr.records)                        // After we exit close the records channel
	ticker := time.NewTicker(minShardPollInterval) // Create a ticker so that we only read records from the API every minShardPollInterval
	defer ticker.Stop()                            // Close the ticker channel after we exit
	kinesisRecords, err := sr.readKinesisRecords() // Read a slice of shardRecord
	for !sr.closed {
		// Empty shardRecs case:
		if len(kinesisRecords) == 0 {
			select {
			case <-ticker.C: // Wait for the timer
			case <-sr.close: // Return if we received a close signal
				return
			}
			// Get some more shard records:
			kinesisRecords, err = sr.readKinesisRecords()
			if err != nil {
				sr.errors <- err
			}
		}
		// If we didn't get any shard records, try again later
		if len(kinesisRecords) == 0 {
			continue
		}
		shardRecord, err := sr.convertKinesisRecordToShardRecord(kinesisRecords[0])
		kinesisRecords = kinesisRecords[1:]
		if err != nil {
			sr.errors <- err
		}
		select {
		case sr.records <- shardRecord: // try to send a record
		case <-sr.close: // OR try to close
			return
		}
	}
}

func (sr *shardReader) convertKinesisRecordToShardRecord(kr *kinesis.Record) (result *ShardRecord, err error) {
	result = &ShardRecord{}
	rec, _, err := msgp.ReadMapStrIntfBytes(kr.Data, nil)
	if err != nil {
		return
	}
	result.Record = Record(rec)
	result.SequenceNumber = SequenceNumber(*kr.SequenceNumber)
	result.ShardID = ShardID(sr.shardID)
	return
}

// initShardIterator initializes the nextIterator field with a valid iterator
// value or returns an error
func (sr *shardReader) initShardIterator() (err error) {
	// If we already have a nextIterator return
	if sr.nextIterator != "" {
		return
	}

	gsi := &kinesis.GetShardIteratorInput{}
	// shardIteratorType is AFTER_SEQUENCE_NUMBER if startAfterSequenceNumber is
	// passed in as a parameter, otherwise LATEST is used
	shardIteratorType := shardIteratorTypeLatest
	if sr.startAfterSequenceNumber != "" {
		shardIteratorType = shardIteratorTypeAfterSequenceNumber
		gsi.StartingSequenceNumber = aws.String(string(sr.startAfterSequenceNumber))
	}
	gsi.StreamName = aws.String(sr.stream)
	gsi.ShardId = aws.String(string(sr.shardID))
	gsi.ShardIteratorType = aws.String(shardIteratorType)
	gso, err := sr.kinesisService.GetShardIterator(gsi)
	// Handle GetShardIterator errors:
	if err != nil {
		err = fmt.Errorf("error initializing shard iterator for stream: %q, shard: %q, error: %s", sr.stream, sr.shardID, err.Error())
		return
	}
	sr.nextIterator = *gso.ShardIterator
	return
}

// readKinesisRecords loads records from the shard using the latest shardIterator
func (s *shardReader) readKinesisRecords() (records []*kinesis.Record, err error) {
	// Initialize nextIterator if necessary:
	if s.nextIterator == "" {
		err = s.initShardIterator()
		if err != nil {
			log.Println("failed to initialize iterator:", err.Error())
			return
		}
	}

	// Load the records:
	gro, err := s.kinesisService.GetRecords(&kinesis.GetRecordsInput{
		Limit:         aws.Int64(recordLimit),
		ShardIterator: aws.String(s.nextIterator),
	})
	if err != nil {
		log.Println("failed to get records:", err.Error())
		return
	}
	records = gro.Records

	// Update nextIterator if available
	if gro.NextShardIterator != nil {
		s.nextIterator = *gro.NextShardIterator
	} else {
		s.nextIterator = ""
		s.startAfterSequenceNumber = ""
	}
	return
}
