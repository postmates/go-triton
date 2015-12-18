package triton

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/tinylib/msgp/msgp"
	"io"
	"log"
	"sync"
	"time"
)

type TailAt struct {
	stream            string
	streamInited      bool
	bucket            string
	client            string
	at                time.Time
	closed            bool
	records           chan Record
	errors            chan error
	region            string
	kinesisService    KinesisService
	s3Service         S3Service
	pollInterval      time.Duration
	emptyPollInterval time.Duration
	sync.Mutex
}

// NewTailAtParams are the parameters for invoking NewTailAt
type NewTailAtParams struct {
	S3Service         S3Service
	KinesisService    KinesisService
	StreamName        string
	Bucket            string
	Client            string
	At                time.Time
	PollInterval      time.Duration
	EmptyPollInterval time.Duration
}

const (
	// The amount of time to poll for new kinesis records if we encounter an empty stream
	DefaultEmptyPollInterval = 10 * time.Second
	DefaultPollInterval      = 1 * time.Millisecond
)

// NewTail returns a new tailing stream starting at "at"
func NewTailAt(params *NewTailAtParams) (tail *TailAt) {
	if params.EmptyPollInterval == 0 {
		params.EmptyPollInterval = DefaultEmptyPollInterval
	}

	if params.PollInterval == 0 {
		params.PollInterval = DefaultPollInterval
	}

	tail = &TailAt{
		at:                params.At,
		emptyPollInterval: params.EmptyPollInterval,
		kinesisService:    params.KinesisService,
		pollInterval:      params.PollInterval,
		s3Service:         params.S3Service,
		stream:            params.StreamName,
		bucket:            params.Bucket,
		client:            params.Client,
		records:           make(chan Record),
		errors:            make(chan error),
	}
	return
}

func (t *TailAt) Next() (record Record, err error) {
	t.Lock()
	defer t.Unlock()
	go t.initStream()
	select {
	case record = <-t.records:
	case err = <-t.errors:
	}
	return
}

func (t *TailAt) sendArchivedRecords() (lastMetadata *StreamMetadata, err error) {
	log.Println("sending archived records")
	archiveRepository := NewArchiveRepository(t.s3Service, nil, t.bucket, t.stream, t.client)
	aTime := t.at.AddDate(0, 0, -1)
	end := t.at.AddDate(0, 0, 2)
	var lastArchive *StoreArchive
	for aTime.Before(end) {
		if t.closed {
			break
		}
		if !aTime.Before(end) {
			break
		}
		log.Println("checking", aTime)
		var archives []StoreArchive
		archives, err = archiveRepository.ArchivesAtDate(aTime)
		if err != nil {
			return
		}
		log.Println("archives: ", archives)
		for _, archive := range archives {
			if t.closed {
				return
			}
			lastArchive = &archive
			log.Println("archive time:", archive.T, "tail time:", t.at)
			if archive.T.Before(t.at) {
				log.Println("archive is before target")
				continue
			}
			for {
				log.Println("reading records from archive")
				var rec map[string]interface{}
				rec, err = archive.ReadRecord()
				if err == io.EOF {
					err = nil
					break
				} else if err != nil {
					return
				}
				log.Println("record", rec)
				t.records <- rec
			}
		}
		aTime = aTime.AddDate(0, 0, 1)
	}
	log.Println("done sending archives")
	if lastArchive != nil {
		log.Println("loading metadata")
		lastMetadata, err = lastArchive.GetStreamMetadata()
	}
	return
}

func (t *TailAt) initStream() {
	if t.streamInited {
		return
	}
	t.streamInited = true

	lastStreamMetadata, err := t.sendArchivedRecords()
	if err != nil {
		t.errors <- err
		return
	}
	log.Println("done sending archived records")
	err = t.sendKinesisRecords(lastStreamMetadata)
	if err != nil {
		t.errors <- err
	}
}

func (t *TailAt) sendKinesisRecords(previousMetadata *StreamMetadata) (err error) {
	shards, err := t.listShards()
	if err != nil {
		return
	}

	// send all of the records in `startingKey`
	// load metadata for starting key
	// then send kinesis records
	// load the sequenceNumbers for the last key
	for _, shard := range shards {
		var lastSequenceNumber SequenceNumber
		if previousMetadata != nil && previousMetadata.Shards[shard] != nil {
			lastSequenceNumber = previousMetadata.Shards[shard].MaxSequenceNumber
		}
		go t.sendKinesisRecordsForShard(shard, lastSequenceNumber)
	}
	return
}

// sendKinesisRecordsForShard starts sending records to TailAt.records for the shard optionally starting at starting startingSequenceNumber
//
// If a startingSequenceNumber is specified the reader tries to start reading
// records at startingSequenceNumber, otherwise it tries to find a starting
// sequence number at TRIM_HORIZON to begin reading.
func (t *TailAt) sendKinesisRecordsForShard(shard ShardID, startingSequenceNumber SequenceNumber) {
	log.Println("sendKinesisRecordsForShard", shard, startingSequenceNumber)

	// Start reading from shard and send records to t.records
	iterator, err := t.getStreamIterator(shard, startingSequenceNumber)
	if err != nil {
		t.errors <- err
		return
	}
	for {
		if t.closed {
			break
		}
		var getRecordsInput kinesis.GetRecordsInput
		getRecordsInput.ShardIterator = aws.String(iterator)
		var getRecordsOutput *kinesis.GetRecordsOutput
		getRecordsOutput, err = t.kinesisService.GetRecords(&getRecordsInput)
		if err != nil {
			// catch rate limiting errors
			t.errors <- err
			return
		}
		iterator = *getRecordsOutput.NextShardIterator
		if len(getRecordsOutput.Records) == 0 {
			time.Sleep(t.emptyPollInterval)
		} else {
			for _, kinesisRecord := range getRecordsOutput.Records {
				rec, _, err := msgp.ReadMapStrIntfBytes(kinesisRecord.Data, nil)
				if err != nil {
					t.errors <- fmt.Errorf("unexpected error decoding record (%s): %s", shard, err.Error())
					return
				}
				t.records <- rec
			}
		}
	}
}

// getStreamIterator returns an iterator for the stream and shard, optionally starting at startingSequenceNumber or at TrimHorizon
func (t *TailAt) getStreamIterator(shardID ShardID, startingSequenceNumber SequenceNumber) (iteratorID string, err error) {
	// Handle the case where startingSequenceNumber is invalid
	var getShardIteratorInput kinesis.GetShardIteratorInput
	getShardIteratorInput.ShardId = aws.String(string(shardID))
	getShardIteratorInput.StreamName = aws.String(t.stream)
	if startingSequenceNumber != "" {
		getShardIteratorInput.StartingSequenceNumber = aws.String(string(startingSequenceNumber))
		getShardIteratorInput.ShardIteratorType = aws.String(kinesis.ShardIteratorTypeAtSequenceNumber)
	} else {
		getShardIteratorInput.ShardIteratorType = aws.String(kinesis.ShardIteratorTypeTrimHorizon)
	}
	getShardIteratorOutput, err := t.kinesisService.GetShardIterator(&getShardIteratorInput)
	if err != nil {
		return
	}
	iteratorID = *getShardIteratorOutput.ShardIterator
	return
}

// listShardsForStream helper method to list all the shards for a stream
func (t *TailAt) listShards() (result []ShardID, err error) {
	describeStreamOutput, err := t.kinesisService.DescribeStream(&kinesis.DescribeStreamInput{
		StreamName: aws.String(t.stream),
	})
	if err != nil {
		return
	}
	for _, shard := range describeStreamOutput.StreamDescription.Shards {
		result = append(result, ShardID(*shard.ShardId))
	}
	return
}

// Close closes the tail stream
func (t *TailAt) Close() {
	t.Lock()
	defer t.Unlock()
	t.closed = true
}
