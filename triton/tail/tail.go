package tail

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/postmates/go-triton/triton"
	"io"
	"sync"
	"time"
)

type TailAt struct {
	stream            string
	streamInited      bool
	bucket            string
	at                time.Time
	closed            bool
	records           chan triton.Record
	errors            chan error
	region            string
	kinesisService    triton.KinesisService
	s3Service         triton.S3Service
	PollInterval      time.Duration
	EmptyPollInterval time.Duration
	sync.Mutex
}

// NewTail returns a new tailing stream starting at "at"
func NewTailAt(bucket string, streamName string, at time.Time) (tail *TailAt, err error) {
	tail = &TailAt{
		stream:            streamName,
		at:                at,
		EmptyPollInterval: time.Second * 10,
		PollInterval:      time.Second,
	}
	return
}

func (t *TailAt) Next() (record triton.Record, err error) {
	t.Lock()
	defer t.Unlock()
	go t.initStream()
	select {
	case record = <-t.records:
	case err = <-t.errors:
	}
	return
}

func (t *TailAt) sendArchivedRecords() (lastMetadata *triton.StreamMetadata, err error) {
	archiveRepository := triton.NewS3StoreArchiveRepository(t.s3Service, t.bucket, t.stream)
	aTime := t.at.AddDate(0, 0, -1)
	end := t.at.AddDate(0, 0, 2)
	var lastArchive *triton.StoreArchive
	for !t.closed && aTime.Before(end) {
		var archives []triton.StoreArchive
		archives, err = archiveRepository.ArchivesAtDate(t.at)
		if err != nil {
			return
		}
		for _, archive := range archives {
			if t.closed {
				return
			}
			lastArchive = &archive
			if archive.T.Before(t.at) {
				continue
			}
			for {
				var rec map[string]interface{}
				rec, err = archive.ReadRecord()
				if err == io.EOF {
					err = nil
					break
				} else if err != nil {
					return
				}
				t.records <- rec
			}
		}
		aTime = t.at.AddDate(0, 0, 1)
	}
	if lastArchive != nil {
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
	err = t.sendKinesisRecords(lastStreamMetadata)
	if err != nil {
		t.errors <- err
	}
}

func (t *TailAt) sendKinesisRecords(previousMetadata *triton.StreamMetadata) (err error) {
	shards, err := t.listShards()
	if err != nil {
		return
	}

	// send all of the records in `startingKey`
	// load metadata for starting key
	// then send kinesis records
	// load the sequenceNumbers for the last key
	for _, shard := range shards {
		var lastSequenceNumber triton.SequenceNumber
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
func (t *TailAt) sendKinesisRecordsForShard(shard triton.ShardID, startingSequenceNumber triton.SequenceNumber) {
	// Start reading from shard and send records to t.records
	iterator, err := t.getStreamIterator(shard, startingSequenceNumber)
	if err != nil {
		t.errors <- err
		return
	}
	for !t.closed {
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
			time.Sleep(t.EmptyPollInterval)
		}
	}
}

// getStreamIterator returns an iterator for the stream and shard, optionally starting at startingSequenceNumber or at LATEST
func (t *TailAt) getStreamIterator(shardID triton.ShardID, startingSequenceNumber triton.SequenceNumber) (iteratorID string, err error) {
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
func (t *TailAt) listShards() (result []triton.ShardID, err error) {
	describeStreamOutput, err := t.kinesisService.DescribeStream(&kinesis.DescribeStreamInput{
		StreamName: aws.String(t.stream),
	})
	if err != nil {
		return
	}
	for _, shard := range describeStreamOutput.StreamDescription.Shards {
		result = append(result, triton.ShardID(*shard.ShardId))
	}
	return
}

// Close closes the tail stream
func (t *TailAt) Close() {
	t.Lock()
	defer t.Unlock()
	t.closed = true
}
