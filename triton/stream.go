package triton

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	//"github.com/aws/aws-sdk-go/aws/awsutil"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

type Stream struct {
	StreamName         string
	ShardID            string
	ShardIteratorType  string
	NextIteratorValue  *string
	LastSequenceNumber *string

	service     KinesisService
	records     []*kinesis.Record
	lastRequest *time.Time
}

const MIN_POLL_INTERVAL = 1.0 * time.Second

func (s *Stream) initIterator() (err error) {
	gsi := kinesis.GetShardIteratorInput{
		StreamName:        aws.String(s.StreamName),
		ShardID:           aws.String(s.ShardID),
		ShardIteratorType: aws.String(s.ShardIteratorType),
	}

	gso, err := s.service.GetShardIterator(&gsi)
	if err != nil {
		return err
	}

	s.NextIteratorValue = gso.ShardIterator
	return nil
}

func (s *Stream) wait(minInterval time.Duration) {
	if s.lastRequest != nil {
		sleepTime := minInterval - time.Since(*s.lastRequest)
		if sleepTime >= time.Duration(0) {
			time.Sleep(sleepTime)
		}
	}

	n := time.Now()
	s.lastRequest = &n
}

func (s *Stream) fetchMoreRecords() (err error) {
	s.wait(MIN_POLL_INTERVAL)

	if s.NextIteratorValue == nil {
		err := s.initIterator()
		if err != nil {
			return err
		}
	}

	gri := &kinesis.GetRecordsInput{
		Limit:         aws.Long(1000),
		ShardIterator: s.NextIteratorValue,
	}

	gro, err := s.service.GetRecords(gri)
	if err != nil {
		return err
	}

	s.records = gro.Records
	s.NextIteratorValue = gro.NextShardIterator

	return nil
}

func (s *Stream) Read() (r *kinesis.Record, err error) {
	if len(s.records) == 0 {
		err := s.fetchMoreRecords()
		if err != nil {
			return nil, err
		}
	}

	if len(s.records) > 0 {
		r := s.records[0]
		s.records = s.records[1:]
		return r, nil
	} else {
		return nil, nil
	}
}

func NewStreamFromSequence(svc KinesisService, streamName string, shardId string, sequenceNumber string) (s *Stream) {
	s = &Stream{
		StreamName:         streamName,
		ShardID:            shardId,
		ShardIteratorType:  "AFTER_SEQUENCE_NUMBER",
		LastSequenceNumber: aws.String(sequenceNumber),
		service:            svc,
	}

	return s
}

func NewStream(svc KinesisService, streamName string, shardId string) (s *Stream) {
	s = &Stream{
		StreamName:        streamName,
		ShardID:           shardId,
		ShardIteratorType: "LATEST",
		service:           svc,
	}

	return s
}

func OpenStream(sc *StreamConfig, shardNum int) (s *Stream, err error) {
	svc := kinesis.New(&aws.Config{Region: sc.RegionName})

	resp, err := svc.DescribeStream(&kinesis.DescribeStreamInput{StreamName: aws.String(sc.StreamName)})
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == "ResourceNotFoundException" {
				return nil, fmt.Errorf("Failed to find stream %v", awsErr.Message())
			}
		}
		return nil, err
	}

	if len(resp.StreamDescription.Shards) < shardNum {
		err = fmt.Errorf("Stream doesn't have a shard %d", shardNum)
		return nil, err
	}

	shardID := *resp.StreamDescription.Shards[shardNum].ShardID

	s = NewStream(svc, sc.StreamName, shardID)

	return s, nil
}
