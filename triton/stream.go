package triton

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

type Stream struct {
	StreamName         string
	ShardID            string
	NextIteratorValue  *string
	LastSequenceNumber *string
}

func createShardLatestIterator(svc KinesisService, streamName string, shardID string) (ShardIterator *string, err error) {
	gsi := kinesis.GetShardIteratorInput{
		ShardID:           aws.String(shardID),
		ShardIteratorType: aws.String("LATEST"),
		StreamName:        aws.String(streamName),
	}

	gso, err := svc.GetShardIterator(&gsi)
	if err != nil {
		return nil, err
	}

	return gso.ShardIterator, nil
}

func createShardAfterSequenceIterator(svc KinesisService, streamName string, shardID string, sequenceNumber string) (ShardIterator *string, err error) {

	gsi := kinesis.GetShardIteratorInput{
		ShardID:           aws.String(shardID),
		ShardIteratorType: aws.String("AFTER_SEQUENCE_NUMBER"),
		StreamName:        aws.String(streamName),
	}

	gso, err := svc.GetShardIterator(&gsi)
	if err != nil {
		return nil, err
	}

	return gso.ShardIterator, nil
}

func NewStreamFromSequence(svc KinesisService, streamName string, shardId string, sequenceNumber string) (s *Stream, err error) {
	i, err := createShardAfterSequenceIterator(svc, streamName, shardId, sequenceNumber)
	if err != nil {
		return nil, err
	}

	s = &Stream{
		StreamName:         streamName,
		ShardID:            shardId,
		NextIteratorValue:  i,
		LastSequenceNumber: &sequenceNumber,
	}

	return s, nil
}

func NewStream(svc KinesisService, streamName string, shardId string) (s *Stream, err error) {
	i, err := createShardLatestIterator(svc, streamName, shardId)
	if err != nil {
		return nil, err
	}

	s = &Stream{
		StreamName:        streamName,
		ShardID:           shardId,
		NextIteratorValue: i,
	}

	return s, nil
}
