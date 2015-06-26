package triton

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

type NullKinesisService struct{}

func (s *NullKinesisService) GetShardIterator(*kinesis.GetShardIteratorInput) (*kinesis.GetShardIteratorOutput, error) {
	gso := &kinesis.GetShardIteratorOutput{ShardIterator: aws.String("123")}
	return gso, nil
}

func TestNewStream(t *testing.T) {
	svc := NullKinesisService{}

	s, err := NewStream(&svc, "test-stream", "shard-0001")
	if err != nil {
		t.Errorf("Failed with: %v", err)
		return
	}

	if s.StreamName != "test-stream" {
		t.Errorf("bad stream name")
	}

	if s.ShardID != "shard-0001" {
		t.Errorf("bad ShardID")
	}

	if *s.NextIteratorValue != "123" {
		t.Errorf("bad NextIteratorValue")
	}
}

func TestNewStreamFromSequence(t *testing.T) {
	svc := NullKinesisService{}

	s, err := NewStreamFromSequence(&svc, "test-stream", "shard-0001", "abc123")
	if err != nil {
		t.Errorf("Failed with: %v", err)
		return
	}
	if s.StreamName != "test-stream" {
		t.Errorf("bad stream name")
	}

	if s.ShardID != "shard-0001" {
		t.Errorf("bad ShardID")
	}

	if *s.NextIteratorValue != "123" {
		t.Errorf("bad NextIteratorValue")
	}
	if *s.LastSequenceNumber != "abc123" {
		t.Errorf("bad LastSequenceNumber")
	}
}
