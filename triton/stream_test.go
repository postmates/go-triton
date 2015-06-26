package triton

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

type NullKinesisService struct{}

func (s *NullKinesisService) GetShardIterator(*kinesis.GetShardIteratorInput) (*kinesis.GetShardIteratorOutput, error) {
	gso := &kinesis.GetShardIteratorOutput{ShardIterator: aws.String("123")}
	return gso, nil
}

func (s *NullKinesisService) GetRecords(*kinesis.GetRecordsInput) (*kinesis.GetRecordsOutput, error) {
	rec := &kinesis.Record{}
	records := []*kinesis.Record{rec}
	gso := &kinesis.GetRecordsOutput{
		NextShardIterator:  aws.String("124"),
		MillisBehindLatest: aws.Long(0),
		Records:            records,
	}
	return gso, nil
}

func TestNewStream(t *testing.T) {
	svc := NullKinesisService{}

	s := NewStream(&svc, "test-stream", "shard-0001")
	if s.StreamName != "test-stream" {
		t.Errorf("bad stream name")
	}

	if s.ShardID != "shard-0001" {
		t.Errorf("bad ShardID")
	}

	if s.NextIteratorValue != nil {
		t.Errorf("bad NextIteratorValue")
	}
}

func TestNewStreamFromSequence(t *testing.T) {
	svc := NullKinesisService{}

	s := NewStreamFromSequence(&svc, "test-stream", "shard-0001", "abc123")
	if s.StreamName != "test-stream" {
		t.Errorf("bad stream name")
	}

	if s.ShardID != "shard-0001" {
		t.Errorf("bad ShardID")
	}

	if s.NextIteratorValue != nil {
		t.Errorf("bad NextIteratorValue")
	}

	if *s.LastSequenceNumber != "abc123" {
		t.Errorf("bad LastSequenceNumber")
	}
}

func TestStreamWait(t *testing.T) {
	svc := NullKinesisService{}
	s := NewStream(&svc, "test-stream", "shard-0000")

	n := time.Now()
	s.wait(100 * time.Millisecond)
	if time.Since(n).Seconds() > 0.050 {
		t.Errorf("Shouldn't have waited")
	}

	s.wait(100 * time.Millisecond)
	if time.Since(n).Seconds() < 0.050 {
		t.Errorf("Should have waited")
	}

}

func TestFetchMoreRecords(t *testing.T) {
	svc := NullKinesisService{}
	s := NewStream(&svc, "test-stream", "shard-0000")

	err := s.fetchMoreRecords()
	if err != nil {
		t.Errorf("Received error %v", err)
		return
	}

	if len(s.records) != 1 {
		t.Errorf("Should have a record")
	}
}

func TestRead(t *testing.T) {
	svc := NullKinesisService{}
	s := NewStream(&svc, "test-stream", "shard-0000")

	r, err := s.Read()
	if err != nil {
		t.Errorf("Received error %v", err)
		return
	}

	if r == nil {
		t.Errorf("Should be a record")
	}
}
