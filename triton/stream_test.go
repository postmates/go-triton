package triton

import (
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

type NullKinesisService struct{}

func (s *NullKinesisService) GetShardIterator(*kinesis.GetShardIteratorInput) (*kinesis.GetShardIteratorOutput, error) {
	gso := &kinesis.GetShardIteratorOutput{ShardIterator: aws.String("123")}
	return gso, nil
}

func (s *NullKinesisService) GetRecords(*kinesis.GetRecordsInput) (*kinesis.GetRecordsOutput, error) {
	records := []*kinesis.Record{}
	gso := &kinesis.GetRecordsOutput{
		NextShardIterator:  aws.String("124"),
		MillisBehindLatest: aws.Int64(0),
		Records:            records,
	}
	return gso, nil
}

func (s *NullKinesisService) DescribeStream(input *kinesis.DescribeStreamInput) (*kinesis.DescribeStreamOutput, error) {
	return nil, fmt.Errorf("Not Implemented")
}

func (s *NullKinesisService) PutRecords(input *kinesis.PutRecordsInput) (*kinesis.PutRecordsOutput, error) {
	results := []*kinesis.PutRecordsResultEntry{}
	gso := &kinesis.PutRecordsOutput{
		Records:           results,
		FailedRecordCount: aws.Int64(0),
	}
	return gso, nil
}

type FailingKinesisService struct{}

func (s *FailingKinesisService) DescribeStream(input *kinesis.DescribeStreamInput) (*kinesis.DescribeStreamOutput, error) {
	return nil, fmt.Errorf("Not Implemented")
}

func (s *FailingKinesisService) GetRecords(*kinesis.GetRecordsInput) (*kinesis.GetRecordsOutput, error) {
	err := awserr.New("ProvisionedThroughputExceededException", "slow down dummy", fmt.Errorf("error"))
	return nil, err
}

func (s *FailingKinesisService) GetShardIterator(*kinesis.GetShardIteratorInput) (*kinesis.GetShardIteratorOutput, error) {
	gso := &kinesis.GetShardIteratorOutput{ShardIterator: aws.String("123")}
	return gso, nil
}

func (s *FailingKinesisService) PutRecords(input *kinesis.PutRecordsInput) (*kinesis.PutRecordsOutput, error) {
	err := awserr.New("ProvisionedThroughputExceededException", "slow down dummy", fmt.Errorf("error"))
	return nil, err
}

func TestNewShardStreamReader(t *testing.T) {
	svc := NullKinesisService{}

	s := NewShardStreamReader(&svc, "test-stream", "shard-0001")
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

func TestNewShardStreamReaderFromSequence(t *testing.T) {
	svc := NullKinesisService{}

	s := NewShardStreamReaderFromSequence(&svc, "test-stream", "shard-0001", "abc123")
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
	s := NewShardStreamReader(&svc, "test-stream", "shard-0000")

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
	svc := newTestKinesisService()
	st := newTestKinesisStream("test-stream")
	s1 := newTestKinesisShard()

	r1 := make(Record)
	s1.AddRecord("a", r1)
	st.AddShard("shard-0000", s1)
	svc.AddStream(st)

	s := NewShardStreamReader(svc, "test-stream", "shard-0000")

	err := s.fetchMoreRecords()
	if err != nil {
		t.Errorf("Received error %v", err)
		return
	}

	if len(s.records) != 1 {
		t.Errorf("Should have a record")
	}
}

func TestRetryShardStreamReader(t *testing.T) {
	svc := FailingKinesisService{}

	s := NewShardStreamReader(&svc, "test-stream", "shard-0001")

	err := s.fetchMoreRecords()
	if err != nil {
		t.Errorf("Received error %v", err)
		return
	}

	if len(s.records) != 0 {
		t.Errorf("Should have no records")
	}

	if s.retries != 1 {
		t.Errorf("Should have a retry")
	}
}

func TestRead(t *testing.T) {
	svc := newTestKinesisService()
	st := newTestKinesisStream("test-stream")
	s1 := newTestKinesisShard()

	r1 := make(Record)
	s1.AddRecord("a", r1)
	st.AddShard("shard-0000", s1)
	svc.AddStream(st)

	s := NewShardStreamReader(svc, "test-stream", "shard-0000")

	r, err := s.Get()
	if err != nil {
		t.Errorf("Received error %v", err)
		return
	}

	if r == nil {
		t.Errorf("Should be a record")
	}
}

func TestListShards(t *testing.T) {
	svc := newTestKinesisService()
	st := newTestKinesisStream("test-stream")

	s1 := newTestKinesisShard()
	st.AddShard("0", s1)

	s2 := newTestKinesisShard()
	st.AddShard("1", s2)

	svc.AddStream(st)

	shards, err := ListShards(svc, "test-stream")
	if err != nil {
		t.Error(err)
		return
	}
	if len(shards) != 2 {
		t.Error("Failed to find 2 shards:", len(shards))
		return
	}

	found0 := false
	found1 := false

	for _, sid := range shards {
		if sid == "0" {
			found0 = true
		} else if sid == "1" {
			found1 = true
		}
	}

	if !found0 {
		t.Error("Failed to identify shard 0")
	}
	if !found1 {
		t.Error("Failed to identify shard 1")
	}
}
