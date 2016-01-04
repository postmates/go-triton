package triton

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
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

func TestNewShardStreamReader(t *testing.T) {
	svc := NullKinesisService{}

	s := newShardReader(&newShardReaderParams{
		kinesisService: &svc,
		stream:         "test-stream",
		shardID:        "shard-0001",
	})
	if s.stream != "test-stream" {
		t.Errorf("bad stream name")
	}

	if s.shardID != ShardID("shard-0001") {
		t.Errorf("bad ShardID")
	}

	if s.nextIterator != "" {
		t.Errorf("bad NextIteratorValue")
	}
}

func TestRead(t *testing.T) {
	svc := newTestKinesisService()
	st := newTestKinesisStream("test-stream")
	s1 := newTestKinesisShard()

	r1 := make(map[string]interface{})
	s1.AddRecord(SequenceNumber("a"), r1)
	st.AddShard("shard-0000", s1)
	svc.AddStream(st)

	s := newShardReader(&newShardReaderParams{
		kinesisService: svc,
		stream:         "test-stream",
		shardID:        "shard-0000",
	})
	defer func() {
		s.close <- true
	}()
	select {
	case record := <-s.records:
		if record == nil {
			t.Errorf("Should be a record")
		}
	case err := <-s.errors:
		t.Errorf("Received error %v", err)
	}
}

func TestListShards(t *testing.T) {
	svc := newTestKinesisService()
	st := newTestKinesisStream("test-stream")

	s1 := newTestKinesisShard()
	st.AddShard(ShardID("0"), s1)

	s2 := newTestKinesisShard()
	st.AddShard(ShardID("1"), s2)

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
		if sid == ShardID("0") {
			found0 = true
		} else if sid == ShardID("1") {
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
