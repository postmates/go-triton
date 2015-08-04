// Mock Kinesis Service
package triton

import (
	"bytes"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/tinylib/msgp/msgp"
)

type testKinesisRecords struct {
	sn         SequenceNumber
	recordData [][]byte
}

type testKinesisShard struct {
	records []testKinesisRecords
}

func (s *testKinesisShard) AddRecord(sn SequenceNumber, rec map[string]interface{}) {
	b := bytes.NewBuffer(make([]byte, 0, 1024))
	w := msgp.NewWriter(b)
	err := w.WriteMapStrIntf(rec)
	if err != nil {
		panic(err)
	}
	w.Flush()
	rs := testKinesisRecords{sn, [][]byte{b.Bytes()}}
	s.records = append(s.records, rs)
}

func newTestKinesisShard() *testKinesisShard {
	return &testKinesisShard{make([]testKinesisRecords, 0)}
}

type testKinesisStream struct {
	StreamName string
	shards     map[ShardID]*testKinesisShard
}

func (s *testKinesisStream) AddShard(sid ShardID, ts *testKinesisShard) {
	s.shards[sid] = ts
}

func newTestKinesisStream(name string) *testKinesisStream {
	return &testKinesisStream{name, make(map[ShardID]*testKinesisShard)}
}

type testKinesisService struct {
	streams map[string]*testKinesisStream
}

func newTestKinesisService() *testKinesisService {
	return &testKinesisService{make(map[string]*testKinesisStream)}
}

func (s *testKinesisService) AddStream(stream *testKinesisStream) {
	s.streams[stream.StreamName] = stream
}

func (s *testKinesisService) GetShardIterator(*kinesis.GetShardIteratorInput) (*kinesis.GetShardIteratorOutput, error) {
	gso := &kinesis.GetShardIteratorOutput{ShardIterator: aws.String("1")}
	return gso, nil
}

func (s *testKinesisService) GetRecords(*kinesis.GetRecordsInput) (*kinesis.GetRecordsOutput, error) {
	records := []*kinesis.Record{}
	gso := &kinesis.GetRecordsOutput{
		NextShardIterator:  aws.String("124"),
		MillisBehindLatest: aws.Int64(0),
		Records:            records,
	}
	return gso, nil
}

func (s *testKinesisService) DescribeStream(input *kinesis.DescribeStreamInput) (*kinesis.DescribeStreamOutput, error) {
	shards := make([]*kinesis.Shard, 0)
	dso := &kinesis.DescribeStreamOutput{
		StreamDescription: &kinesis.StreamDescription{
			Shards:       shards,
			StreamARN:    aws.String("test"),
			StreamName:   input.StreamName,
			StreamStatus: aws.String("ACTIVE"),
		},
	}

	return dso, nil
}
