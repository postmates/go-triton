package triton

// Mock Kinesis Service

import (
	"bytes"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/tinylib/msgp/msgp"
)

type testKinesisRecords struct {
	sn         string
	recordData [][]byte
}

type testKinesisShard struct {
	records []testKinesisRecords
}

func (s *testKinesisShard) AddRecord(sn string, rec Record) {
	b := bytes.NewBuffer(make([]byte, 0, 1024))
	w := msgp.NewWriter(b)
	err := w.WriteMapStrIntf(rec)
	if err != nil {
		panic(err)
	}
	w.Flush()
	s.AddData(sn, b.Bytes())
}

func (s *testKinesisShard) AddData(sn string, data []byte) {
	rs := testKinesisRecords{sn, [][]byte{data}}
	s.records = append(s.records, rs)
}

func (s *testKinesisShard) PopData() (string, []byte) {
	r := s.records[0]
	s.records = s.records[1:]
	return r.sn, r.recordData[0]
}

func (s *testKinesisShard) PopRecord() (string, Record) {
	sn, data := s.PopData()

	b := bytes.NewBuffer(data)
	r := make(Record)

	w := msgp.NewReader(b)
	w.ReadMapStrIntf(r)

	return sn, r
}

func (s *testKinesisShard) NextSequenceNumber() string {
	return time.Now().String()
}

func newTestKinesisShard() *testKinesisShard {
	return &testKinesisShard{make([]testKinesisRecords, 0)}
}

type testKinesisStream struct {
	StreamName string
	shards     map[string]*testKinesisShard
}

func (s *testKinesisStream) AddShard(sid string, ts *testKinesisShard) {
	s.shards[sid] = ts
}

func newTestKinesisStream(name string) *testKinesisStream {
	return &testKinesisStream{name, make(map[string]*testKinesisShard)}
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

func parseIterator(iterVal string) (string, string, string) {
	vals := strings.Split(iterVal, ":")
	return vals[0], vals[1], vals[2]
}

func (s *testKinesisService) GetShardIterator(i *kinesis.GetShardIteratorInput) (*kinesis.GetShardIteratorOutput, error) {
	iterVal := fmt.Sprintf("%s:%s:%s", *i.StreamName, *i.ShardId, "")
	gso := &kinesis.GetShardIteratorOutput{ShardIterator: aws.String(iterVal)}
	return gso, nil
}

func (s *testKinesisService) GetRecords(gri *kinesis.GetRecordsInput) (*kinesis.GetRecordsOutput, error) {
	streamName, shardID, sn := parseIterator(*gri.ShardIterator)

	records := []*kinesis.Record{}

	stream, ok := s.streams[streamName]
	if !ok {
		return nil, fmt.Errorf("Failed to find stream")
	}

	shard, ok := stream.shards[shardID]
	if !ok {
		return nil, fmt.Errorf("Failed to find shard")
	}

	// For our mock implementation, we just assume iterator == sequence number
	nextSn := ""
	for _, r := range shard.records {
		if r.sn > sn {
			for _, rd := range r.recordData {
				records = append(records, &kinesis.Record{SequenceNumber: aws.String(string(r.sn)), Data: rd})
			}
			nextSn = string(r.sn)
			break
		}
	}

	// If we didn't find a new next iterator, just keep the original
	nextIter := *gri.ShardIterator

	if nextSn != "" {
		nextIter = fmt.Sprintf("%s:%s:%s", streamName, shardID, nextSn)
	}

	log.Printf("%s - serving %d records. Next iter %s", *gri.ShardIterator, len(records), nextIter)
	gso := &kinesis.GetRecordsOutput{
		NextShardIterator:  aws.String(nextIter),
		MillisBehindLatest: aws.Int64(0),
		Records:            records,
	}
	return gso, nil
}

func (s *testKinesisService) DescribeStream(input *kinesis.DescribeStreamInput) (*kinesis.DescribeStreamOutput, error) {

	stream, ok := s.streams[*input.StreamName]
	if !ok {
		// TODO: Probably a real error condition we could simulate
		return nil, fmt.Errorf("Failed to find stream")
	}

	var shards []*kinesis.Shard
	for sid := range stream.shards {
		shards = append(shards, &kinesis.Shard{ShardId: aws.String(string(sid))})
	}

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

func (s *testKinesisService) PutRecords(input *kinesis.PutRecordsInput) (*kinesis.PutRecordsOutput, error) {
	stream, ok := s.streams[*input.StreamName]
	if !ok {
		return nil, fmt.Errorf("Failed to find stream")
	}

	records := make([]*kinesis.PutRecordsResultEntry, len(input.Records))
	for i, r := range input.Records {
		shard, ok := stream.shards[*r.PartitionKey]
		if !ok {
			return nil, fmt.Errorf("Failed to find shard")
		}

		sn := shard.NextSequenceNumber()
		shard.AddData(sn, r.Data)

		records[i] = &kinesis.PutRecordsResultEntry{
			SequenceNumber: aws.String(string(sn)),
			ShardId:        r.PartitionKey,
		}
	}

	output := &kinesis.PutRecordsOutput{
		Records:           records,
		FailedRecordCount: aws.Int64(0),
	}

	return output, nil
}

type nullKinesisService struct{}

func (s *nullKinesisService) GetShardIterator(*kinesis.GetShardIteratorInput) (*kinesis.GetShardIteratorOutput, error) {
	gso := &kinesis.GetShardIteratorOutput{ShardIterator: aws.String("123")}
	return gso, nil
}

func (s *nullKinesisService) GetRecords(*kinesis.GetRecordsInput) (*kinesis.GetRecordsOutput, error) {
	records := []*kinesis.Record{}
	gso := &kinesis.GetRecordsOutput{
		NextShardIterator:  aws.String("124"),
		MillisBehindLatest: aws.Int64(0),
		Records:            records,
	}
	return gso, nil
}

func (s *nullKinesisService) DescribeStream(input *kinesis.DescribeStreamInput) (*kinesis.DescribeStreamOutput, error) {
	return nil, fmt.Errorf("Not Implemented")
}

func (s *nullKinesisService) PutRecords(input *kinesis.PutRecordsInput) (*kinesis.PutRecordsOutput, error) {
	results := []*kinesis.PutRecordsResultEntry{}
	gso := &kinesis.PutRecordsOutput{
		Records:           results,
		FailedRecordCount: aws.Int64(0),
	}
	return gso, nil
}

type failingKinesisService struct{}

func (s *failingKinesisService) DescribeStream(input *kinesis.DescribeStreamInput) (*kinesis.DescribeStreamOutput, error) {
	return nil, fmt.Errorf("Not Implemented")
}

func (s *failingKinesisService) GetRecords(*kinesis.GetRecordsInput) (*kinesis.GetRecordsOutput, error) {
	err := awserr.New("ProvisionedThroughputExceededException", "slow down dummy", fmt.Errorf("error"))
	return nil, err
}

func (s *failingKinesisService) GetShardIterator(*kinesis.GetShardIteratorInput) (*kinesis.GetShardIteratorOutput, error) {
	gso := &kinesis.GetShardIteratorOutput{ShardIterator: aws.String("123")}
	return gso, nil
}

func (s *failingKinesisService) PutRecords(input *kinesis.PutRecordsInput) (*kinesis.PutRecordsOutput, error) {
	err := awserr.New("ProvisionedThroughputExceededException", "slow down dummy", fmt.Errorf("error"))
	return nil, err
}
