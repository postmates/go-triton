// Mock Kinesis Service
package triton

import (
	"bytes"
	"fmt"
	"log"
	"strings"

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

func (s *testKinesisShard) AddOverlengthRecord(sn SequenceNumber, rec map[string]interface{}) {
	b := bytes.NewBuffer(make([]byte, 0, 1024))
	w := msgp.NewWriter(b)
	err := w.WriteMapStrIntf(rec)
	if err != nil {
		panic(err)
	}
	w.Flush()
	b.Write([]byte("Hello Failure"))
	rs := testKinesisRecords{sn, [][]byte{b.Bytes()}}
	s.records = append(s.records, rs)
}

func (s *testKinesisShard) AddBadEncodingRecord(sn SequenceNumber) {
	b := bytes.NewBuffer(make([]byte, 0, 1024))
	b.Write([]byte("Hello Failure"))
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

	shard, ok := stream.shards[ShardID(shardID)]
	if !ok {
		return nil, fmt.Errorf("Failed to find shard")
	}

	// For our mock implementation, we just assume iterator == sequence number
	nextSn := ""
	for _, r := range shard.records {
		if r.sn > SequenceNumber(sn) {
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
	shards := make([]*kinesis.Shard, 0)

	stream, ok := s.streams[*input.StreamName]
	if !ok {
		// TODO: Probably a real error condition we could simulate
		return nil, fmt.Errorf("Failed to find stream")
	}

	for sid, _ := range stream.shards {
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

type noopCheckpointer struct{}

func (c noopCheckpointer) Checkpoint(shardID ShardID, sequenceNumber SequenceNumber) (err error) {
	return
}

func (c noopCheckpointer) LastSequenceNumber(shardID ShardID) (seq SequenceNumber, err error) {
	return
}
