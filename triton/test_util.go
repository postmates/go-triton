// Mock Kinesis Service
package triton

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
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

// S3 testing:

type object struct {
	content []byte
}

type bucket struct {
	items map[string]*object
}

func newBucket() *bucket {
	return &bucket{
		items: make(map[string]*object),
	}
}

// testS3Service represents a S3 service as defined by the aws-sdk-go S3 API
type testS3Service struct {
	buckets map[string]*bucket
}

// newTestS3Service creates a new testS3Service instance
func newTestS3Service() *testS3Service {
	return &testS3Service{
		buckets: make(map[string]*bucket),
	}
}

// Put is a helper method to set key to a value byte slice
func (m *testS3Service) Put(bucket string, key string, value []byte) {
	log.Printf("testS3Service Put: %s/%s %d \n", bucket, key, len(value))
	b, bucketExists := m.buckets[bucket]
	if !bucketExists {
		b = newBucket()
		m.buckets[bucket] = b
	}
	b.items[key] = &object{
		content: value,
	}
}

// GetObject is a minimal mock implementation of the S3 GetObject API method.
// This currently only supports the Key parameter and the Body response
// parameter.
func (m *testS3Service) GetObject(input *s3.GetObjectInput) (output *s3.GetObjectOutput, err error) {
	bucket, bucketExists := m.buckets[*input.Bucket]
	if !bucketExists {
		err = awserr.New(fmt.Sprintf("%s", http.StatusNotFound), "No such entity", fmt.Errorf("bucket %q does not exist", *input.Bucket))
		return
	}

	obj, keyExists := bucket.items[*input.Key]
	if !keyExists {
		err = awserr.New(fmt.Sprintf("%s", http.StatusNotFound), "No such entity", fmt.Errorf("key %q does not exist", *input.Key))
		return
	}

	output = &s3.GetObjectOutput{}
	output.Body = ioutil.NopCloser(bytes.NewBuffer(obj.content))
	return
}

// ListObjects is a mock implementation of the S3 ListObjects method.  This
// currently only supports the prefix parameter
func (m *testS3Service) ListObjects(input *s3.ListObjectsInput) (result *s3.ListObjectsOutput, err error) {
	log.Println("ListObjects:", *input)
	result = &s3.ListObjectsOutput{}
	bucket, bucketExists := m.buckets[*input.Bucket]
	log.Printf("testS3Service list: %s\n", *input.Bucket)
	if !bucketExists {
		err = awserr.New(fmt.Sprintf("%s", http.StatusNotFound), "No such entity", fmt.Errorf("bucket %q does not exist", *input.Bucket))
		return
	}
	for key, _ := range bucket.items {
		if input.Prefix != nil {
			if !strings.HasPrefix(key, *input.Prefix) {
				continue
			}
		}
		result.Contents = append(result.Contents, &s3.Object{
			Key: aws.String(key),
		})
	}
	return
}

// ListObjectsPages is an implementation of the S3 ListObjectsPages API method.
// This currently doesn't support any parameters and just falls through to
// ListObjects()
func (m *testS3Service) ListObjectsPages(input *s3.ListObjectsInput, f func(*s3.ListObjectsOutput, bool) bool) (err error) {
	res, err := m.ListObjects(input)
	if res != nil {
		f(res, true)
	}
	return
}

func (m *testS3Service) uploader() *testS3UploaderService {
	return &testS3UploaderService{testS3Service: m}

}

type testS3UploaderService struct {
	testS3Service *testS3Service
}

func (u *testS3UploaderService) Upload(input *s3manager.UploadInput) (output *s3manager.UploadOutput, err error) {
	var buf bytes.Buffer
	io.Copy(&buf, input.Body)
	u.testS3Service.Put(*input.Bucket, *input.Key, buf.Bytes())
	return
}
