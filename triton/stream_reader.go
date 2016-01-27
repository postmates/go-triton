package triton

import (
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/cenkalti/backoff"
)

const maxRetries = 4
const initialRetryInterval = 50 * time.Millisecond
const maxRetryDuration = 5 * time.Second

// NewStream returns a stream configured for all shards using env variables
// to determine the Kinesis Service.
func NewStream(region, name string) (*Stream, error) {
	sess := session.New(&aws.Config{Region: aws.String(region)})
	svc := kinesis.New(sess)
	return NewStreamShardsService(svc, region, name, nil)
}

// NewStreamShardsService returns a Stream configured to the specified shards
// using the specificed Kinesis service.
//
// If no shards are given, the stream will be configured to return from all
// shards for a given stream.
func NewStreamShardsService(svc KinesisService, region, name string, shards []string) (*Stream, error) {
	if shards == nil || len(shards) == 0 {
		req := &kinesis.DescribeStreamInput{StreamName: aws.String(name)}
		resp, err := svc.DescribeStream(req)
		if err != nil {
			return nil, err
		}

		shards = make([]string, len(resp.StreamDescription.Shards))
		for idx, s := range resp.StreamDescription.Shards {
			shards[idx] = *s.ShardId
		}
	}

	return &Stream{
		region:   region,
		name:     name,
		shardIDs: shards,
		service:  svc,
	}, nil
}

type Stream struct {
	region   string
	name     string
	shardIDs []string

	seqNums  map[string]string // shardID -> seqNums
	shardIdx int               // allow for round robin reads
	lock     *sync.RWMutex     // only serial read allowed

	service KinesisService // Interactions with Kinesis
}

// Read records from a stream.
//
// Read will iterate through shards in a round robin fashion and return at most
// len(p) records from a single round.
func (s *Stream) Read(p []Record) (int, error) {
	s.lock.Lock()

	n := 0
	for reads := 0; reads < len(s.shardIDs) && n <= len(p); reads++ {
		nextShardIdx := s.shardIdx % len(s.shardIDs)
		nShard, err := s.readShard(s.shardIDs[nextShardIdx], p[n:len(p)])
		n += nShard

		if err != nil {
			return n, err
		}
	}

	s.lock.Unlock()
	return n, nil
}

func (s *Stream) Seek(shardID, sequenceNumber string) {
	s.lock.Lock()
	s.seqNums[shardID] = sequenceNumber
	s.lock.Unlock()
}

func (s *Stream) Shards() []string {
	return s.shardIDs
}

func (s *Stream) SequenceNumber(shard string) string {
	s.lock.RLock()
	sn := s.seqNums[shard]
	s.lock.RUnlock()
	return sn
}

func (s *Stream) readShard(shard string, p []Record) (int, error) {
	gri := &kinesis.GetRecordsInput{
		Limit:         aws.Int64(int64(len(p))), // read at most buffer
		ShardIterator: aws.String(s.SequenceNumber(shard)),
	}

	retries := 0
	eb := backoff.NewExponentialBackOff()
	eb.InitialInterval = initialRetryInterval
	eb.MaxElapsedTime = maxRetryDuration

	// Request loop (if retry)
REQUEST:
	gro, err := s.service.GetRecords(gri)

	awsErr := err.(awserr.Error)
	if err != nil && retries < maxRetries &&
		(awsErr.Code() == "ProvisionedThroughputExceededException" ||
			awsErr.Code() == "ServiceUnavailable" ||
			awsErr.Code() == "InternalFailure" ||
			awsErr.Code() == "Throttling") {
		time.Sleep(eb.NextBackOff()) // wait to try again
		retries++
		goto REQUEST
	} else if err != nil {
		return 0, err
	}

	// Convert records from []byte -> Record
	for idx, raw := range gro.Records {
		if p[idx], err = UnmarshalRecord(raw.Data); err != nil {
			return idx, err
		}
	}

	s.seqNums[shard] = *gro.NextShardIterator
	return len(gro.Records), nil
}
