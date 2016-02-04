package triton

import (
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/cenkalti/backoff"
)

const maxRetries = 5
const initialRetryInterval = 500 * time.Millisecond
const maxRetryDuration = 10 * time.Second

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

	service KinesisService // Interactions with Kinesis
}

func (s *Stream) Reader(off string) (Reader, error) {
	// decompose off to seqnums and shard ids
	comps := strings.Split(off, "__")

	if len(comps)%2 != 0 {
		return nil, fmt.Errorf("Invalid offset")
	}

	seqNums := map[string]string{}
	for i := 0; i < len(comps); i += 2 {
		seqNums[comps[i]] = comps[i+1]
	}

	sr := &streamReader{s, seqNums: seqNums}
	return sr, nil
}

type streamReader struct {
	*Stream

	seqNums  map[string]string // shardID -> seqNums
	shardIdx int               // allow for round robin reads
}

// Read records from a stream.
//
// Read will iterate through shards in a round robin fashion and return at most
// len(r) records from a single round.
func (sr *streamReader) Read(r []Record) (int, string, error) {
	n := 0
	for reads := 0; reads < len(sr.shardIDs) && n <= len(r); reads++ {
		nextShardIdx := s.shardIdx % len(sr.shardIDs)
		shardID := sr.shardIDs[nextShardIdx]
		nShard, sn, err := sr.readShard(shardID, sr.seqNums[shardID], r[n:len(r)])
		if err != nil {
			return n, err
		}

		n += nShard
		sr.seqNums[shardID] = sn // Update sequence number
		sr.shardIdx++
	}

	// TODO: create offset from seqNums
	return n, "", nil
}

func (sr *streamReader) readShard(shard, sn string, r []Record) (int, string, error) {
	gri := &kinesis.GetRecordsInput{
		Limit:         aws.Int64(int64(len(r))), // read at most buffer
		ShardIterator: aws.String(sn),
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
		if r[idx], err = UnmarshalRecord(raw.Data); err != nil {
			return idx, err
		}
	}

	return len(gro.Records), *gro.NextShardIterator, nil
}
