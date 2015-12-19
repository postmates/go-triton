package triton

import (
	"encoding/json"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"sort"
	"sync"
)

type StreamMetadata struct {
	// shard ID => ShardInfo
	Shards     map[ShardID]*ShardInfo `json:"shards"`
	sync.Mutex `json:"-"`
}

func NewStreamMetadata() *StreamMetadata {
	return &StreamMetadata{
		Shards: make(map[ShardID]*ShardInfo),
	}
}

func (s *StreamMetadata) noteSequenceNumber(shardID ShardID, sequenceNum SequenceNumber) {
	s.Lock()
	defer s.Unlock()
	sh := s.Shards[shardID]
	if sh == nil {
		sh = &ShardInfo{}
		s.Shards[shardID] = sh
	}
	sh.noteSequenceNumber(sequenceNum)
}

type ShardInfo struct {
	MinSequenceNumber SequenceNumber `json:"min_sequence_number"`
	MaxSequenceNumber SequenceNumber `json:"max_sequence_number"`
}

func (s *ShardInfo) noteSequenceNumber(sequenceNum SequenceNumber) {
	if s.MinSequenceNumber == "" {
		s.MinSequenceNumber = sequenceNum
	} else {
		nums := []string{
			string(sequenceNum),
			string(s.MinSequenceNumber),
		}
		sort.Sort(sort.StringSlice(nums))
		s.MinSequenceNumber = SequenceNumber(nums[0])
	}
	if s.MaxSequenceNumber == "" {
		s.MaxSequenceNumber = sequenceNum
	} else {
		nums := []string{
			string(sequenceNum),
			string(s.MaxSequenceNumber),
		}
		sort.Sort(sort.StringSlice(nums))
		s.MaxSequenceNumber = SequenceNumber(nums[1])
	}
}

// ReadStreamMetadataParams are params for ReadStreamMetadata
type ReadStreamMetadataParams struct {
	s3Service  S3Service // S3 client
	Bucket     string    // S3 Bucket
	ArchiveKey string    // The base s3 key of the archive. We store the metadata like $s3key.metadata
}

const (
	streamMetadataExt = ".metadata"
	awsNoSuchEntity   = "404"
)

// ReadStreamMetadata loads the metadata for a stream archive
//
// Returns result=>nil on no metadata

func ReadStreamMetadata(client S3Service, bucket, key string) (result *StreamMetadata, err error) {
	getObjectOutput, err := client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key + streamMetadataExt),
	})
	if err != nil {
		if awsError, ok := err.(awserr.Error); ok {
			// Return nil on 404/NoSuchEntity
			if awsError.Code() == awsNoSuchEntity {
				err = nil
				return
			}
		}
		return
	}
	defer getObjectOutput.Body.Close()
	result = NewStreamMetadata()
	err = json.NewDecoder(getObjectOutput.Body).Decode(result)
	return
}
