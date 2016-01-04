package triton

import (
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

// A StoreArchive represents an instance of a data file stored, usually, in S3.
type StoreArchive struct {
	StreamName string
	Bucket     string
	Key        string
	ClientName string

	T time.Time

	s3Svc S3Service
	rdr   Reader
}

func (sa *StoreArchive) ReadRecord() (rec Record, err error) {
	if sa.rdr == nil {
		out, err := sa.s3Svc.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(sa.Bucket),
			Key:    aws.String(sa.Key),
		})

		if err != nil {
			return nil, err
		}

		sa.rdr = NewArchiveReader(out.Body)
	}
	rec, err = sa.rdr.ReadRecord()
	return
}

func (sa *StoreArchive) parseKeyName(keyName string) (err error) {
	key, err := DecodeArchiveKey(keyName)
	if err != nil {
		return
	}
	sa.T = key.Time
	sa.StreamName = key.Stream
	sa.ClientName = key.Client
	return
}

// Read the stream metadata associated with this store archive instance
func (sa *StoreArchive) GetStreamMetadata() (result *StreamMetadata, err error) {
	result, err = ReadStreamMetadata(sa.s3Svc, sa.Bucket, sa.Key)

	return
}

// NewStoreArchive returns a StoreArchive instance
func NewStoreArchive(bucketName, keyName string, svc S3Service) (sa StoreArchive, err error) {
	sa.Bucket = bucketName
	sa.Key = keyName
	sa.s3Svc = svc
	err = sa.parseKeyName(keyName)
	if err != nil {
		return
	}
	return
}
