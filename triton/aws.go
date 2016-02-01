package triton

import (
	"io"

	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// These 'Services' are just shims to allow for easier testing.  They also give
// us an idea of the minimum functionality we're using from our APIs.

type KinesisService interface {
	DescribeStream(*kinesis.DescribeStreamInput) (*kinesis.DescribeStreamOutput, error)
	GetShardIterator(*kinesis.GetShardIteratorInput) (*kinesis.GetShardIteratorOutput, error)
	GetRecords(*kinesis.GetRecordsInput) (*kinesis.GetRecordsOutput, error)
	PutRecords(input *kinesis.PutRecordsInput) (*kinesis.PutRecordsOutput, error)
}

type S3Service interface {
	ListObjects(*s3.ListObjectsInput) (*s3.ListObjectsOutput, error)
	Download(w io.WriterAt, input *s3.GetObjectInput, options ...func(*s3manager.Downloader)) (n int64, err error)
	Upload(input *s3manager.UploadInput, options ...func(*s3manager.Uploader)) (*s3manager.UploadOutput, error)
}

type nullS3Service struct{}

func (s *nullS3Service) Download(w io.WriterAt, input *s3.GetObjectInput, options ...func(*s3manager.Downloader)) (int64, error) {
	return 0, nil
}

func (s *nullS3Service) ListObjects(*s3.ListObjectsInput) (*s3.ListObjectsOutput, error) {
	loo := &s3.ListObjectsOutput{}
	return loo, nil
}

func (s *nullS3Service) Upload(input *s3manager.UploadInput, options ...func(*s3manager.Uploader)) (*s3manager.UploadOutput, error) {
	return &s3manager.UploadOutput{}, nil
}
