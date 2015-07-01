package triton

import (
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type KinesisService interface {
	GetShardIterator(*kinesis.GetShardIteratorInput) (*kinesis.GetShardIteratorOutput, error)
	GetRecords(*kinesis.GetRecordsInput) (*kinesis.GetRecordsOutput, error)
}

type S3Service interface {
}

type S3UploaderService interface {
	Upload(input *s3manager.UploadInput) (*s3manager.UploadOutput, error)
}
