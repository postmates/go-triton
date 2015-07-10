package triton

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// These 'Services' are just shims to allow for easier testing.  They also give
// us an idea of the minimum functionality we're using from our APIs.

type KinesisService interface {
	DescribeStream(*kinesis.DescribeStreamInput) (*kinesis.DescribeStreamOutput, error)
	GetShardIterator(*kinesis.GetShardIteratorInput) (*kinesis.GetShardIteratorOutput, error)
	GetRecords(*kinesis.GetRecordsInput) (*kinesis.GetRecordsOutput, error)
}

type S3Service interface {
}

type S3UploaderService interface {
	Upload(input *s3manager.UploadInput) (*s3manager.UploadOutput, error)
}

type DynamoDBService interface {
	UpdateItem(input *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error)
}
