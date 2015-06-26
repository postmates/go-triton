package triton

import (
	"github.com/aws/aws-sdk-go/service/kinesis"
)

type KinesisService interface {
	GetShardIterator(*kinesis.GetShardIteratorInput) (*kinesis.GetShardIteratorOutput, error)
}
