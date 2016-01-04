package triton

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	//"github.com/aws/aws-sdk-go/aws/awsutil"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

// Utility function to pick a shard id given an integer shard number.
// Use this if you want the 2nd shard, but don't know what the id would be.
func PickShardID(svc KinesisService, streamName string, shardNum int) (sid ShardID, err error) {
	resp, err := svc.DescribeStream(&kinesis.DescribeStreamInput{StreamName: aws.String(streamName)})
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == "ResourceNotFoundException" {
				err = fmt.Errorf("Failed to find stream %q: %v", streamName, awsErr.Message())
				return
			}
		}

		return
	}

	if len(resp.StreamDescription.Shards) < shardNum {
		err = fmt.Errorf("Stream doesn't have a shard %d", shardNum)
		return
	}

	sid = ShardID(*resp.StreamDescription.Shards[shardNum].ShardId)
	return
}

func ListShards(svc KinesisService, streamName string) (shards []ShardID, err error) {
	resp, err := svc.DescribeStream(&kinesis.DescribeStreamInput{StreamName: aws.String(streamName)})
	if err != nil {
		return
	}

	for _, s := range resp.StreamDescription.Shards {
		shards = append(shards, ShardID(*s.ShardId))
	}

	return
}
