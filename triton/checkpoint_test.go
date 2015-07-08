package triton

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type NullDynamoDBService struct {
	// Just enough to record the last Update
	Key              map[string]*dynamodb.AttributeValue
	AttributeUpdates map[string]*dynamodb.AttributeValueUpdate
}

func (s *NullDynamoDBService) UpdateItem(input *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
	s.Key = input.Key
	s.AttributeUpdates = input.AttributeUpdates

	o := dynamodb.UpdateItemOutput{}

	return &o, nil
}

func TestNewCheckpointer(t *testing.T) {
	streamName := "test-stream"
	shardID := "shard-0000"
	ksvc := NullKinesisService{}
	s := NewStream(&ksvc, streamName, shardID)

	db := NullDynamoDBService{}
	c := NewCheckpointer("test", s, &db)

	if c == nil {
		t.Errorf("Failed to create")
		return
	}

	if c.clientName != "test" {
		t.Errorf("Name mismatch")
	}

	if c.streamName != streamName {
		t.Errorf("StreamName mismatch")
	}

	if c.shardID != shardID {
		t.Errorf("shardID mismatch")
	}
}

func TestCheckpoint(t *testing.T) {
	streamName := "test-stream"
	shardID := "shard-0000"
	ksvc := NullKinesisService{}
	s := NewStream(&ksvc, streamName, shardID)

	db := NullDynamoDBService{}
	c := NewCheckpointer("test", s, &db)

	s.LastSequenceNumber = aws.String("1234")

	c.Checkpoint()

	if db.Key == nil {
		t.Errorf("No update")
		return
	}

	t.Errorf("Found %v", db.Key)
}
