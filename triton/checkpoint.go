package triton

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type Checkpointer struct {
	clientName string
	streamName string
	shardID    string

	db DynamoDBService
}

func (c *Checkpointer) Checkpoint() (err error) {
	// iu := make(map[string]*dynamodb.AttributeValueUpdate)

	key := make(map[string]*dynamodb.AttributeValue)
	key["clientName"] = &dynamodb.AttributeValue{S: aws.String(c.clientName)}

	pi := dynamodb.UpdateItemInput{
		TableName: aws.String("triton"),
		Key:       key,
		//Item: item,
	}

	_, err = c.db.UpdateItem(&pi)

	return
}

func NewCheckpointer(clientName string, stream *Stream, db DynamoDBService) *Checkpointer {
	c := Checkpointer{
		clientName: clientName,
		streamName: stream.StreamName,
		shardID:    stream.ShardID,
		db:         db,
	}

	return &c
}
