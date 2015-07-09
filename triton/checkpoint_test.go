package triton

import (
	"database/sql"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	_ "github.com/mattn/go-sqlite3"
)

const DB_URL string = "test.db"

func openTestDB() *sql.DB {
	db, err := sql.Open("sqlite3", DB_URL)
	if err != nil {
		panic(err)
	}

	return db
}

func closeTestDB(db *sql.DB) {
	db.Close()
	os.Remove(DB_URL)
}

func TestNewCheckpointer(t *testing.T) {
	db := openTestDB()
	defer closeTestDB(db)

	streamName := "test-stream"
	shardID := "shard-0000"
	ksvc := NullKinesisService{}
	s := NewStream(&ksvc, streamName, shardID)

	c, err := NewCheckpointer("test", s, db)
	if err != nil {
		t.Errorf("Failed to create: %v", err)
		return
	}

	if c == nil {
		t.Errorf("Failed to create")
		return
	}

	if c.clientName != "test" {
		t.Errorf("Name mismatch")
	}
}

func TestCheckpoint(t *testing.T) {
	db := openTestDB()
	defer closeTestDB(db)

	streamName := "test-stream"
	shardID := "shard-0000"
	ksvc := NullKinesisService{}
	s := NewStream(&ksvc, streamName, shardID)

	c, _ := NewCheckpointer("test", s, db)

	s.LastSequenceNumber = aws.String("1234")

	err := c.Checkpoint()
	if err != nil {
		t.Errorf("Failed to checkpoint: %v", err)
		return
	}

	seqNum, err := c.LastSequenceNumber()
	if err != nil {
		t.Errorf("Failed to load sequence number")
		return
	}

	if seqNum != "1234" {
		t.Errorf("Sequence number mismatch: %v", seqNum)
		return
	}
}

func TestCheckpointUpdate(t *testing.T) {
	db := openTestDB()
	defer closeTestDB(db)

	streamName := "test-stream"
	shardID := "shard-0000"
	ksvc := NullKinesisService{}
	s := NewStream(&ksvc, streamName, shardID)

	c, _ := NewCheckpointer("test", s, db)

	s.LastSequenceNumber = aws.String("1234")

	err := c.Checkpoint()
	if err != nil {
		t.Errorf("Failed to checkpoint: %v", err)
		return
	}

	s.LastSequenceNumber = aws.String("51234")

	err = c.Checkpoint()
	if err != nil {
		t.Errorf("Failed to checkpoint: %v", err)
		return
	}

	seqNum, err := c.LastSequenceNumber()
	if err != nil {
		t.Errorf("Failed to load sequence number")
		return
	}

	if seqNum != "51234" {
		t.Errorf("Sequence number mismatch: %v", seqNum)
		return
	}
}

func TestEmptyCheckpoint(t *testing.T) {
	db := openTestDB()
	defer closeTestDB(db)

	streamName := "test-stream"
	shardID := "shard-0000"
	ksvc := NullKinesisService{}
	s := NewStream(&ksvc, streamName, shardID)

	c, _ := NewCheckpointer("test", s, db)

	err := c.Checkpoint()
	if err != nil {
		t.Errorf("Failed to set empty checkpoint")
	}
}

func TestEmptyLastSequenceNumber(t *testing.T) {
	db := openTestDB()
	defer closeTestDB(db)

	streamName := "test-stream"
	shardID := "shard-0000"
	ksvc := NullKinesisService{}
	s := NewStream(&ksvc, streamName, shardID)

	c, _ := NewCheckpointer("test", s, db)

	seq, err := c.LastSequenceNumber()
	if err != nil {
		t.Errorf("Failed to get empty checkpoint")
	}

	if seq != "" {
		t.Errorf("Should have received empty seq")
	}
}
