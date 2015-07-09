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

	c := NewCheckpointer("test", s, db)

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

	c := NewCheckpointer("test", s, db)

	s.LastSequenceNumber = aws.String("1234")

	err := c.Checkpoint()
	if err != nil {
		t.Errorf("Failed to checkpoint: %v", err)
	}
}
