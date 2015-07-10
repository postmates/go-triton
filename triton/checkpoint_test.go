package triton

import (
	"database/sql"
	"os"
	"testing"

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
	c, err := NewCheckpointer("test", streamName, shardID, db)
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

	if c.streamName != streamName {
		t.Errorf("streamName mismatch")
	}

	if c.shardID != shardID {
		t.Errorf("shardID mismatch")
	}
}

func TestCheckpoint(t *testing.T) {
	db := openTestDB()
	defer closeTestDB(db)

	c, _ := NewCheckpointer("test", "test-stream", "shard-000", db)

	err := c.Checkpoint("1234")
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

	c, _ := NewCheckpointer("test", "test-stream", "shard-000", db)

	err := c.Checkpoint("1234")
	if err != nil {
		t.Errorf("Failed to checkpoint: %v", err)
		return
	}

	err = c.Checkpoint("51234")
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

func TestEmptyLastSequenceNumber(t *testing.T) {
	db := openTestDB()
	defer closeTestDB(db)

	c, _ := NewCheckpointer("test", "test-stream", "shard-000", db)

	seq, err := c.LastSequenceNumber()
	if err != nil {
		t.Errorf("Failed to get empty checkpoint")
	}

	if seq != "" {
		t.Errorf("Should have received empty seq")
	}
}
