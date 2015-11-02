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
	c, err := NewCheckpointer("test", streamName, db)
	if err != nil {
		t.Errorf("Failed to create: %v", err)
		return
	}

	if c == nil {
		t.Errorf("Failed to create")
		return
	}

	cdb := c.(*dbCheckpointer)

	if cdb.clientName != "test" {
		t.Errorf("Name mismatch")
	}

	if cdb.streamName != streamName {
		t.Errorf("streamName mismatch")
	}
}

func TestCheckpoint(t *testing.T) {
	db := openTestDB()
	defer closeTestDB(db)

	sid := ShardID("shardId-0000")
	c, _ := NewCheckpointer("test", "test-stream", db)

	err := c.Checkpoint(sid, "1234")
	if err != nil {
		t.Errorf("Failed to checkpoint: %v", err)
		return
	}

	seqNum, err := c.LastSequenceNumber(sid)
	if err != nil {
		t.Errorf("Failed to load sequence number", err)
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

	sid := ShardID("shardId-0000")

	c, _ := NewCheckpointer("test", "test-stream", db)

	err := c.Checkpoint(sid, "1234")
	if err != nil {
		t.Errorf("Failed to checkpoint: %v", err)
		return
	}

	err = c.Checkpoint(sid, "51234")
	if err != nil {
		t.Errorf("Failed to checkpoint: %v", err)
		return
	}

	seqNum, err := c.LastSequenceNumber(sid)
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

	sid := ShardID("shardId-0000")

	c, _ := NewCheckpointer("test", "test-stream", db)

	seq, err := c.LastSequenceNumber(sid)
	if err != nil {
		t.Errorf("Failed to get empty checkpoint")
	}

	if seq != "" {
		t.Errorf("Should have received empty seq")
	}
}

func TestStats(t *testing.T) {
	db := openTestDB()
	defer closeTestDB(db)

	sid := ShardID("shardId-0000")

	c, _ := NewCheckpointer("test", "test-stream", db)

	err := c.Checkpoint(sid, "1234")
	if err != nil {
		t.Errorf("Failed to checkpoint: %v", err)
		return
	}

	stats, err := GetCheckpointStats("test", db)
	if err != nil {
		t.Error(err)
		return
	}

	v, ok := stats["test.test-stream.shardId-0000.age"]
	if !ok {
		t.Errorf("Failed to find value")
		return
	}

	if v > 1 || v < 0 {
		t.Errorf("Bad value, should be basically 0: %d", v)
	}
}
