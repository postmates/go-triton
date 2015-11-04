package triton

import (
	"database/sql"
	"fmt"
	"log"
	"time"
)

type Checkpointer interface {
	Checkpoint(ShardID, SequenceNumber) error
	LastSequenceNumber(ShardID) (SequenceNumber, error)
}

// A checkpointer manages saving and loading savepoints for reading from a
// Kinesis stream. It expects a reasonably compliant SQL database to read and write to.
// On first use, it will attempt to create the table to store results in.
// Checkpoints are unique based on client and (streamName, shardID)
type dbCheckpointer struct {
	clientName string
	streamName string

	db *sql.DB
}

// Stores the provided recent sequence number
func (c *dbCheckpointer) Checkpoint(sid ShardID, sn SequenceNumber) (err error) {
	txn, err := c.db.Begin()
	if err != nil {
		return err
	}

	rows, err := txn.Query(
		"SELECT 1 FROM triton_checkpoint WHERE client=$1 AND stream=$2 AND shard=$3",
		c.clientName, c.streamName, string(sid))
	if err != nil {
		txn.Rollback()
		return err
	}

	hasCheckpoint := rows.Next()

	rows.Close()

	if hasCheckpoint {
		log.Printf("Updating checkpoint for %s-%s: %s", c.streamName, sid, sn)
		res, err := txn.Exec(
			"UPDATE triton_checkpoint SET seq_num=$1, updated=$2 WHERE client=$3 AND stream=$4 AND shard=$5",
			string(sn), time.Now().Unix(), c.clientName, c.streamName, string(sid))
		if err != nil {
			txn.Rollback()
			return err
		}

		n, err := res.RowsAffected()
		if n <= 0 {
			txn.Rollback()
			panic("Should have updated rows")
		}

	} else {
		log.Printf("Creating checkpoint for %s-%s: %s", c.streamName, sid, sn)
		_, err := txn.Exec(
			"INSERT INTO triton_checkpoint VALUES ($1, $2, $3, $4, $5)",
			c.clientName, c.streamName, string(sid), string(sn), time.Now().Unix())

		if err != nil {
			txn.Rollback()
			panic(err)
		}
	}

	err = txn.Commit()

	return
}

// Returns the most recently checkpointed sequence number
func (c *dbCheckpointer) LastSequenceNumber(sid ShardID) (sn SequenceNumber, err error) {
	seqNum := ""
	err = c.db.QueryRow("SELECT seq_num FROM triton_checkpoint WHERE client=$1 AND stream=$2 AND shard=$3",
		c.clientName, c.streamName, string(sid)).Scan(&seqNum)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", nil
		} else {
			return
		}
	}
	sn = SequenceNumber(seqNum)

	return
}

const CREATE_TABLE_STMT = `
CREATE TABLE IF NOT EXISTS triton_checkpoint (
	client VARCHAR(255) NOT NULL,
	stream VARCHAR(255) NOT NULL,
	shard VARCHAR(255) NOT NULL,
	seq_num VARCHAR(255) NOT NULL,
	updated INTEGER NOT NULL,
	PRIMARY KEY (client, stream, shard))
`

func initDB(db *sql.DB) (err error) {
	_, err = db.Exec(CREATE_TABLE_STMT)
	return
}

// Create a new Checkpointer.
// May return an error if the database is not usable.
func NewCheckpointer(clientName string, streamName string, db *sql.DB) (Checkpointer, error) {
	err := initDB(db)
	if err != nil {
		return nil, fmt.Errorf("Failed to initialize db: %v", err)
	}

	c := dbCheckpointer{
		clientName: clientName,
		streamName: streamName,
		db:         db,
	}

	return &c, nil
}

func GetCheckpointStats(clientName string, db *sql.DB) (stat map[string]int64, err error) {
	stat = make(map[string]int64)
	rows, err := db.Query("SELECT updated, stream, shard FROM triton_checkpoint WHERE client=$1", clientName)
	if err != nil {
		return
	}

	defer rows.Close()

	for rows.Next() {
		var updated int64
		var stream, shard string

		err = rows.Scan(&updated, &stream, &shard)
		if err != nil {
			return
		}

		age := time.Now().Unix() - updated
		statName := fmt.Sprintf("%s.%s.%s.age", clientName, stream, shard)
		stat[statName] = age
	}

	return
}
