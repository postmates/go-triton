package triton

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/mattn/go-sqlite3"
)

type Checkpointer struct {
	clientName string
	stream     *Stream

	db *sql.DB
}

const CREATE_TABLE = `
CREATE TABLE triton_checkpoint (
	client VARCHAR(255),
	stream VARCHAR(255),
	shard VARCHAR(255),
	seq_num VARCHAR(255),
	PRIMARY KEY (client, stream, shard))
`

func (c *Checkpointer) Checkpoint() (err error) {
	if c.stream.LastSequenceNumber == nil {
		log.Printf("Skipping checkpoint for %s-%s", c.stream.StreamName, c.stream.ShardID)
		return
	}

	txn, err := c.db.Begin()
	if err != nil {
		return err
	}

	rows, err := txn.Query(
		"SELECT 1 FROM triton_checkpoint WHERE client=$1 AND stream=$2 AND shard=$3",
		c.clientName, c.stream.StreamName, c.stream.ShardID)
	if err != nil {
		txn.Rollback()
		return err
	}

	defer rows.Close()

	if rows.Next() {
		log.Printf("Updating checkpoint for %s-%s: %s",
			c.stream.StreamName, c.stream.ShardID, *c.stream.LastSequenceNumber)
		res, err := txn.Exec(
			"UPDATE triton_checkpoint SET seq_num=$1 WHERE client=$2 AND stream=$3 AND shard=$4",
			c.stream.LastSequenceNumber, c.clientName, c.stream.StreamName, c.stream.ShardID)
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
		log.Printf("Creating checkpoint for %s-%s: %s",
			c.stream.StreamName, c.stream.ShardID, *c.stream.LastSequenceNumber)
		_, err := txn.Exec(
			"INSERT INTO triton_checkpoint VALUES ($1, $2, $3, $4)",
			c.clientName, c.stream.StreamName, c.stream.ShardID, c.stream.LastSequenceNumber)

		if err != nil {
			txn.Rollback()
			panic(err)
		}
	}

	err = txn.Commit()

	return
}

func (c *Checkpointer) LastSequenceNumber() (seqNum string, err error) {
	seqNum = ""

	err = c.db.QueryRow("SELECT seq_num FROM triton_checkpoint WHERE client=$1 AND stream=$2 AND shard=$3",
		c.clientName, c.stream.StreamName, c.stream.ShardID).Scan(&seqNum)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", nil
		} else {
			return
		}
	}

	return
}

func checkDB(db *sql.DB) bool {
	_, err := db.Exec("select 1 from triton_checkpoint")
	// TODO: pgsql

	if sErr, ok := err.(sqlite3.Error); ok { // Now the error number is accessible directly
		if sErr.Code == sqlite3.ErrError {
			return false
		}

		panic(err)
	}

	return true
}

func initDB(db *sql.DB) (err error) {
	_, err = db.Exec(CREATE_TABLE)
	return
}

func NewCheckpointer(clientName string, stream *Stream, db *sql.DB) *Checkpointer {
	if !checkDB(db) {
		err := initDB(db)
		if err != nil {
			panic(err)
		}

		if !checkDB(db) {
			panic(fmt.Errorf("Failed with db"))
		}
	}

	c := Checkpointer{
		clientName: clientName,
		stream:     stream,
		db:         db,
	}

	return &c
}
