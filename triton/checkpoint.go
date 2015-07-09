package triton

import (
	"database/sql"
	"fmt"

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
		fmt.Println("Do UPDATE")
		res, err := txn.Exec(
			"UPDATE triton_checkpoint VALUES seq_num=$4 WHERE client=$1 AND stream=$2 AND shard=$3",
			c.clientName, c.stream.StreamName, c.stream.ShardID, c.stream.LastSequenceNumber)
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
		fmt.Println("Do INSERT")
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
