package main

import (
	"database/sql"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/s3"
	_ "github.com/mattn/go-sqlite3"
	"github.com/postmates/postal-go-triton/triton"
)

var LOG_INTERVAL = 10 * time.Second

func openStreamConfig(streamName string) *triton.StreamConfig {
	fname := os.Getenv("TRITON_CONFIG")
	if fname == "" {
		log.Fatalln("TRITON_CONFIG not specific")
	}

	f, err := os.Open(fname)
	if err != nil {
		log.Fatalln("Failed to open config", err)
	}

	c, err := triton.NewConfigFromFile(f)
	if err != nil {
		log.Fatalln("Failed to load config", err)
	}

	sc, err := c.ConfigForName(streamName)
	if err != nil {
		log.Fatalln("Failed to load config for stream", err)
	}

	return sc
}

func openDB() *sql.DB {
	db, err := sql.Open("sqlite3", "triton-s3.db")
	if err != nil {
		log.Fatalln("Failed to open db", err)
	}

	return db
}

func loopStream(stream *triton.Stream, store *triton.Store, sigs chan os.Signal) {
	logTime := time.Now()
	recCount := 0

	for {
		if time.Since(logTime) >= LOG_INTERVAL {
			log.Printf("Recorded %d records", recCount)
			logTime = time.Now()
			recCount = 0
		}

		r, err := stream.Read()
		if err != nil {
			log.Fatalln("Failed to read from stream:", err)
		}

		if r == nil {
			panic("r is nil?")
		}

		recCount += 1
		err = store.PutRecord(r)
		if err != nil {
			log.Fatalln("Failed to put record:", err)
		}

		select {
		case <-sigs:
			// The caller is probably closing too, but just to make sure our
			// graceful exit looks really graceful, do it here.
			store.Close()
			return
		default:
			continue
		}
	}
}

func main() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	sc := openStreamConfig("courier_activity")

	ksvc := kinesis.New(&aws.Config{Region: sc.RegionName})

	shardID, err := triton.PickShardID(ksvc, sc.StreamName, 0)

	db := openDB()
	defer db.Close()

	c, err := triton.NewCheckpointer("triton-s3", sc.StreamName, shardID, db)
	if err != nil {
		log.Fatalln("Failed to open Checkpointer", err)
	}

	seqNum, err := c.LastSequenceNumber()
	if err != nil {
		log.Fatalln("Failed to load last sequence number", err)
	}

	var stream *triton.Stream
	if len(seqNum) > 0 {
		log.Printf("Opening stream %s-%s at %s", sc.StreamName, shardID, seqNum)
		stream = triton.NewStreamFromSequence(ksvc, sc.StreamName, shardID, seqNum)
	} else {
		log.Printf("Opening stream %s-%s at LATEST", sc.StreamName, shardID)
		stream = triton.NewStream(ksvc, sc.StreamName, shardID)
	}

	bucketName := "postal-triton-dev"
	s3_svc := s3.New(&aws.Config{Region: sc.RegionName})
	u := triton.NewUploader(s3_svc, bucketName)

	store := triton.NewStore(sc.StreamName, shardID, u, c)
	defer store.Close()

	loopStream(stream, store, sigs)
	log.Println("Done")
}
