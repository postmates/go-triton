package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/codegangsta/cli"
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
	// TODO: pgsql option
	db, err := sql.Open("sqlite3", "triton-s3.db")
	if err != nil {
		log.Fatalln("Failed to open db", err)
	}

	return db
}

// Loop on records read from the stream, send it to the store.
// Provided signal channel will tell us when to quit.
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

func store(streamName, bucketName string, shardNum int) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	sc := openStreamConfig(streamName)

	ksvc := kinesis.New(&aws.Config{Region: sc.RegionName})

	shardID, err := triton.PickShardID(ksvc, sc.StreamName, shardNum)

	db := openDB()
	defer db.Close()

	c, err := triton.NewCheckpointer("triton-store", sc.StreamName, shardID, db)
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

	s3_svc := s3.New(&aws.Config{Region: sc.RegionName})
	u := triton.NewUploader(s3_svc, bucketName)

	store := triton.NewStore(sc.StreamName, shardID, u, c)
	defer store.Close()

	loopStream(stream, store, sigs)
	log.Println("Done")
}

func listShards(streamName string) {
	sc := openStreamConfig(streamName)
	ksvc := kinesis.New(&aws.Config{Region: sc.RegionName})

	shards, err := triton.ListShards(ksvc, sc.StreamName)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error listing shards", err)
		os.Exit(1)
	}

	for _, st := range shards {
		fmt.Println(st)
	}
}

func main() {
	app := cli.NewApp()
	app.Name = "triton"
	app.Usage = "Utilities for the Triton Data Pipeline"
	app.Version = "0.0.1"
	app.Commands = []cli.Command{
		{
			Name:    "store",
			Aliases: []string{"s"},
			Usage:   "store triton data to s3",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "stream",
					Usage: "Named triton stream",
				},
				cli.IntFlag{
					Name:  "shard",
					Usage: "Shard number",
				},
				cli.StringFlag{
					Name:   "bucket",
					Usage:  "Destination S3 bucket",
					EnvVar: "TRITON_BUCKET",
				}},
			Action: func(c *cli.Context) {
				if c.String("bucket") == "" {
					fmt.Fprintln(os.Stderr, "bucket name required")
					cli.ShowSubcommandHelp(c)
					os.Exit(1)
				}

				if c.String("stream") == "" {
					fmt.Fprintln(os.Stderr, "stream name required")
					cli.ShowSubcommandHelp(c)
					os.Exit(1)
				}

				store(c.String("stream"), c.String("bucket"), c.Int("shard"))
			},
		},
		{
			Name:  "shards",
			Usage: "list shards for stream",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "stream",
					Usage: "Named triton stream",
				}},
			Action: func(c *cli.Context) {
				if c.String("stream") == "" {
					fmt.Fprintln(os.Stderr, "stream name required")
					cli.ShowSubcommandHelp(c)
					os.Exit(1)
				}

				listShards(c.String("stream"))
			},
		},
	}

	app.Run(os.Args)
}
