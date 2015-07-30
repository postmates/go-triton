package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/codegangsta/cli"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	"github.com/postmates/go-triton/triton"
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

func openDB(db_url_s string) (db *sql.DB) {
	db_url, err := url.Parse(db_url_s)
	if err != nil {
		log.Fatalln("Failed to parse", db_url_s)
	}

	if db_url.Scheme == "sqlite" {
		db, err = sql.Open("sqlite3", "triton-s3.db")
		if err != nil {
			log.Fatalln("Failed to open db", err)
		}

		// sqlite doesn't so much like concurrency
		db.SetMaxOpenConns(1)
		return
	} else if db_url.Scheme == "postgres" {
		db, err = sql.Open("postgres", db_url_s)
		if err != nil {
			log.Fatalln("Failed to open db", err)
		}
		return
	} else {
		log.Fatalln("Unknown db scheme", db_url.Scheme)
		return
	}
}

// Loop on records read from the stream, send it to the store.
// Provided signal channel will tell us when to quit.
func loopStream(stream *triton.Stream, store *triton.Store, quit chan bool) {
	logTime := time.Now()
	recCount := 0

	defer store.Close()

	for {
		if time.Since(logTime) >= LOG_INTERVAL {
			log.Printf("Recorded %d records for (%s, %s)", recCount, stream.StreamName, stream.ShardID)
			logTime = time.Now()
			recCount = 0
		}

		r, err := stream.Read()
		if err != nil {
			log.Printf("Failed to read from (%s, %s): %v", stream.StreamName, stream.ShardID, err)
			return
		}

		if r == nil {
			log.Printf("r for (%s, %s) is nil?", stream.StreamName, stream.ShardID)
			return
		}

		recCount += 1
		err = store.PutRecord(r)
		if err != nil {
			log.Println("Failed to put record:", err)
			return
		}

		select {
		case <-quit:
			log.Printf("Quit signal received for (%s, %s)", stream.StreamName, stream.ShardID)
			return
		default:
			continue
		}
	}
}

func storeShard(sc *triton.StreamConfig, shardID string, bucketName string, skipToLatest bool, db *sql.DB, quitChan chan bool) {
	ksvc := kinesis.New(&aws.Config{Region: aws.String(sc.RegionName)})

	c, err := triton.NewCheckpointer("triton-store", sc.StreamName, shardID, db)
	if err != nil {
		log.Println("Failed to open Checkpointer", err)
		return
	}

	seqNum, err := c.LastSequenceNumber()
	if err != nil {
		log.Println("Failed to load last sequence number", err)
		return
	}

	var stream *triton.Stream
	if !skipToLatest && len(seqNum) > 0 {
		log.Printf("Opening stream %s-%s at %s", sc.StreamName, shardID, seqNum)
		stream = triton.NewStreamFromSequence(ksvc, sc.StreamName, shardID, seqNum)
	} else {
		log.Printf("Opening stream %s-%s at LATEST", sc.StreamName, shardID)
		stream = triton.NewStream(ksvc, sc.StreamName, shardID)
	}

	s3_svc := s3.New(&aws.Config{Region: aws.String(sc.RegionName)})
	u := triton.NewUploader(s3_svc, bucketName)

	store := triton.NewStore(sc.StreamName, shardID, u, c)

	loopStream(stream, store, quitChan)
}

// Store Command
// Spin up go routine for each of our shards and store the data in S3
//
// NOTE: for now we're planning on having a single process handle all our
// shards.  In the future, as this thing scales, it will probably be convinient
// to have command line arguments to indicate which shards we should process.
func store(streamName, bucketName string, dbUrl string, skipToLatest bool) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	sc := openStreamConfig(streamName)

	ksvc := kinesis.New(&aws.Config{Region: aws.String(sc.RegionName)})

	db := openDB(dbUrl)
	defer db.Close()

	shards, err := triton.ListShards(ksvc, sc.StreamName)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error listing shards", err)
		os.Exit(1)
	}

	var wg sync.WaitGroup

	anyQuit := make(chan bool, 1)
	quitChans := make([]chan bool, 0, 10)
	for _, shardID := range shards {
		quitChan := make(chan bool, 1)
		quitChans = append(quitChans, quitChan)

		wg.Add(1)

		go func(shardID string, qc chan bool) {
			storeShard(sc, shardID, bucketName, skipToLatest, db, qc)

			// The order is important here, because anyQuit isn't a big
			// channel, it could block.
			wg.Done()

			anyQuit <- true
		}(shardID, quitChan)
	}

	// Channel logic
	// Basically we have a couple of conditions that can cause us to exit.
	// 1. We received an os signal to stop
	// 2. Any of our worker routines quit
	//
	// Once we've decided to stop, we want to tell all our workers to quit,
	// then wait for them to do so.
	select {
	case <-sigs:
		break
	case <-anyQuit:
		break
	}

	log.Println("Quitting")
	for _, c := range quitChans {
		c <- true
	}

	log.Println("Waiting for workers to exit")
	wg.Wait()

	log.Println("Done")
}

// List Shards Command
//
// Just print out a list of shards for the given stream
func listShards(streamName string) {
	sc := openStreamConfig(streamName)
	ksvc := kinesis.New(&aws.Config{Region: aws.String(sc.RegionName)})

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
				cli.StringFlag{
					Name:   "bucket",
					Usage:  "Destination S3 bucket",
					EnvVar: "TRITON_BUCKET",
				},
				cli.BoolFlag{
					Name:  "skip-to-latest",
					Usage: "Skip to latest in stream (ignoring previous checkpoints)",
				},
				cli.StringFlag{
					Name:   "snapshot-db",
					Usage:  "Database connect string for storign snapshots. Defaults to local sqlite.",
					Value:  "sqlite://triton.db",
					EnvVar: "TRITON_DB",
				},
			},
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

				store(c.String("stream"), c.String("bucket"), c.String("snapshot-db"), c.Bool("skip-to-latest"))
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
		{
			Name:  "cat",
			Usage: "cat stored triton data from s3",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "stream",
					Usage: "Named triton stream",
				},
				cli.StringFlag{
					Name:   "bucket",
					Usage:  "Source S3 bucket",
					EnvVar: "TRITON_BUCKET",
				},
				cli.StringFlag{
					Name:  "start-date",
					Usage: "Date to start streaming from YYYYMMDD",
				},
				cli.StringFlag{
					Name:  "end-date",
					Usage: "(optional) Date to stop streaming from YYYYMMDD",
				}},
			Action: func(c *cli.Context) {
				if c.String("stream") == "" {
					fmt.Fprintln(os.Stderr, "stream name required")
					cli.ShowSubcommandHelp(c)
					os.Exit(1)
				}

				if c.String("bucket") == "" {
					fmt.Fprintln(os.Stderr, "bucket name required")
					cli.ShowSubcommandHelp(c)
					os.Exit(1)
				}

				if c.String("start-date") == "" {
					fmt.Fprintln(os.Stderr, "start-date required")
					cli.ShowSubcommandHelp(c)
					os.Exit(1)
				}

				// TODO: configure region
				s3Svc := s3.New(&aws.Config{Region: aws.String("us-west-1")})

				start, err := time.Parse("20060102", c.String("start-date"))
				if err != nil {
					fmt.Fprintln(os.Stderr, "invalid start-date")
					cli.ShowSubcommandHelp(c)
					os.Exit(1)
				}

				end := start
				if c.String("end-date") != "" {
					end, err = time.Parse("20060102", c.String("end-date"))
					if err != nil {
						fmt.Fprintln(os.Stderr, "invalid end-date")
						cli.ShowSubcommandHelp(c)
						os.Exit(1)
					}
				}

				sc := openStreamConfig(c.String("stream"))

				saList, err := triton.ListArchive(c.String("bucket"), sc.StreamName, start, end, s3Svc)
				if err != nil {
					log.Fatalln("Failure listing archive:", err)
				}

				log.Printf("Found %d archive files", len(saList))

				r, err := saList[0].Open()
				if err != nil {
					log.Fatalln("Error opening archive", err)
				}

				for {
					rec, err := r.ReadRecord()
					if err != nil {
						if err == io.EOF {
							break
						} else {
							log.Fatalln("Failed reading record", err)
						}
					}

					b, err := json.Marshal(rec)
					if err != nil {
						panic(err)
					}
					fmt.Println(string(b))
				}

			},
		},
	}

	app.Run(os.Args)
}
