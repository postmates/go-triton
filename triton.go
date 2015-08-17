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

// Store Command
//
// NOTE: for now we're planning on having a single process handle all our
// shards.  In the future, as this thing scales, it will probably be convinient
// to have command line arguments to indicate which shards we should process.
func store(clientName, streamName, bucketName string, dbUrl string, skipToLatest bool) {
	sc := openStreamConfig(streamName)

	config := aws.NewConfig().WithRegion(sc.RegionName)
	kSvc := kinesis.New(config)
	s3Svc := s3.New(config)

	db := openDB(dbUrl)
	defer db.Close()

	c, err := triton.NewCheckpointer(clientName, sc.StreamName, db)
	if err != nil {
		log.Println("Failed to open Checkpointer", err)
		return
	}

	if skipToLatest {
		// TODO: Reset checkpointer
	}

	stream, err := triton.NewStreamReader(kSvc, sc.StreamName, c)

	u := triton.NewUploader(s3Svc, bucketName)

	storeName := fmt.Sprintf("%s-%s", sc.StreamName, clientName)
	store := triton.NewStore(storeName, stream, u)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		select {
		case <-sigs:
			stream.Stop()
			return
		}
	}()

	// Blocks till EOF
	err = store.Store()
	if err != nil {
		log.Fatalln("Error during store:", err)
	}

	store.Close()

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
				cli.StringFlag{
					Name:   "client-name",
					Usage:  "optional name of triton client",
					Value:  "store",
					EnvVar: "TRITON_CLIENT",
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

				store(c.String("client-name"), c.String("stream"), c.String("bucket"), c.String("snapshot-db"), c.Bool("skip-to-latest"))
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

				set, err := triton.NewStoreReader(s3Svc, c.String("bucket"), sc.StreamName, start, end)
				if err != nil {
					log.Fatalln("Failure listing archive:", err)
				}

				for {
					rec, err := set.ReadRecord()
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
