package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/postmates/postal-go-triton/triton"
)

func main() {
	fname := os.Getenv("TRITON_CONFIG")
	if fname == "" {
		fmt.Println("TRITON_CONFIG not specific")
		os.Exit(1)
	}

	f, err := os.Open(fname)
	if err != nil {
		panic(err)
	}

	c, err := triton.NewConfigFromFile(f)
	if err != nil {
		panic(err)
	}

	sc, err := c.ConfigForName("courier_activity")
	if err != nil {
		panic(err)
	}

	s, err := triton.OpenStream(sc, 0)
	if err != nil {
		panic(err)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	bucketName := "postal-triton-dev"

	svc := s3.New(&aws.Config{Region: sc.RegionName})
	u := triton.NewUploader(svc, bucketName)

	st := triton.NewStore(sc, "0000", u)

	defer st.Close()

	for {
		r, err := s.Read()
		if err != nil {
			panic(err)
		}

		if r == nil {
			panic("r is nil?")
		}

		st.Put(r.Data)

		fmt.Printf("Record %v\n", *r.SequenceNumber)
		select {
		case <-sigs:
			st.Close()
			os.Exit(0)
		default:
			continue
		}
	}
}
