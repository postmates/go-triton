package main

import (
	"fmt"
	"os"

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

	bucketName := "com.postmates.triton_dev"

	st := triton.NewS3Store(sc, bucketName)

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
	}
}
