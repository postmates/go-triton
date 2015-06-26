package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/awsutil"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/golang/snappy/snappy"
	"github.com/postmates/postal-go-triton/triton"
	"gopkg.in/vmihailenco/msgpack.v2"
)

type CourierLocation struct {
	CourierId uint64  `msgpack:"courier_id"`
	Lat       float64 `msgpack:"lat"`
	Lng       float64 `msgpack:"lng"`
}

func main1() {
	inFile, err := os.Open("courier_location.mp")
	if err != nil {
		log.Fatal(err)
	}

	defer inFile.Close()

	dec := msgpack.NewDecoder(inFile)

	outFile, err := os.Create("courier_location.szm")
	if err != nil {
		log.Fatal(err)
	}

	outWriter := snappy.NewWriter(outFile)
	defer outFile.Close()

	//var v map[string]interface{}
	var v CourierLocation

	b := make([]byte, 0, 1024*1024)
	buf := bytes.NewBuffer(b)

	cc := 0
	rc := 0
	tc := 0
	for {
		err = dec.Decode(&v)
		if err != nil {
			if err == io.EOF {
				break
			}

			fmt.Printf("%v", err)
			break
		}

		b, err := msgpack.Marshal(v)
		if err != nil {
			log.Println("Failed to encode %v: %v", v, err)
		}

		buf.Write(b)

		tc += 1
		rc += 1
		if rc >= 100 {
			cc += 1
			outWriter.Write(buf.Bytes())
			buf.Reset()
			rc = 0
		}
	}

	if buf.Len() > 0 {
		cc += 1
		outWriter.Write(buf.Bytes())
		buf.Reset()
	}

	fmt.Println("Wrote", tc, "records in", cc, "chunks")

}

func main2() {
	inFile, err := os.Open("courier_location.mpz")
	if err != nil {
		log.Fatal(err)
	}

	defer inFile.Close()

	dec := snappy.NewReader(inFile)

	//buf := &bytes.Buffer{}
	buf := make([]byte, 1024*1024)
	for {
		n, err := dec.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal("Failed to read: ", err)
		}

		if n == 0 {
			log.Println("Read 0?")
			continue
		}

		continue

		var v CourierLocation
		err = msgpack.Unmarshal(buf, &v)
		if err != nil {
			log.Fatal("Failed to unmarshal: ", err, n)
		}

		fmt.Printf("(%f, %f)\n", v.Lat, v.Lng)
	}
}

func main3() {
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

	sc, err := c.ConfigForName("test")
	if err != nil {
		panic(err)
	}

	svc := kinesis.New(&aws.Config{Region: sc.RegionName})

	resp, err := svc.DescribeStream(&kinesis.DescribeStreamInput{StreamName: &sc.StreamName})
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == "ResourceNotFoundException" {
				fmt.Printf("Failed to find stream: %v\n", awsErr.Message())
				return
			}
		}

		panic(err)
	}
	fmt.Println(awsutil.StringValue(resp))

}

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

	for {
		r, err := s.Read()
		if err != nil {
			panic(err)
		}

		fmt.Printf("Record %v\n", *r.SequenceNumber)
	}
}
