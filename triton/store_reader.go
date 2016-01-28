package triton

import (
	"fmt"
	"io"
	"log"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/tinylib/msgp/msgp"
)

func listDatesFromRange(start, end time.Time) (dates []time.Time) {
	dates = make([]time.Time, 0, 2)
	current := start
	day, _ := time.ParseDuration("24h")

	if start.After(end) {
		panic("invalid date range")
	}

	dates = append(dates, current)
	for !current.Equal(end) {
		current = current.Add(day)
		dates = append(dates, current)
	}

	return
}

func NewStoreReader(svc S3Service, bucketName, clientName, streamName string, startDate, endDate time.Time) (Reader, error) {
	allDates := listDatesFromRange(startDate, endDate)
	archives := make(storeArchiveList, 0, len(allDates))

	for _, date := range allDates {
		dateStr := date.Format("20060102")
		prefix := fmt.Sprintf("%s/%s-", dateStr, streamName)
		if clientName != "" {
			prefix = fmt.Sprintf("%s%s-", prefix, clientName)
		}
		resp, err := svc.ListObjects(&s3.ListObjectsInput{
			Bucket: aws.String(bucketName),
			Prefix: aws.String(prefix),
		})

		if err != nil {
			return nil, err
		}

		for _, o := range resp.Contents {
			log.Println("Opening store archive", *o.Key)
			sa, err := newStoreArchive(bucketName, *o.Key, svc)
			if err != nil {
				log.Println("Failed to parse contents", *o.Key, err)
				continue
			}

			archives = append(archives, sa)
		}

		// TODO: Would be nice to handle this, limit is 1000
		if *resp.IsTruncated {
			log.Println("WARNING: truncated s3 response")
		}
	}

	foundClientName := clientName
	for _, a := range archives {
		if foundClientName == "" {
			foundClientName = a.clientName
		}

		if foundClientName != a.clientName {
			return nil, fmt.Errorf("Multiple clients found: %s and %s", foundClientName, a.clientName)
		}
	}

	sort.Sort(archives)

	// cast to io.Readers
	readers := make([]io.Reader, len(archives))
	for idx, archive := range archives {
		readers[idx] = io.Reader(archive)
	}

	return &storeReader{
		reader: io.MultiReader(readers...),
	}, nil
}

type storeReader struct {
	reader io.Reader
}

func (sr *storeReader) Read(p []Record) (int, error) {
	r := msgp.NewReader(sr.reader)
	for i := 0; i < len(p); i++ {
		if err := r.ReadMapStrIntf(p[i]); err != nil {
			return i, err
		}
	}
	return len(p), nil
}
