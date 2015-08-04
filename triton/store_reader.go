package triton

import (
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
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
		dates = append(dates, current)
		current = current.Add(day)
	}

	return
}

func NewStoreReader(svc S3Service, bucketName, streamName string, startDate, endDate time.Time) (Reader, error) {
	allDates := listDatesFromRange(startDate, endDate)
	readers := make([]Reader, 0, len(allDates))

	for _, date := range allDates {
		dateStr := date.Format("20060102")
		prefix := fmt.Sprintf("%s/%s-", dateStr, streamName)
		resp, err := svc.ListObjects(&s3.ListObjectsInput{
			Bucket: aws.String(bucketName),
			Prefix: aws.String(prefix),
		})

		if err != nil {
			return nil, err
		}

		// TODO: sorting, archive vs. shard
		for _, o := range resp.Contents {
			sa, err := NewStoreArchive(bucketName, *o.Key, svc)
			if err != nil {
				log.Println("Failed to parse contents", *o.Key)
				continue
			}

			readers = append(readers, &sa)
		}

		// TODO: Would be nice to handle this, limit is 1000
		if *resp.IsTruncated {
			log.Println("WARNING: truncated s3 response")
		}
	}

	return NewSerialReader(readers), nil
}
