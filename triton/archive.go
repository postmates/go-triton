package triton

import (
	"fmt"
	"log"
	"regexp"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	//"github.com/aws/aws-sdk-go/aws/awsutil"
	"github.com/aws/aws-sdk-go/service/s3"
)

type StoreArchive struct {
	StreamName string
	Bucket     string
	Key        string
	Shard      string

	T         time.Time
	SortValue int

	s3Svc S3Service
}

func (sa *StoreArchive) Open() (r *Reader, err error) {
	out, err := sa.s3Svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(sa.Bucket),
		Key:    aws.String(sa.Key),
	})

	if err != nil {
		return nil, err
	}

	r = NewReader(out.Body)

	return
}

func (sa *StoreArchive) parseKeyName(keyName string) (err error) {
	re := regexp.MustCompile(`(?P<day>\d{8})\/(?P<stream>.+)\-(?P<ts>\d+)\.tri$`)
	res := re.FindAllStringSubmatch(keyName, -1)

	if len(res) != 1 {
		return fmt.Errorf("Invalid key name")
	}

	sa.T, err = time.Parse("20060102", res[0][1])

	n, err := fmt.Sscanf(res[0][3], "%d", &sa.SortValue)
	if n != 1 {
		return fmt.Errorf("Failed to parse sort value")
	}

	nameRe := regexp.MustCompile(`(.+)-(archive|shardId-\d+)`)
	nameRes := nameRe.FindAllStringSubmatch(res[0][2], -1)
	if len(nameRes) != 1 {
		return fmt.Errorf("Failure parsing stream name: %v", nameRes)
	}
	if nameRes[0][2] != "archive" {
		sa.Shard = nameRes[0][2]
	}

	sa.StreamName = nameRes[0][1]

	return
}

func NewStoreArchive(bucketName, keyName string, svc S3Service) (sa StoreArchive, err error) {
	sa.Bucket = bucketName
	sa.Key = keyName
	sa.s3Svc = svc

	err = sa.parseKeyName(keyName)
	if err != nil {
		return sa, err
	}

	return sa, nil
}

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

func ListArchive(bucketName, streamName string, startDate, endDate time.Time, s3Svc S3Service) ([]StoreArchive, error) {
	allDates := listDatesFromRange(startDate, endDate)
	saList := make([]StoreArchive, 0, len(allDates))

	for _, date := range allDates {
		dateStr := date.Format("20060102")
		prefix := fmt.Sprintf("%s/%s-", dateStr, streamName)
		resp, err := s3Svc.ListObjects(&s3.ListObjectsInput{
			Bucket: aws.String(bucketName),
			Prefix: aws.String(prefix),
		})

		if err != nil {
			return nil, err
		}

		// TODO: sorting, archive vs. shard
		for _, o := range resp.Contents {
			sa, err := NewStoreArchive(bucketName, *o.Key, s3Svc)
			if err != nil {
				log.Println("Failed to parse contents", *o.Key)
				continue
			}

			saList = append(saList, sa)
		}

		// TODO: Would be nice to handle this, limit is 1000
		if *resp.IsTruncated {
			log.Println("WARNING: truncated s3 response")
		}
	}

	return saList, nil
}
