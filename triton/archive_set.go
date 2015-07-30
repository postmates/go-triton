package triton

import (
	"fmt"
	"io"
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

type ArchiveSet struct {
	saSet         []StoreArchive
	currentReader *Reader
}

func (as *ArchiveSet) openNext() {
	if len(as.saSet) > 0 {
		nr, err := as.saSet[0].Open()
		if err != nil {
			// TODO: This coudl be handled, but we need to open the next one
			panic(err)
		}
		as.currentReader = nr
		as.saSet = as.saSet[1:]
	}
}

func (as *ArchiveSet) ReadRecord() (rec map[string]interface{}, err error) {
	for {
		if as.currentReader == nil {
			as.openNext()
		}

		// Nothing to open
		if as.currentReader == nil {
			return nil, io.EOF
		}

		rec, err := as.currentReader.ReadRecord()
		if err != nil {
			if err == io.EOF {
				as.currentReader = nil
			}
		} else {
			return rec, nil
		}
	}
}

func NewArchiveSet(bucketName, streamName string, startDate, endDate time.Time, s3Svc S3Service) (*ArchiveSet, error) {
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

	return &ArchiveSet{saList, nil}, nil
}
