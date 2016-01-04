package triton

import (
	"fmt"
	"log"
	"sort"
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
		current = current.Add(day)
		dates = append(dates, current)
	}

	return
}

// Sortable list of store archives.
//
// Though archives, when they come out of S3 are lexigraphically sorted, we
// want to just be sure that we're really handling our dates and times
// correctly.
type StoreArchiveList []StoreArchive

func (l StoreArchiveList) Len() int {
	return len(l)
}

func (l StoreArchiveList) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}

func (l StoreArchiveList) Less(i, j int) bool {
	return l[i].T.Before(l[j].T)
}

func NewStoreReader(svc S3Service, bucketName, clientName, streamName string, startDate, endDate time.Time) (result Reader, err error) {
	allDates := listDatesFromRange(startDate, endDate)
	archives := make(StoreArchiveList, 0, len(allDates))

	for _, date := range allDates {
		dateStr := date.Format("20060102")
		prefix := fmt.Sprintf("%s/%s-", dateStr, streamName)
		if clientName != "" {
			prefix = fmt.Sprintf("%s%s-", prefix, clientName)
		}
		var resp *s3.ListObjectsOutput
		resp, err = svc.ListObjects(&s3.ListObjectsInput{
			Bucket: aws.String(bucketName),
			Prefix: aws.String(prefix),
		})

		if err != nil {
			return
		}

		for _, o := range resp.Contents {
			log.Println("Opening store archive", *o.Key)
			sa, err := NewStoreArchive(bucketName, *o.Key, svc)
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
			foundClientName = a.ClientName
		}

		if foundClientName != a.ClientName {
			err = fmt.Errorf("Multiple clients found: %s and %s", foundClientName, a.ClientName)
			return
		}
	}

	sort.Sort(archives)

	// Convert to a list of Readers... feels like there should be a better way
	// here. Is this what generics are for? Or is there an interface for a list?
	readers := make([]Reader, 0, len(archives))
	for i := range archives {
		readers[i] = &archives[i]
	}
	result = NewSerialReader(readers)
	return
}
