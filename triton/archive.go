package triton

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

// A StoreArchive represents an instance of a data file stored, usually, in S3.
type StoreArchive struct {
	StreamName string
	Bucket     string
	Key        string
	ClientName string

	T         time.Time
	SortValue int

	s3Svc S3Service
	rdr   Reader
}

func (sa *StoreArchive) ReadRecord() (rec Record, err error) {
	if sa.rdr == nil {
		out, err := sa.s3Svc.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(sa.Bucket),
			Key:    aws.String(sa.Key),
		})

		if err != nil {
			return nil, err
		}

		sa.rdr = NewArchiveReader(out.Body)
	}

	rec, err = sa.rdr.ReadRecord()
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

	nameParts := strings.Split(res[0][2], "-")
	if len(nameParts) != 2 {
		return fmt.Errorf("Failure parsing stream name: %v", res[0][2])
	}
	sa.StreamName = nameParts[0]
	sa.ClientName = nameParts[1]

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
