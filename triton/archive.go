package triton

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3"
	"regexp"
	"time"
)

type StoreArchive struct {
	StreamName string
	Bucket     string
	Key        string
	Shard      string

	T         time.Time
	SortValue int

	s3_srv *s3.S3
}

func (sa *StoreArchive) Open() (r *Reader, err error) {
	return nil, nil
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

func NewStoreArchive(bucketName, keyName string, s3_srv *s3.S3) (sa StoreArchive, err error) {
	sa.Bucket = bucketName
	sa.Key = keyName

	err = sa.parseKeyName(keyName)
	if err != nil {
		return sa, err
	}

	return sa, nil
}
