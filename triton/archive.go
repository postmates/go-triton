package triton

import (
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

// Sortable list of store archives.
//
// Though archives, when they come out of S3 are lexigraphically sorted, we
// want to just be sure that we're really handling our dates and times
// correctly.
type storeArchiveList []*storeArchive

func (l storeArchiveList) Len() int      { return len(l) }
func (l storeArchiveList) Swap(i, j int) { l[i], l[j] = l[j], l[i] }
func (l storeArchiveList) Less(i, j int) bool {
	if l[i].t != l[j].t {
		return l[i].t.Before(l[j].t)
	}
	return l[i].sortValue < l[j].sortValue
}

// storeArchive represents an instance of a data file stored, usually, in S3.
type storeArchive struct {
	streamName string
	bucket     string
	key        string
	clientName string

	t         time.Time
	sortValue int

	s3Svc S3Service
	file  io.ReadCloser
}

func (sa *storeArchive) Read(p []byte) (int, error) {
	// get file on first read
	if sa.file == nil {
		out, err := sa.s3Svc.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(sa.bucket),
			Key:    aws.String(sa.key),
		})

		if err != nil {
			return 0, err
		}

		sa.file = out.Body
	}

	return sa.file.Read(p)
}

func (sa *storeArchive) parseKeyName(keyName string) error {
	re := regexp.MustCompile(`(?P<day>\d{8})\/(?P<stream>.+)\-(?P<ts>\d+)\.tri$`)
	res := re.FindAllStringSubmatch(keyName, -1)

	if len(res) != 1 {
		return fmt.Errorf("Invalid key name")
	}

	var err error
	sa.t, err = time.Parse("20060102", res[0][1])
	if err != nil {
		return err
	}

	n, err := fmt.Sscanf(res[0][3], "%d", &sa.sortValue)
	if n != 1 {
		return fmt.Errorf("Failed to parse sort value")
	}

	nameParts := strings.Split(res[0][2], "-")
	if len(nameParts) != 2 {
		return fmt.Errorf("Failure parsing stream name: %v", res[0][2])
	}
	sa.streamName = nameParts[0]
	sa.clientName = nameParts[1]

	return nil
}

func newStoreArchive(bucketName, keyName string, svc S3Service) (*storeArchive, error) {
	archive := &storeArchive{
		bucket: bucketName,
		key:    keyName,
		s3Svc:  svc,
	}

	err := archive.parseKeyName(keyName)
	if err != nil {
		return archive, err
	}

	return archive, nil
}
