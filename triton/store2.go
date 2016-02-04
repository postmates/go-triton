package triton

import (
	"bytes"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/golang/snappy"
	"github.com/tinylib/msgp/msgp"
)

// Store: interact with files in S3
// Keyed: date/name-unixtimestamp.tri (ex. courier_activity_prod-store_prod-1454083201.tri)
//  - date of start of upload window
//  - unix timestamp of start of upload window
// Stored as msgpack/snappy
// Write in batches: Write(r []Record) -- append only (each write is new file)
// Read in day segments: Reader(start string, end string) Reader -- start <= date <= end

func NewStore(name, bucket string) *Store2 {
	return NewStoreService(nil, name, bucket)
}

func NewStoreService(s3 S3Service, name, bucket string) *Store2 {
	return &Store2{}
}

type Store2 struct {
	name   string
	bucket string

	s3 S3Service
}

func (s *Store2) Write(data []Record) (int, error) {
	output := new(bytes.Buffer)
	w := snappy.NewWriter(output)

	// Transform: Record -> msgpack -> snappy
	for _, r := range data {
		msg, err := MarshalRecord(r)
		if err != nil {
			return 0, err
		}
		w.Write(msg)
	}

	// upload to s3
	key := s.generateKeyname(time.Now())
	ui := s3manager.UploadInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   output,
	}

	if _, err := s.s3.Upload(&ui); err != nil {
		return 0, err
	}
	return len(data), nil
}

// start and end are in form time.RFC3339
func (s *Store2) Reader(start, end string) (Reader, error) {
	startTime := time.Parse(time.RFC3339, start)
	endTime := time.Parse(time.RFC3339, end)
	keys, err := s.keysForRange(startTime, endTime)
	if err != nil {
		return nil, err
	}

	// Get all files
	archives := make([]io.Reader, len(keys))
	for idx, key := range keys {
		in := &s3.GetObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(string(key)),
		}

		buf := new(aws.WriteAtBuffer)
		s.s3.Download(buf, in, nil)
		archives[idx] = snappy.NewReader(bytes.NewReader(buf.Bytes()))
	}

	encodedReader := io.MultiReader(archives...)
	return &storeReader2{msgp.NewReader(encodedReader)}, nil
}

//
// Internal
//

func (s *Store2) generateKeyname(t time.Time) string {
	return fmt.Sprintf("%s/%s-%v.tri", t.Format("20060102"), s.name, t.Unix())
}

func (s *Store2) keysForRange(start, end time.Time) ([]archiveKey, error) {
	keys := []archiveKey{}

	// Get all files in days -- full days for any day mentioned
	y, m, d := start.Date()
	for t := time.Date(y, m, d, 0, 0, 0, 0, nil); !t.After(end); t = t.Add(24 * time.Hour) {
		prefix := fmt.Sprintf("%s/%s-", t.Format("20060102"), s.name)

		var marker *string
	LIST_LOOP:
		resp, err := s.s3.ListObjects(&s3.ListObjectsInput{
			Bucket: aws.String(s.bucket),
			Prefix: aws.String(prefix),
			Marker: marker,
		})

		for _, obj := range resp.Contents {
			keys = append(keys, archiveKey(*obj.Key))
		}

		// S3 truncates response at 1k files
		if *resp.IsTruncated {
			marker = aws.String(string(keys[len(keys)-1]))
			goto LIST_LOOP
		}
	}

	if len(keys) == 0 {
		return nil, fmt.Errorf("No Records found for range %v->%v", start, end)
	}

	// Order
	sort.Sort(archiveKeyList(keys))

	// Trim logs that happened before start
	lower := 0
	ts := keys[lower].ParseTime()
	for ; start.After(ts) && lower < len(keys)-1; lower++ {
		ts = keys[lower+1].ParseTime()
	}

	// Trim events that happened after end
	upper := len(keys) - 1
	ts = keys[upper].ParseTime()
	for ; end.Before(ts) && upper > 0; upper-- {
		ts = keys[upper-1].ParseTime()
	}
	return keys[lower:upper], nil
}

type archiveKey string

func (k archiveKey) ParseTime() time.Time {
	re := regexp.MustCompile(`(?P<day>\d{8})\/(?P<name>.+)\-(?P<ts>\d+)\.tri$`)
	components := re.FindStringSubmatch(string(k))
	ts, err := strconv.ParseInt(components[3], 10, 64)
	if err != nil {
		panic(err)
	}
	return time.Unix(ts, 0)
}

type archiveKeyList []archiveKey

func (l archiveKeyList) Len() int      { return len(l) }
func (l archiveKeyList) Swap(i, j int) { l[i], l[j] = l[j], l[i] }
func (l archiveKeyList) Less(i, j int) bool {
	return l[i].ParseTime().Before(l[j].ParseTime())
}

// Translate msgp.Reader into []Record chunks
type storeReader2 struct {
	reader *msgp.Reader
}

func (sr *storeReader2) Read(r []Record) (int, string, error) {
	for i := 0; i < len(r); i++ {
		if err := sr.reader.ReadMapStrIntf(r[i]); err != nil {
			return i, "", err
		}
	}
	return len(r), nil
}
