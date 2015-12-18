package triton

import (
	"bytes"
	"github.com/golang/snappy"
	"github.com/tinylib/msgp/msgp"
	"io"
	"io/ioutil"
	"log"
	"testing"
	"time"
)

type recordEncoder struct {
	out *msgp.Writer
}

func newRecordEncoder(w io.Writer) *recordEncoder {
	return &recordEncoder{
		out: msgp.NewWriter(snappy.NewWriter(w)),
	}
}

func (e *recordEncoder) Encode(r Record) (err error) {
	if err = e.out.WriteMapStrIntf(map[string]interface{}(r)); err != nil {
		return
	}
	err = e.out.Flush()
	return
}

func TestTailStream(t *testing.T) {
	s3Service := newTestS3Service()
	s3UploaderService := s3Service.uploader()
	kinesisService := newTestKinesisService()
	stream := "test_stream"
	bucket := "testbucket"
	tailTime := time.Now()
	ar := NewArchiveRepository(s3Service, s3UploaderService, bucket, stream, "archive")
	rec1 := NewRecord()
	rec1["key"] = "1"
	rec2 := NewRecord()
	rec2["key"] = "2"
	rec3 := NewRecord()
	rec3["key"] = "3"

	// prepare s3

	// send day1
	recBuf := bytes.Buffer{}
	newRecordEncoder(&recBuf).Encode(rec1)
	metadata := NewStreamMetadata()
	metadata.noteSequenceNumber(ShardID("A"), SequenceNumber("1"))
	ar.Upload(tailTime.AddDate(0, 0, -1), ioutil.NopCloser(&recBuf), metadata)

	// send day2
	recBuf = bytes.Buffer{}
	newRecordEncoder(&recBuf).Encode(rec2)
	metadata = NewStreamMetadata()
	metadata.noteSequenceNumber(ShardID("A"), SequenceNumber("2"))
	ar.Upload(tailTime.Add(time.Hour), ioutil.NopCloser(&recBuf), metadata)
	log.Println("uploaded records")

	// put day3 record on kinesis:
	st := newTestKinesisStream("test_stream")
	s1 := newTestKinesisShard()
	s1.AddRecord(SequenceNumber("3"), rec3)
	st.AddShard(ShardID("A"), s1)
	kinesisService.AddStream(st)

	// run the tail:
	tailAt := NewTailAt(&NewTailAtParams{
		S3Service:      s3Service,
		KinesisService: kinesisService,
		Bucket:         bucket,
		StreamName:     stream,
		At:             tailTime,
	})
	log.Println("loading record")
	record, err := tailAt.Next()
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	if record["key"] != "2" {
		t.Fatalf("unexpected record: %s", record["key"])
	}

	record, err = tailAt.Next()
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	if record["key"] != "3" {
		t.Fatalf("unexpected record: %s", record["key"])
	}
}
