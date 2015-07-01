package triton

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/golang/snappy/snappy"
	//"github.com/aws/aws-sdk-go/aws"
	//"github.com/aws/aws-sdk-go/aws/awserr"
	//"github.com/aws/aws-sdk-go/aws/awsutil"
	//"github.com/aws/aws-sdk-go/service/s3"
	//"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type Store interface {
	PutRaw(*[]byte) error
	Close() error
}

type S3Store struct {
	streamName string
	//uploader        S3UploaderService
	currentWriter   io.WriteCloser
	currentFilename *string

	buf *bytes.Buffer
}

func (s *S3Store) closeWriter() {
	// TODO: Need to deal with this errors... we could have an out of disk
	// space, for example.
	if s.currentWriter != nil {
		err := s.flushBuffer()
		if err != nil {
			panic(err)
		}

		err = s.currentWriter.Close()
		if err != nil {
			panic(err)
		}

		s.currentWriter = nil
	}
}

func (s *S3Store) openWriter(fname string) (err error) {
	if s.currentWriter != nil {
		return fmt.Errorf("Existing writer still open")
	}

	f, err := os.Create(fname)
	if err != nil {
		return err
	}

	s.currentFilename = &fname
	s.currentWriter = f

	return
}

func (s *S3Store) generateFilename(t time.Time) (fname string) {
	ds := t.Format("2006010215")
	fname = fmt.Sprintf("%s-%s.tri", s.streamName, ds)

	return
}

func (s *S3Store) getCurrentWriter() (w io.Writer, err error) {
	fn := s.generateFilename(time.Now())

	if s.currentFilename != nil && *s.currentFilename != fn {
		s.closeWriter()
	}

	if s.currentWriter == nil {
		err := s.openWriter(fn)
		if err != nil {
			return nil, err
		}
	}

	return s.currentWriter, nil
}

func (s *S3Store) flushBuffer() (err error) {

	if s.currentWriter == nil {
		return fmt.Errorf("Flush without a current buffer")
	}

	sw := snappy.NewWriter(s.currentWriter)
	_, err = s.buf.WriteTo(sw)
	if err != nil {
		return
	}

	s.buf.Reset()
	return
}

func (s *S3Store) Put(b []byte) (err error) {
	// We get the current writer here, even though we're not using it directly.
	// This might trigger a log rotation and flush based on time.
	_, err = s.getCurrentWriter()
	if err != nil {
		return
	}

	if s.buf.Len()+len(b) >= BUFFER_SIZE {
		s.flushBuffer()
	}

	s.buf.Write(b)

	return
}

func (s *S3Store) Close() (err error) {
	s.closeWriter()
	return nil
}

const BUFFER_SIZE int = 1024 * 1024

func NewS3Store(sc *StreamConfig, bucketName string) (s *S3Store) {
	/*
		uo := &s3manager.UploadOptions{S3: svc}
		u := s3manager.NewUploader(uo)
	*/

	b := make([]byte, 0, BUFFER_SIZE)
	buf := bytes.NewBuffer(b)

	s = &S3Store{
		streamName: sc.StreamName,
		buf:        buf,
		//uploader:   u,
	}

	return
}
