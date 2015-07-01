package triton

import (
	"fmt"
	"io"
	"os"
	"time"

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
}

func (s *S3Store) closeWriter() {
	if s.currentWriter != nil {
		err := s.currentWriter.Close()
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

	s.currentWriter = f
	s.currentFilename = &fname
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

func (s *S3Store) PutRaw(b []byte) (err error) {
	w, err := s.getCurrentWriter()
	if err != nil {
		return
	}

	_, err = w.Write(b)

	return
}

func (s *S3Store) Close() (err error) {
	return nil
}

func NewS3Store(sc *StreamConfig, bucketName string) (s *S3Store) {
	/*
		uo := &s3manager.UploadOptions{S3: svc}
		u := s3manager.NewUploader(uo)
	*/

	s = &S3Store{
		streamName: sc.StreamName,
		//uploader:   u,
	}

	return
}
