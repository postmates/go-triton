package triton

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/golang/snappy/snappy"
)

type Store struct {
	streamName      string
	shard           string
	uploader        *S3Uploader
	currentWriter   io.WriteCloser
	currentFilename *string

	buf *bytes.Buffer
}

func (s *Store) closeWriter() {
	// TODO: Need to deal with this errors... we could have an out of disk
	// space, for example.
	if s.currentWriter != nil {
		log.Println("Closing file", *s.currentFilename)
		err := s.flushBuffer()
		if err != nil {
			panic(err)
		}

		err = s.currentWriter.Close()
		if err != nil {
			panic(err)
		}

		if s.uploader != nil {
			err = s.uploader.Upload(*s.currentFilename)
			if err != nil {
				log.Panicln("Failed to upload", s.currentFilename)
			}

			err = os.Remove(*s.currentFilename)
			if err != nil {
				log.Panicln("Failed to cleanup", s.currentFilename)
			}
		}

		s.currentWriter = nil
	}
}

func (s *Store) openWriter(fname string) (err error) {
	if s.currentWriter != nil {
		return fmt.Errorf("Existing writer still open")
	}

	log.Println("Opening file", fname)
	f, err := os.Create(fname)
	if err != nil {
		return err
	}

	s.currentFilename = &fname
	s.currentWriter = f

	return
}

func (s *Store) generateFilename(t time.Time) (fname string) {
	ds := t.Format("2006010215")
	fname = fmt.Sprintf("%s-%s-%s.tri", s.streamName, s.shard, ds)

	return
}

func (s *Store) getCurrentWriter() (w io.Writer, err error) {
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

func (s *Store) flushBuffer() (err error) {

	if s.currentWriter == nil {
		return fmt.Errorf("Flush without a current buffer")
	}

	log.Printf("Flushing updates for %s to disk\n", *s.currentFilename)

	sw := snappy.NewWriter(s.currentWriter)
	_, err = s.buf.WriteTo(sw)
	if err != nil {
		return
	}

	s.buf.Reset()
	return
}

func (s *Store) Put(b []byte) (err error) {
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

func (s *Store) Close() (err error) {
	s.closeWriter()
	return nil
}

const BUFFER_SIZE int = 1024 * 1024

func NewStore(sc *StreamConfig, shard string, up *S3Uploader) (s *Store) {
	b := make([]byte, 0, BUFFER_SIZE)
	buf := bytes.NewBuffer(b)

	s = &Store{
		streamName: sc.StreamName,
		shard:      shard,
		buf:        buf,
		uploader:   up,
	}

	return
}
