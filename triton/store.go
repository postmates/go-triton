package triton

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/golang/snappy/snappy"
)

type CheckpointService interface {
	Checkpoint(string) error
}

// A store manages buffering records together into files, and uploading them somewhere.
type Store struct {
	streamName string
	shardID    string

	// Our uploaders manages sending our datafiles somewhere
	uploader *S3Uploader

	// A Checkpointer stores what records have be committed to our uploader
	checkpointer CheckpointService

	currentLogTime     time.Time
	currentWriter      io.WriteCloser
	currentFilename    *string
	lastSequenceNumber string

	buf *bytes.Buffer
}

func (s *Store) closeWriter() error {
	if s.currentWriter != nil {
		log.Println("Closing file", *s.currentFilename)
		err := s.flushBuffer()
		if err != nil {
			log.Println("Failed to flush", err)
			return fmt.Errorf("Failed to close writer")
		}

		err = s.currentWriter.Close()
		if err != nil {
			log.Println("Failed to close", err)
			return fmt.Errorf("Failed to close writer")
		}
		s.currentWriter = nil

		if s.uploader != nil {
			err = s.uploader.Upload(*s.currentFilename, s.generateKeyname())
			if err != nil {
				log.Println("Failed to upload:", err)
				return fmt.Errorf("Failed to upload")
			}

			err = os.Remove(*s.currentFilename)
			if err != nil {
				log.Println("Failed to cleanup:", err)
				return fmt.Errorf("Failed to cleanup writer")
			}
		}

		// Now that we've successfully uploaded the data, we can checkpoint
		// this write.
		if s.checkpointer != nil {
			if len(s.lastSequenceNumber) > 0 {
				err = s.checkpointer.Checkpoint(s.lastSequenceNumber)
				if err != nil {
					log.Println("Failed to checkpoint:", err)
					return fmt.Errorf("Failed to checkpoint store")
				}
			} else {
				log.Println("Empty checkpoint")
			}
		}

		s.lastSequenceNumber = ""
	}

	return nil
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
	s.currentLogTime = time.Now()

	return
}

func (s *Store) generateFilename() (name string) {
	name = fmt.Sprintf("%s-%s.tri", s.streamName, s.shardID)

	return
}

func (s *Store) generateKeyname() (name string) {
	day_s := s.currentLogTime.Format("20060102")
	ts_s := fmt.Sprintf("%d", s.currentLogTime.Unix())

	name = fmt.Sprintf("%s/%s-%s-%s.tri", day_s, s.streamName, s.shardID, ts_s)

	return
}

func (s *Store) getCurrentWriter() (w io.Writer, err error) {
	if s.currentWriter != nil {
		// Rotate by the hour
		if s.currentLogTime.Hour() != time.Now().Hour() {
			err = s.closeWriter()
			if err != nil {
				return nil, err
			}
		}
	}

	if s.currentWriter == nil {
		err := s.openWriter(s.generateFilename())
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

func (s *Store) PutRecord(r *kinesis.Record) (err error) {
	err = s.Put(r.Data)
	if err == nil {
		s.lastSequenceNumber = *r.SequenceNumber
	}
	return
}

func (s *Store) Close() (err error) {
	err = s.closeWriter()
	return
}

const BUFFER_SIZE int = 1024 * 1024

func NewStore(streamName, shardID string, up *S3Uploader, checkpointer CheckpointService) (s *Store) {
	b := make([]byte, 0, BUFFER_SIZE)
	buf := bytes.NewBuffer(b)

	s = &Store{
		streamName:   streamName,
		shardID:      shardID,
		buf:          buf,
		uploader:     up,
		checkpointer: checkpointer,
	}

	return
}
