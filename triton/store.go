package triton

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/golang/snappy"
	"github.com/tinylib/msgp/msgp"
	"io"
	"log"
	"os"
	"time"
)

// A store manages buffering records together into files, and uploading them somewhere.
type Store struct {
	name           string
	reader         StreamReader
	streamMetadata *StreamMetadata

	// Our uploaders manages sending our datafiles somewhere
	uploader        *S3Uploader
	currentLogTime  time.Time
	currentWriter   io.WriteCloser
	currentFilename *string
	buf             *bytes.Buffer
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
			keyName := s.generateKeyname()
			err = s.uploader.Upload(*s.currentFilename, keyName)
			if err != nil {
				log.Println("Failed to upload:", err)
				return fmt.Errorf("Failed to upload")
			}

			err = os.Remove(*s.currentFilename)
			if err != nil {
				log.Println("Failed to cleanup:", err)
				return fmt.Errorf("Failed to cleanup writer")
			}
			err = s.uploadMetadata(keyName)
			if err != nil {
				return fmt.Errorf("failed to upload metadata: %s", err.Error())
			}
		}

		s.currentFilename = nil
		s.reader.Checkpoint()
	}
	s.streamMetadata = NewStreamMetadata()

	return nil
}

func (s *Store) uploadMetadata(keyName string) (err error) {
	// upload the metadata
	var metadataBuf bytes.Buffer
	err = json.NewEncoder(&metadataBuf).Encode(&s.streamMetadata)
	if err != nil {
		err = fmt.Errorf("failed to upload metadata: %s", err.Error())
		return
	}
	s.uploader.UploadData(&metadataBuf, keyName+".metadata")
	return
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
	name = fmt.Sprintf("%s.tri", s.name)

	return
}

func (s *Store) generateKeyname() (name string) {
	return ArchiveKey{Time: s.currentLogTime, Stream: s.name}.Path()
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

func (s *Store) PutRecord(rec map[string]interface{}) (err error) {
	// TODO: Looks re-usable
	b := make([]byte, 0, 1024)
	b, err = msgp.AppendMapStrIntf(b, rec)
	if err != nil {
		return
	}

	err = s.Put(b)
	return
}

func (s *Store) Put(b []byte) (err error) {
	// We get the current writer here, even though we're not using it directly.
	// This might trigger a log rotation and flush based on time.
	_, err = s.getCurrentWriter()
	if err != nil {
		return
	}

	if s.buf.Len()+len(b) >= bufferSize {
		s.flushBuffer()
	}

	s.buf.Write(b)

	return
}

func (s *Store) Close() (err error) {
	err = s.closeWriter()
	return
}

func (s *Store) Store() (err error) {
	for {
		var shardRec *ShardRecord
		shardRec, err = s.reader.ReadShardRecord()
		if err != nil {
			if err == io.EOF {
				err = nil
			}
		}
		s.streamMetadata.noteSequenceNumber(shardRec.ShardID, shardRec.SequenceNumber)
		err = s.PutRecord(shardRec.Record)
		if err != nil {
			return
		}
	}
	return
}

const bufferSize int = 1024 * 1024

func NewStore(name string, r StreamReader, up *S3Uploader) (s *Store) {
	b := make([]byte, 0, bufferSize)
	buf := bytes.NewBuffer(b)

	s = &Store{
		name:           name,
		reader:         r,
		buf:            buf,
		uploader:       up,
		streamMetadata: NewStreamMetadata(),
	}

	return
}
