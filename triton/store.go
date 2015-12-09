package triton

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/golang/snappy"
	"github.com/skarademir/naturalsort"
	"github.com/tinylib/msgp/msgp"
	"io"
	"log"
	"os"
	"sort"
	"time"
)

type CheckpointService interface {
	Checkpoint(string) error
}

// A store manages buffering records together into files, and uploading them somewhere.
type Store struct {
	name           string
	reader         StreamReader
	streamMetadata *streamMetadata

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
	s.streamMetadata = newStreamMetadata()

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
	s.uploader.UploadBuf(&metadataBuf, keyName+".metadata")
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
	day_s := s.currentLogTime.Format("20060102")
	ts_s := fmt.Sprintf("%d", s.currentLogTime.Unix())

	name = fmt.Sprintf("%s/%s-%s.tri", day_s, s.name, ts_s)

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

	if s.buf.Len()+len(b) >= BUFFER_SIZE {
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
		// TODO: We're unmarshalling and then marshalling msgpack here when
		// there is not real reason except that's a more useful general
		// interface.  We should add another that is ReadRaw
		rec, err := s.reader.ReadRecord()
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return err
			}
		}

		err = s.PutRecord(rec)
		if err != nil {
			return err
		}
	}

	return nil
}

const BUFFER_SIZE int = 1024 * 1024

func NewStore(name string, r StreamReader, up *S3Uploader) (s *Store) {
	b := make([]byte, 0, BUFFER_SIZE)
	buf := bytes.NewBuffer(b)

	s = &Store{
		name:           name,
		reader:         r,
		buf:            buf,
		uploader:       up,
		streamMetadata: newStreamMetadata(),
	}

	return
}

type streamMetadata struct {
	// shard ID => shardInfo
	Shards map[string]*shardInfo `json:"shards"`
}

func newStreamMetadata() *streamMetadata {
	return &streamMetadata{
		Shards: make(map[string]*shardInfo),
	}
}

func (s *streamMetadata) noteSequenceNumber(sequenceNum string, shardID string) {
	sh := s.Shards[shardID]
	if sh == nil {
		sh = &shardInfo{}
		s.Shards[shardID] = sh
	}
	sh.noteSequenceNumber(sequenceNum)
}

type shardInfo struct {
	MinSequenceNumber string `json:"min_sequence_number"`
	MaxSequenceNumber string `json:"max_sequence_number"`
}

func (s *shardInfo) noteSequenceNumber(sequenceNum string) {
	if s.MinSequenceNumber == "" {
		s.MinSequenceNumber = sequenceNum
	} else {
		nums := naturalsort.NaturalSort([]string{
			sequenceNum,
			s.MinSequenceNumber,
		})
		sort.Sort(nums)
		s.MinSequenceNumber = nums[0]
	}
	if s.MaxSequenceNumber == "" {
		s.MaxSequenceNumber = sequenceNum
	} else {
		nums := naturalsort.NaturalSort([]string{
			sequenceNum,
			s.MaxSequenceNumber,
		})
		sort.Sort(nums)
		s.MaxSequenceNumber = nums[1]
	}
}
