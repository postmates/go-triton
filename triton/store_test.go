package triton

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/golang/snappy/snappy"
)

type NullS3Service struct{}

func newTestStreamConfig() *StreamConfig {
	sc := StreamConfig{
		StreamName:       "test_stream",
		RegionName:       "us-west-1",
		PartitionKeyName: "value",
	}

	return &sc
}

func TestNewStore(t *testing.T) {
	//svc := NullS3Service{}

	sc := newTestStreamConfig()

	NewStore(sc, "0001", nil)
}

func TestGenerateFilename(t *testing.T) {
	sc := newTestStreamConfig()

	s := NewStore(sc, "0001", nil)

	n := time.Date(2015, 6, 30, 2, 45, 0, 0, time.UTC)
	fname := s.generateFilename(n)
	if fname != "test_stream-0001-2015063002.tri" {
		t.Errorf("Bad file file %v", fname)
	}
}

func TestOpenWriter(t *testing.T) {
	sc := newTestStreamConfig()

	s := NewStore(sc, "0001", nil)

	w, err := s.getCurrentWriter()
	if err != nil {
		t.Errorf("Failed getting current writer: %v", err)
		return
	}

	defer os.Remove(*s.currentFilename)

	w2, err := s.getCurrentWriter()
	if w != w2 {
		t.Errorf("Failed getting current writer: %v", err)
		return
	}
}

func TestOpenAndCloseWriter(t *testing.T) {
	sc := newTestStreamConfig()

	s := NewStore(sc, "0001", nil)

	_, err := s.getCurrentWriter()
	if err != nil {
		t.Errorf("Failed getting current writer: %v", err)
		return
	}

	fname := *s.currentFilename
	defer os.Remove(fname)

	s.closeWriter()

	if s.currentWriter != nil {
		t.Errorf("Writer still open")
		return
	}
}

func TestPut(t *testing.T) {
	sc := newTestStreamConfig()

	s := NewStore(sc, "0001", nil)

	testData := []byte{0x01, 0x02, 0x03}

	err := s.Put(testData)
	if err != nil {
		t.Errorf("Failed to put %v", err)
	}

	fname := *s.currentFilename
	defer os.Remove(fname)

	s.Close()

	f, err := os.Open(fname)
	if err != nil {
		t.Errorf("Failed to open")
		return
	}

	df := snappy.NewReader(f)
	data, err := ioutil.ReadAll(df)
	if err != nil {
		t.Errorf("Failed to read %v", err)
	} else {
		if bytes.Compare(data, testData) != 0 {
			t.Errorf("Data mismatch")
		}
	}
}
