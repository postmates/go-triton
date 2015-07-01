package triton

import (
	"os"
	"testing"
	"time"
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

func TestNewS3Store(t *testing.T) {
	//svc := NullS3Service{}

	sc := newTestStreamConfig()

	NewS3Store(sc, "triton-test")
}

func TestGenerateFilename(t *testing.T) {
	sc := newTestStreamConfig()

	s := NewS3Store(sc, "triton-test")

	n := time.Date(2015, 6, 30, 2, 45, 0, 0, time.UTC)
	fname := s.generateFilename(n)
	if fname != "test_stream-2015063002.tri" {
		t.Errorf("Bad file file %v", fname)
	}
}

func TestOpenWriter(t *testing.T) {
	sc := newTestStreamConfig()

	s := NewS3Store(sc, "triton-test")

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

	s := NewS3Store(sc, "triton-test")

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
