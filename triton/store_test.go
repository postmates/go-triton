package triton

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/golang/snappy"
)

type nullStreamReader struct{}

func (nsr *nullStreamReader) ReadRecord() (map[string]interface{}, error) {
	return nil, io.EOF
}

func (nsr *nullStreamReader) ReadShardRecord() (*ShardRecord, error) {
	return nil, io.EOF
}

func (nsr *nullStreamReader) Checkpoint() error {
	return nil
}

func (nsr *nullStreamReader) Stop() {
}

func TestGenerateFilename(t *testing.T) {
	s := NewStore("test", nil, nil)

	fname := s.generateFilename()
	if fname != "test.tri" {
		t.Errorf("Bad file file %v", fname)
	}
}

func TestGenerateKeyname(t *testing.T) {
	s := NewStore("test", nil, nil)

	s.currentLogTime = time.Date(2015, 6, 30, 2, 45, 0, 0, time.UTC)
	name := s.generateKeyname()
	if name != "20150630/test-1435632300.tri" {
		t.Errorf("Bad file %v", name)
	}
}

func TestOpenWriter(t *testing.T) {
	s := NewStore("test", nil, nil)

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
	s := NewStore("test", &nullStreamReader{}, nil)

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
	s := NewStore("test", &nullStreamReader{}, nil)

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

func TestShardInfo(t *testing.T) {
	si := &ShardInfo{}
	si.noteSequenceNumber("12345")
	si.noteSequenceNumber("12346")

	if si.MinSequenceNumber != "12345" {
		t.Fatalf("expecting the min sequence number to be 12345 but got %q", si.MinSequenceNumber)
	}
	if si.MaxSequenceNumber != "12346" {
		t.Fatalf("expecting the max sequence number to be 12346 but got %q", si.MaxSequenceNumber)
	}

}

func BenchmarkShardInfo(b *testing.B) {
	si := &ShardInfo{}
	for i := 0; i < b.N; i++ {
		si.noteSequenceNumber("12345")
	}
}
