package triton

import (
	"io"
	"testing"
)

func TestSerialReaderEmtpy(t *testing.T) {
	var readers []Reader

	r := NewSerialReader(readers)

	sr := r.(*SerialReader)
	if sr.r_idx != 0 {
		t.Error("Should be 0")
	}

	_, err := r.ReadRecord()
	if err != io.EOF {
		t.Error("Should EOF")
	}
}

type instantEOFReader struct{}

func (sr *instantEOFReader) ReadRecord() (rec Record, err error) {
	return nil, io.EOF
}

func TestSerialReaderEOF(t *testing.T) {
	var readers []Reader
	readers = append(readers, &instantEOFReader{})

	r := NewSerialReader(readers)

	sr := r.(*SerialReader)
	if sr.r_idx != 0 {
		t.Error("Should be 0")
	}

	_, err := r.ReadRecord()
	if err != io.EOF {
		t.Error("Should EOF")
	}

	if sr.r_idx != 1 {
		t.Error("Should be 1", sr.r_idx)
	}
}
