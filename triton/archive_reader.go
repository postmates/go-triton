package triton

import (
	"io"

	"github.com/golang/snappy"
	"github.com/tinylib/msgp/msgp"
)

// An ArchiveReader understands how to translate our archive data store
// format into indivdual records.
type ArchiveReader struct {
	mr *msgp.Reader
}

func (r *ArchiveReader) ReadRecord() (rec Record, err error) {
	rec = make(Record)

	err = r.mr.ReadMapStrIntf(rec)
	return
}

func NewArchiveReader(ir io.Reader) (or Reader) {
	sr := snappy.NewReader(ir)
	mr := msgp.NewReader(sr)

	return &ArchiveReader{mr}
}
