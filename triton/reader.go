package triton

import (
	"io"

	"github.com/golang/snappy"
	"github.com/tinylib/msgp/msgp"
)

type Reader struct {
	mr *msgp.Reader
}

func (r *Reader) ReadRecord() (rec map[string]interface{}, err error) {
	rec = make(map[string]interface{})

	err = r.mr.ReadMapStrIntf(rec)
	return
}

func NewReader(ir io.Reader) (or Reader) {
	sr := snappy.NewReader(ir)
	mr := msgp.NewReader(sr)

	return Reader{mr}
}
