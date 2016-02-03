package triton

import (
	"io"
	"log"
)

type Reader interface {
	ReadRecord() (rec Record, err error)
}

// A SerialReader let's us read from multiple readers, in sequence
type SerialReader struct {
	readers []Reader
	r_idx   int
}

func (sr *SerialReader) ReadRecord() (rec Record, err error) {
	for sr.r_idx < len(sr.readers) {
		rec, err = sr.readers[sr.r_idx].ReadRecord()
		if err != nil {
			if err == io.EOF {
				log.Println("Archive complete. Next...")
				sr.r_idx += 1
			} else {
				return
			}
		} else {
			return rec, nil
		}
	}

	return nil, io.EOF
}

func NewSerialReader(readers []Reader) Reader {
	return &SerialReader{readers, 0}
}
