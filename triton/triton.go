// Package triton provides an opinionated interface with Kinesis
package triton

import "github.com/tinylib/msgp/msgp"

type Record map[string]interface{}

func MarshalRecord(r Record) ([]byte, error) {
	return msgp.AppendMapStrIntf([]byte{}, r)
}

func UnmarshalRecord(data []byte) (Record, error) {
	r, _, err := msgp.ReadMapStrIntfBytes(data, nil)
	return r, err
}
