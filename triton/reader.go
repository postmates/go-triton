package triton

// An interface for reading records
// A call to ReadRecord() will return either a record or an error.  At the end of a stream io.EOF will be returned.
type Reader interface {
	ReadRecord() (rec Record, err error)
}
