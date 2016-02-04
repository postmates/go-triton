package triton

// Reader is an interface for a basic read
type Reader interface {
	// Read reads up to len(r) bytes into r. It returns the number of records
	// read (0 <= n <= len(r)), the offset after the read, and any error
	// encountered.
	Read(r []Record) (n int, off string, err error)
}
