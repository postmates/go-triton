package triton

// Reader is an interface for a basic read
type Reader interface {
	// Read reads up to len(p) bytes into p. It returns the number of records read
	// (0 <= n <= len(p)) and any error encountered.
	Read(p []Record) (int, error)
}
