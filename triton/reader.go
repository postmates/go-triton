package triton

// Reader is an interface for a basic read
//
// Read reads up to len(p) bytes into p. It returns the number of bytes read
// (0 <= n <= len(p)) and any error encountered.
type Reader interface {
	Read(p []Record) (int, error)
}
