package triton

import "time"

const defaultBatchSize = 10
const defaultFlushInterval = 1 * time.Second

// MaxBatchSize is the limit Kinesis has on a PutRecords call
const MaxBatchSize = 500

// NewBatchWriter creates a batch version of an existing Writer using default
// values for size and interval.
func NewBatchWriter(w Writer) *BatchWriter {
	return NewBatchWriterSize(w, defaultBatchSize, defaultFlushInterval)
}

// NewBatchWriterSize creates a batch writer using the given size and interval.
func NewBatchWriterSize(w Writer, size int, intr time.Duration) *BatchWriter {
	if size <= 0 {
		size = defaultBatchSize
	} else if size > MaxBatchSize {
		size = MaxBatchSize
	}

	if intr <= 0 {
		intr = defaultFlushInterval
	}

	bufferSize := 10000

	bw := &BatchWriter{
		w:           w,
		buf:         make(chan Record, bufferSize),
		signal:      make(chan struct{}, bufferSize),
		flushSignal: make(chan struct{}, 0), // must be blocking write/read
		errors:      make(chan error, 1),

		size: size,
		intr: intr,

		ticker: time.NewTicker(intr),
	}

	go bw.writeLoop()

	return bw
}

// BatchWriter implements an asyncronous writer that writes records in batches.
// A batch is written when either the buffer size is exceeded or the time
// interval since the last write has been exceeded.
//
// Write errors are written to the Errors() channel. It is highly recommended
// that you actively monitor this channel because the writer will not stop
// after an error.
type BatchWriter struct {
	w      Writer
	buf    chan Record
	errors chan error

	size int
	intr time.Duration

	ticker      *time.Ticker
	signal      chan struct{}
	flushSignal chan struct{}
}

func (bw *BatchWriter) writeLoop() {
	for {
		select {
		case _, ok := <-bw.signal:
			if !ok {
				return
			}

			if len(bw.buf) >= bw.size {
				bw.flush()
			}
		case <-bw.ticker.C:
			bw.flush()
		case _, ok := <-bw.flushSignal:
			if ok {
				bw.flush()
				bw.flushSignal <- struct{}{}
			}
		}
	}
}

// flush writes everything in the current buffer. To prevent concurrent flushes,
// only write_loop() is allowed to call this.
func (bw *BatchWriter) flush() {
	numBuffered := len(bw.buf)
	if numBuffered == 0 {
		return
	}

	records := make([](Record), len(bw.buf))
	for i := 0; i < numBuffered; i++ {
		records[i] = <-bw.buf // this will never block
	}

	// write in blocks of batch size
	for n := 0; n < numBuffered; n += bw.size {
		end := n + bw.size
		if end > numBuffered {
			end = numBuffered
		}

		if err := bw.w.WriteRecords(records[n:end]...); err != nil {
			bw.writeError(err)
		}
	}
}

// Flush forces all buffered records to be sent.
// If there is an error it will have been written to the Errors chan.
func (bw *BatchWriter) Flush() {
	bw.flushSignal <- struct{}{} // ask to flush
	<-bw.flushSignal             // block until finish
}

// Close prevents future writes and flushes all currently buffered records.
// If there is an error it will have been written to the Errors chan.
func (bw *BatchWriter) Close() {
	bw.ticker.Stop()
	close(bw.signal)
	bw.Flush()
	close(bw.flushSignal)
	close(bw.errors)
}

// Errors returns the channel that errors will be returned on. It is highly
// recommended that you monitor this channel.
func (bw *BatchWriter) Errors() <-chan error {
	return bw.errors
}

// writeError trys to write to error chan but will not block.
func (bw *BatchWriter) writeError(err error) {
	select {
	case bw.errors <- err:
	default:
	}
}

// WriteRecords performs an asyncronous write to Kinesis.
//
// The returned error will always be nil in the current implementation.
// It is recommended you read errors from Errors().
func (bw *BatchWriter) WriteRecords(rs ...Record) error {
	for _, r := range rs {
		bw.buf <- r
	}
	bw.signal <- struct{}{} // signal that buffer changed
	return nil
}
