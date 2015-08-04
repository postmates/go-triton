package triton

// A StreamReader is a higher-level interface for reading data from a live Triton stream.
//
// By implementing a Reader interface, we can delivery processed triton data to the client.
// In addition, we provide checkpointing service.
type StreamReader interface {
	Reader
	Checkpoint() error
}

func NewStreamReader(svc KinesisService, streamName string, c Checkpointer) (sr StreamReader) {
	// TODO: Implement
	return
}

// TODO: An interface to choose shards
