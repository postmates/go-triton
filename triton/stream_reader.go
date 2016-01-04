package triton

type StreamReader interface {
	ShardReader
	Reader
	Checkpoint() error
}

type streamReader struct {
	reader                  ShardReader
	checkpointer            Checkpointer
	shardIDToSequenceNumber map[ShardID]SequenceNumber
}

func NewStreamReader(kinesisService KinesisService, stream string, c Checkpointer) (rc StreamReader, err error) {
	rc = &streamReader{
		reader: NewMultiShardReader(&NewMultiShardReaderParams{
			KinesisService: kinesisService,
			Stream:         stream,
		}),
		checkpointer:            c,
		shardIDToSequenceNumber: make(map[ShardID]SequenceNumber),
	}
	return
}

func (rc *streamReader) Stop() {
	rc.reader.Stop()
}

func (rc *streamReader) Checkpoint() (err error) {
	for shardID, seqNum := range rc.shardIDToSequenceNumber {
		err = rc.checkpointer.Checkpoint(shardID, seqNum)
		if err != nil {
			return
		}
	}
	return
}

// implement the Reader interface

func (rc *streamReader) ReadRecord() (rec Record, err error) {
	sr, err := rc.ReadShardRecord()
	if err != nil {
		return
	}
	rec = sr.Record
	return
}

// Implement the ShardReader interface
func (rc *streamReader) ReadShardRecord() (rec *ShardRecord, err error) {
	rec, err = rc.reader.ReadShardRecord()
	if err != nil {
		return
	}
	rc.shardIDToSequenceNumber[rec.ShardID] = rec.SequenceNumber
	return
}
