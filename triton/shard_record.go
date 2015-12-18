package triton

type ShardRecord struct {
	Record         Record
	ShardID        ShardID
	SequenceNumber SequenceNumber
}

type ShardReader interface {
	ReadShardRecord() (result *ShardRecord, err error)
}
