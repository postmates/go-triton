package triton

type ShardRecord struct {
	Record         map[string]interface{}
	ShardID        ShardID
	SequenceNumber SequenceNumber
}

type ShardReader interface {
	ReadShardRecord() (result *ShardRecord, err error)
}
