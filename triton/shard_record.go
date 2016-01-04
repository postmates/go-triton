package triton

type ShardRecord struct {
	Record         Record
	ShardID        ShardID
	SequenceNumber SequenceNumber
}
