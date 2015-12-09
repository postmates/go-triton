package triton

type shardRecord struct {
	record   map[string]interface{}
	shard    string
	sequence string
}

type shardReader interface {
	readShardRecord() (result *shardRecord, err error)
}
