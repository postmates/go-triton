package triton

// Some types to make sure our lists of func args don't get confused
type ShardID string
type SequenceNumber string

// For tracking ShardID => Last Sequence Number
type ShardToSequenceNumber map[ShardID]SequenceNumber
