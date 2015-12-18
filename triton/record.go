package triton

type Record map[string]interface{}

func NewRecord() Record { return make(Record) }
