package triton

import (
	"testing"
)

func TestNewShardStreamReaderFromSequence(t *testing.T) {
	svc := NullKinesisService{}
	s := newShardReader(&newShardReaderParams{
		kinesisService:           &svc,
		stream:                   "test-stream",
		shardID:                  "shard-0001",
		startAfterSequenceNumber: SequenceNumber("abc123"),
	})
	if s.stream != "test-stream" {
		t.Errorf("bad stream name")
	}

	if s.shardID != "shard-0001" {
		t.Errorf("bad ShardID")
	}

	if s.nextIterator != "" {
		t.Errorf("bad NextIteratorValue")
	}

}

func TestReadKinesisRecords(t *testing.T) {
	svc := newTestKinesisService()
	st := newTestKinesisStream("test-stream")
	s1 := newTestKinesisShard()

	r1 := make(map[string]interface{})
	s1.AddRecord(SequenceNumber("a"), r1)
	st.AddShard("shard-0000", s1)
	svc.AddStream(st)

	s := newShardReader(&newShardReaderParams{
		kinesisService: svc,
		stream:         "test-stream",
		shardID:        "shard-0000",
	})
	records, err := s.readKinesisRecords()
	if err != nil {
		t.Errorf("Received error %v", err)
		return
	}

	if len(records) != 1 {
		t.Errorf("Should have a record")
	}
}

func TestReadShardRecords(t *testing.T) {
	svc := newTestKinesisService()
	st := newTestKinesisStream("test-stream")
	s1 := newTestKinesisShard()

	r1 := map[string]interface{}{"foo": "bar"}
	s1.AddRecord(SequenceNumber("a"), r1)
	st.AddShard("shard-0000", s1)
	svc.AddStream(st)

	s := newShardReader(&newShardReaderParams{
		kinesisService: svc,
		stream:         "test-stream",
		shardID:        "shard-0000",
	})
	select {
	case e := <-s.errors:
		t.Fatalf("unexpected error: %s", e.Error())
	case r := <-s.records:
		if r.Record["foo"].(string) != "bar" {
			t.Fatalf("expecting bar")
		}
	}
}
