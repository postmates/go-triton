package triton

import (
	"testing"
)

func TestReadFromMultiShardReader(t *testing.T) {
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
