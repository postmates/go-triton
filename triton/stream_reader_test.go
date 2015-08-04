package triton

import "testing"

func TestNewStreamReader(t *testing.T) {
	svc := newTestKinesisService()
	st := newTestKinesisStream("test-stream")
	s1 := newTestKinesisShard()

	r1 := make(map[string]interface{})
	r1["value"] = "a"
	s1.AddRecord(SequenceNumber("a"), r1)
	st.AddShard(ShardID("0"), s1)

	s2 := newTestKinesisShard()
	r2 := make(map[string]interface{})
	r2["value"] = "b"
	s2.AddRecord(SequenceNumber("b"), r2)
	st.AddShard(ShardID("1"), s2)

	svc.AddStream(st)

	db := openTestDB()
	defer closeTestDB(db)

	c, err := NewCheckpointer("test", "test-stream", db)
	if err != nil {
		t.Error(err)
		return
	}

	sr := NewStreamReader(svc, "test-stream", c)

	_, err = sr.ReadRecord()
	if err != nil {
		t.Error(err)
		return
	}

	_, err = sr.ReadRecord()
	if err != nil {
		t.Error(err)
		return
	}

	sr.Checkpoint()

	c1, err := c.LastSequenceNumber(ShardID("0"))
	if c1 != SequenceNumber("a") {
		t.Error("Bad sequence number", c1)
	}

	c2, err := c.LastSequenceNumber(ShardID("1"))
	if c2 != SequenceNumber("b") {
		t.Error("Bad sequence number", c2)
	}
}
