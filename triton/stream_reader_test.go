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

	sr, err := NewStreamReader(svc, "test-stream", c)
	if err != nil {
		t.Error(err)
		return
	}

	foundA := false
	foundB := false

	rec1, err := sr.ReadRecord()
	if err != nil {
		t.Error(err)
		return
	}

	// Records could be in any order
	if rec1["value"].(string) == "a" {
		foundA = true
	} else if rec1["value"].(string) == "b" {
		foundB = true
	}

	rec2, err := sr.ReadRecord()
	if err != nil {
		t.Error(err)
		return
	}
	if rec2["value"].(string) == "a" {
		foundA = true
	} else if rec2["value"].(string) == "b" {
		foundB = true
	}

	if !(foundA && foundB) {
		t.Error("Failed to find records a and b")
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

	sr.Stop()
}

func TestProcessStreamToChanErrors(t *testing.T) {
	// This tests whether the stream can recover from and ignore bad data
	svc := newTestKinesisService()
	st := newTestKinesisStream("test-stream")
	s1 := newTestKinesisShard()

	r1 := make(map[string]interface{})
	r1["value"] = "a"
	s1.AddOverlengthRecord(SequenceNumber("a"), r1)
	r2 := make(map[string]interface{})
	r2["value"] = "b"
	s1.AddRecord(SequenceNumber("b"), r2)
	st.AddShard(ShardID("0"), s1)

	s2 := newTestKinesisShard()
	s2.AddBadEncodingRecord(SequenceNumber("c"))
	r3 := make(map[string]interface{})
	r3["value"] = "d"
	s2.AddRecord(SequenceNumber("d"), r3)
	st.AddShard(ShardID("1"), s2)

	svc.AddStream(st)

	sr, err := NewStreamReader(svc, "test-stream", noopCheckpointer{})
	if err != nil {
		t.Error(err)
		return
	}

	foundB := false
	foundD := false

	rec1, err := sr.ReadRecord()
	if err != nil {
		t.Error(err)
		return
	}

	// Records could be in any order
	if rec1["value"].(string) == "d" {
		foundD = true
	} else if rec1["value"].(string) == "b" {
		foundB = true
	}

	rec2, err := sr.ReadRecord()
	if err != nil {
		t.Error(err)
		return
	}
	if rec2["value"].(string) == "d" {
		foundD = true
	} else if rec2["value"].(string) == "b" {
		foundB = true
	}

	if !(foundD && foundB) {
		t.Error("Failed to find records a and b")
	}

	sr.Stop()
}
