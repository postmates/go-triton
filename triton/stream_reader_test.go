package triton

import "testing"

// db := openTestDB()
// defer closeTestDB(db)
//
// c, err := NewCheckpointer("test", "test-stream", db)
// if err != nil {
// 	t.Error(err)
// 	return
// }

// sr.Checkpoint()
//
// c1, err := c.LastSequenceNumber("0")
// if c1 != "a" {
// 	t.Error("Bad sequence number", c1)
// }
//
// c2, err := c.LastSequenceNumber("1")
// if c2 != "b" {
// 	t.Error("Bad sequence number", c2)
// }
//
// sr.Stop()

func TestStreamRead(t *testing.T) {
	svc := newTestKinesisService()
	st := newTestKinesisStream("test-stream")
	s1 := newTestKinesisShard()

	r1 := Record{"value": "a"}
	s1.AddRecord("a", r1)
	st.AddShard("0", s1)

	s2 := newTestKinesisShard()
	r2 := Record{"value": "b"}
	s2.AddRecord("b", r2)
	st.AddShard("1", s2)
	svc.AddStream(st)

	sr, err := NewStreamShardsService(svc, "us-west-1", "test-stream", nil)
	if err != nil {
		t.Fatal(err)
	}

	records := make([]Record, 4)
	n, err := sr.Read(records)
	if err != nil {
		t.Fatal(err)
	}

	if n != 2 {
		t.Error("Failed to read records:", records)
	}

	// Ordering is strict
	if records[0]["value"] != "a" {
		t.Fatal("Error with record A", records[0])
	}

	if records[1]["value"] != "b" {
		t.Fatal("Error with record B", records[1])
	}
}

func TestStreamReadEmpty(t *testing.T) {
	svc := new(NullKinesisService)
	sr, err := NewStreamShardsService(svc, "us-west-1", "test-stream", nil)
	if err != nil {
		t.Fatal(err)
	}

	records := make([]Record, 4)
	n, err := sr.Read(records)
	if err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatal("Read on empty stream")
	}
}

func TestStreamReadFailing(t *testing.T) {
	svc := new(failingKinesisService)
	sr, err := NewStreamShardsService(svc, "us-west-1", "test-stream", nil)
	if err == nil {
		t.Fatal("Did not error on empty service")
	}

	n, err := sr.Read([]Record{})
	if err == nil {
		t.Fatal("Did not error on record read")
	}
}
