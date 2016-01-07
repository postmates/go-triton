package triton

import (
	"bytes"
	"testing"
	"time"
)

func TestBatchWriterSizeExceeded(t *testing.T) {
	configString := bytes.NewBufferString(`
  my_stream:
    name: test-stream
    partition_key: value
    region: us-west-1
  `)
	c, _ := NewConfigFromFile(configString)
	config, _ := c.ConfigForName("my_stream")

	svc := newTestKinesisService()
	st := newTestKinesisStream(config.StreamName)
	s1 := newTestKinesisShard()
	st.AddShard(ShardID("test-value"), s1)
	svc.AddStream(st)

	w := NewTestWriter(config, svc, 1)
	bw := NewBatchWriterSize(w, 2, 24*time.Hour)

	r := Record(map[string]interface{}{"value": "test-value"})
	bw.WriteRecords(r)

	// Ensure this was not written
	if len(s1.records) != 0 {
		t.Fatal("Batcher did not wait for size to be exceeded")
	}

	bw.WriteRecords(r)

	// wait for write -- this is technically a race condition
	time.Sleep(10 * time.Millisecond)

	if len(s1.records) != 2 {
		t.Fatal("Batcher did not write when size exceeded")
	}
}

func TestBatchWriterTimeExceeded(t *testing.T) {
	configString := bytes.NewBufferString(`
  my_stream:
    name: test-stream
    partition_key: value
    region: us-west-1
  `)
	c, _ := NewConfigFromFile(configString)
	config, _ := c.ConfigForName("my_stream")

	svc := newTestKinesisService()
	st := newTestKinesisStream(config.StreamName)
	s1 := newTestKinesisShard()
	st.AddShard(ShardID("test-value"), s1)
	svc.AddStream(st)

	w := NewTestWriter(config, svc, 1)
	bw := NewBatchWriterSize(w, 1000, 1*time.Millisecond)

	r := Record(map[string]interface{}{"value": "test-value"})
	bw.WriteRecords(r)

	// Ensure this was not written
	if len(s1.records) != 0 {
		t.Fatal("Batcher did not wait for time to be exceeded")
	}

	// wait for write -- this is technically a race condition
	time.Sleep(10 * time.Millisecond)

	if len(s1.records) != 1 {
		t.Fatal("Batcher did not write when time exceeded")
	}
}
