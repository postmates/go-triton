package triton

import (
	"bytes"
	"testing"
	"time"
)

func NewTestWriter(config *StreamConfig, s KinesisService, backoffWait time.Duration) Writer {
	return &writer{
		config:         config,
		svc:            s,
		maxBackoffWait: backoffWait,
	}
}

func TestWriteRecords(t *testing.T) {
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
	st.AddShard("test-value", s1)
	svc.AddStream(st)

	r := Record(map[string]interface{}{"value": "test-value"})
	w := NewTestWriter(config, svc, 1)
	if err := w.WriteRecords(r); err != nil {
		t.Fatal(err)
	}

	_, storedRecord := s1.PopRecord()
	if r["value"] != storedRecord["value"] {
		t.Fatal("Records are not equal", r, storedRecord)
	}
}

func TestWriteRecordsFailing(t *testing.T) {
	configString := bytes.NewBufferString(`
  my_stream:
    name: test-stream
    partition_key: value
    region: us-west-1
  `)
	c, _ := NewConfigFromFile(configString)
	config, _ := c.ConfigForName("my_stream")

	r := Record(map[string]interface{}{"value": "test-value"})
	w := NewTestWriter(config, &failingKinesisService{}, 1)
	if err := w.WriteRecords(r); err == nil {
		t.Fatal("Write did not fail as expected")
	}
}
