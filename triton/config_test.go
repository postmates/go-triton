package triton

import (
	"bytes"
	"testing"
)

var testConfig = `
my_stream:
  name: my_stream_v2
  partition_key: value
  region: us-west-1
`

func TestNewConfigFromFile(t *testing.T) {
	r := bytes.NewBufferString(testConfig)

	c, err := NewConfigFromFile(r)
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	s, err := c.ConfigForName("my_stream")

	if err != nil {
		t.Errorf("Failed to find stream")
		return
	}

	if s.StreamName != "my_stream_v2" {
		t.Errorf("StreamName mismatch")
	}
	if s.RegionName != "us-west-1" {
		t.Errorf("RegionName mismatch")
	}
	if s.PartitionKeyName != "value" {
		t.Errorf("PartitionKeyName mismatch")
	}
}

func TestMissingStream(t *testing.T) {

	c := Config{}

	_, err := c.ConfigForName("foo")
	if err != nil {
		// all good
	} else {
		t.Errorf("Missing error")
		return
	}

}
