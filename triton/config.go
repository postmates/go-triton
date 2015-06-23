package triton

import (
	"fmt"
	"io"
	"io/ioutil"

	"gopkg.in/yaml.v2"
)

type StreamConfig struct {
	StreamName       string `yaml:"name"`
	RegionName       string `yaml:"region"`
	PartitionKeyName string `yaml:"partition_key"`
}

type Config struct {
	Streams map[string]StreamConfig
}

func (c *Config) ConfigForName(n string) (sc *StreamConfig, err error) {
	if scv, ok := c.Streams[n]; ok {
		return &scv, nil
	} else {
		return nil, fmt.Errorf("Failed to find stream")
	}
}

func NewConfigFromFile(r io.Reader) (c *Config, err error) {
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}

	c = &Config{}

	err = yaml.Unmarshal(data, &c.Streams)
	if err != nil {
		return nil, err
	}

	return
}
