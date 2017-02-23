package tritond

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMockClientPut(t *testing.T) {
	c := NewMockClient()

	data := map[string]interface{}{
		"example":  "hello",
		"example2": "world",
	}
	err := c.Put("delivery", "delivery-uuid", data)
	assert.NoError(t, err)
	assert.EqualValues(t, data, c.StreamData["delivery"][0])
}
