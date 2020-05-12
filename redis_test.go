package txstorage

import (
	"testing"

	"github.com/letsfire/redigo/v2/mode/alone"
	"github.com/stretchr/testify/assert"
)

func TestRedis(t *testing.T) {
	client := alone.NewClient()
	rts := NewRedis("test", client)
	var data1 = []byte("hello world")
	id, err1 := rts.Store(data1)
	data2, err2 := rts.Fetch(id)
	err3 := rts.Remove(id)
	data3, _ := rts.Fetch(id)
	assert.Nil(t, err1)
	assert.Nil(t, err2)
	assert.Nil(t, err3)
	assert.EqualValues(t, data1, data2)
	assert.Empty(t, data3)
}
