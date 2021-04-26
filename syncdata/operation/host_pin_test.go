package operation

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestHostPin(t *testing.T) {
	hp := NewHostPin(0)
	host := hp.Unpin()
	assert.Equal(t, "", host)

	hp.Pin("host1")
	host = hp.Unpin()
	assert.Equal(t, "", host)

	hp = NewHostPin(200)

	hp.Pin("host1")
	host = hp.Unpin()
	assert.Equal(t, "host1", host)
	host = hp.Unpin()
	assert.Equal(t, "", host)

	hp.Pin("host1")
	hp.Pin("host2")

	host = hp.Unpin()
	assert.Equal(t, "host2", host)
	host = hp.Unpin()
	assert.Equal(t, "", host)

	hp.Pin("host1")
	time.Sleep(time.Millisecond * 100)
	host = hp.Unpin()
	assert.Equal(t, "host1", host)

	hp.Pin("host1")
	time.Sleep(time.Millisecond * 200)
	host = hp.Unpin()
	assert.Equal(t, "", host)
}
