package streedb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAdjacent(t *testing.T) {
	a := Kv{Key: "abc"}
	b := Kv{Key: "abd"}
	c := Kv{Key: "hij"}

	assert.True(t, a.Adjacent(&b))
	assert.False(t, a.Adjacent(&c))
	assert.False(t, b.Adjacent(&c))
}
