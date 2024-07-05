package streedb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEntryFallsInside(t *testing.T) {
	val := Kv{Key: "hello 15"}
	min := Kv{Key: "hello 16"}
	max := Kv{Key: "hello 28"}

	assert.False(t, EntryFallsInsideMinMax(min, max, val))
	assert.True(t, EntryFallsInsideMinMax(val, max, min))

	min = Kv{Key: "hello 01"}
	max = Kv{Key: "hello 08"}

	assert.False(t, EntryFallsInsideMinMax(min, max, val))
	assert.True(t, EntryFallsInsideMinMax(min, val, max))
}
