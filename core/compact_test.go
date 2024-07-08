package core

import (
	"testing"

	db "github.com/sayden/streedb"
	"github.com/stretchr/testify/assert"
)

func TestIsAdjacent(t *testing.T) {
	a := &db.MetaFile[db.Integer]{Min: db.NewInteger(1, "a", "b"), Max: db.NewInteger(5, "a", "b")}
	b := &db.MetaFile[db.Integer]{Min: db.NewInteger(6, "a", "b"), Max: db.NewInteger(10, "a", "b")}
	c := &db.MetaFile[db.Integer]{Min: db.NewInteger(12, "a", "b"), Max: db.NewInteger(15, "a", "b")}

	assert.True(t, isAdjacent(a, b))
	assert.True(t, isAdjacent(b, a))

	assert.False(t, isAdjacent(a, c))
	assert.False(t, isAdjacent(b, c))
	assert.False(t, isAdjacent(c, a))
	assert.False(t, isAdjacent(c, b))

	s1 := &db.MetaFile[db.Entry]{Min: db.NewKv("hello 10", 0, "a"), Max: db.NewKv("hello 19", 0, "a")}
	s2 := &db.MetaFile[db.Entry]{Min: db.NewKv("hello 29", 0, "a"), Max: db.NewKv("hello 37", 0, "b")}

	assert.False(t, isAdjacent(s1, s2))
}
