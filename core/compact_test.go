package core

import (
	"testing"

	db "github.com/sayden/streedb"
	"github.com/stretchr/testify/assert"
)

func TestIsAdjacent(t *testing.T) {
	a := &db.MetaFile[db.Integer]{Min: db.NewInteger(1), Max: db.NewInteger(5)}
	b := &db.MetaFile[db.Integer]{Min: db.NewInteger(6), Max: db.NewInteger(10)}
	c := &db.MetaFile[db.Integer]{Min: db.NewInteger(12), Max: db.NewInteger(15)}

	assert.True(t, isAdjacent(a, b))
	assert.True(t, isAdjacent(b, a))

	assert.False(t, isAdjacent(a, c))
	assert.False(t, isAdjacent(b, c))
	assert.False(t, isAdjacent(c, a))
	assert.False(t, isAdjacent(c, b))

	s1 := &db.MetaFile[db.Entry]{Min: db.NewKv("hello 10", 0), Max: db.NewKv("hello 19", 0)}
	s2 := &db.MetaFile[db.Entry]{Min: db.NewKv("hello 29", 0), Max: db.NewKv("hello 37", 0)}

	assert.False(t, isAdjacent(s1, s2))
}
