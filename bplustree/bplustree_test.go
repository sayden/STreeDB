package bplustree

import (
	"io"
	"testing"

	db "github.com/sayden/streedb"
	"github.com/stretchr/testify/assert"
)

func IntegerCmp(a, b db.Integer) int {
	if a.N < b.N {
		return -1
	}
	if a.N > b.N {
		return 1
	}

	return 0
}

func TestBPlusTree(t *testing.T) {
	btree := NewTree[db.Entry, db.Entry](db.EntryCmp)
	defer btree.Close()

	val1 := db.NewInteger(1)
	val2 := db.NewInteger(2)
	val3 := db.NewInteger(3)

	btree.Set(val1, val1)
	btree.Set(val2, val2)
	btree.Set(val3, val3)

	var (
		iter       *Enumerator[db.Entry, db.Entry]
		key, value db.Entry
		err        error
		found      bool
	)

	iter, found = btree.Seek(val2)
	assert.True(t, found)

	for key, value, err = iter.Next(); err == nil; key, value, err = iter.Next() {
		t.Logf("key: %v, value: %v", key, value)
	}
	if err != io.EOF {
		assert.NoError(t, err)
	}

	iter, err = btree.SeekFirst()
	assert.NoError(t, err)
	for key, value, err = iter.Next(); err == nil; key, value, err = iter.Next() {
		t.Logf("key: %v, value: %v", key, value)
	}
	if err != io.EOF {
		assert.NoError(t, err)
	}

	iter, err = btree.SeekLast()
	assert.NoError(t, err)
	for key, value, err = iter.Prev(); err == nil; key, value, err = iter.Prev() {
		t.Logf("key: %v, value: %v", key, value)
	}
	if err != io.EOF {
		assert.NoError(t, err)
	}

	ok := btree.Delete(val2)
	assert.True(t, ok)

	_, found = btree.Get(val2)
	assert.False(t, found)
}
