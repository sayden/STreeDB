package bplustree

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func IntCmp(a, b int) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}

	return 0
}

func TestBPlusTree(t *testing.T) {
	btree := NewTree[int, int](IntCmp)
	defer btree.Close()

	btree.Set(1, 1)
	btree.Set(2, 2)
	btree.Set(3, 3)

	var (
		iter       *Enumerator[int, int]
		key, value int
		err        error
		found      bool
	)

	iter, found = btree.Seek(2)
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

	ok := btree.Delete(2)
	assert.True(t, ok)

	_, found = btree.Get(2)
	assert.False(t, found)
}
