package streedb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBplusTreeEntry(t *testing.T) {
	tree := NewBPlusTreeWithValues[string, Kv]()
	keys := []int{3, 7, 1, 11, 2, 5, 9, 6, 4, 8, 10, 0}
	for _, key := range keys {
		s := fmt.Sprintf("%02d", key)
		tree.Insert(s, NewKv(s, int32(key)))
	}

	fmt.Println("B+ Tree structure:")
	tree.Print()
	fmt.Println("\nSearch results:")
	for _, key := range keys {
		s := fmt.Sprintf("%02d", key)
		fmt.Printf("Key %d: %v\n", key, tree.Search(s))
	}
	s := fmt.Sprintf("%02d", 12)
	fmt.Printf("-Key 12: %v\n", tree.Search(s))

	// Delete some keys
	keysToDelete := []int{3, 7}
	for _, key := range keysToDelete {
		fmt.Printf("\nDeleting %d:\n", key)
		s = fmt.Sprintf("%02d", key)
		tree.Delete(s)
		tree.Print()
	}

	fmt.Println("\nFinal search results:")
	for _, key := range keys {
		s = fmt.Sprintf("%02d", key)
		fmt.Printf("Key %d: %v\n", key, tree.Search(s))
	}

	fmt.Println("\nSearch results:")
	for _, key := range keys {
		s = fmt.Sprintf("%02d", key)
		fmt.Printf("Key %d: %v\n", key, tree.Search(s))
	}
	tree.Print()
	s = fmt.Sprintf("%02d", 7)
	node, found := tree.SearchClosest(s)
	assert.False(t, found)
	assert.NotNil(t, node)
}

func TestBplusTree(t *testing.T) {
	tree := NewBPlusTree[int]()
	keys := []int{3, 7, 1, 11, 2, 5, 9, 6, 4, 8, 10, 0}
	for _, key := range keys {
		tree.Insert(key)
	}
	fmt.Println("B+ Tree structure:")
	tree.Print()
	fmt.Println("\nSearch results:")
	for _, key := range keys {
		fmt.Printf("Key %d: %v\n", key, tree.Search(key))
	}
	fmt.Printf("-Key 12: %v\n", tree.Search(12))

	// Delete some keys
	keysToDelete := []int{3, 7}
	for _, key := range keysToDelete {
		fmt.Printf("\nDeleting %d:\n", key)
		tree.Delete(key)
		tree.Print()
	}

	fmt.Println("\nFinal search results:")
	for _, key := range keys {
		fmt.Printf("Key %d: %v\n", key, tree.Search(key))
	}

	fmt.Println("\nSearch results:")
	for _, key := range keys {
		fmt.Printf("Key %d: %v\n", key, tree.Search(key))
	}
	tree.Print()
	node, found := tree.SearchClosest(7)
	assert.False(t, found)
	assert.NotNil(t, node)
}
