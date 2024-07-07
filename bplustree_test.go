package streedb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBplusTree(t *testing.T) {
	tree := NewBPlusTree()
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
	fmt.Printf("Key 12: %v\n", tree.Search(12))

	// Delete some keys
	keysToDelete := []int{3, 7, 4, 11}
	for _, key := range keysToDelete {
		fmt.Printf("\nDeleting %d:\n", key)
		tree.Delete(key)
		tree.Print()
	}

	fmt.Println("\nFinal search results:")
	for _, key := range keys {
		fmt.Printf("Key %d: %v\n", key, tree.Search(key))
		found, v := tree.SearchClosest(key)
		fmt.Printf("Key %d: %v, %v\n", key, found, v)
	}

	fmt.Println("\nSearch results:")
	for _, key := range keys {
		fmt.Printf("Key %d: %v\n", key, tree.Search(key))
	}
	found, node := tree.SearchClosest(7)
	assert.False(t, found)
	assert.NotNil(t, node)
}
