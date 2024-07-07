package streedb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMapLLMax(t *testing.T) {
	dll := &MapLL[Integer, struct{}]{}

	dll.SetMax(Integer{N: 1}, struct{}{})
	dll.SetMax(Integer{N: 2}, struct{}{})
	dll.SetMax(Integer{N: 0}, struct{}{})
	dll.SetMax(Integer{N: 3}, struct{}{})

	// Remove a node and traverse again
	fmt.Println("Removing second node 1")
	dll.Remove(Integer{N: 1}) // Remove the second node

	assert.Equal(t, int32(3), dll.head.key.N)
	assert.Equal(t, int32(2), dll.head.next.key.N)

	// insert again a bunch of nodes, even one repeated
	fmt.Println("Inserting 4, 5, 1, 3")
	dll.SetMax(Integer{N: 4}, struct{}{})
	dll.SetMax(Integer{N: 5}, struct{}{})
	dll.SetMax(Integer{N: 1}, struct{}{})
	dll.SetMax(Integer{N: 3}, struct{}{})

	// Remove fist and last
	fmt.Println("Removing first (0) and last (5)")
	dll.Remove(Integer{N: 0})
	dll.Remove(Integer{N: 5})

	assert.Equal(t, int32(4), dll.head.key.N)
}

func traverseMap(dll *MapLL[Integer, struct{}]) {
	fmt.Println("Traverse Forward:")
	dll.Each(func(i int, k Integer, v struct{}) { fmt.Printf("%v ", k) })
	fmt.Println()
}

func TestMapLLMin(t *testing.T) {
	dll := &MapLL[Integer, struct{}]{}

	dll.SetMin(Integer{N: 1}, struct{}{})
	dll.SetMin(Integer{N: 2}, struct{}{})
	dll.SetMin(Integer{N: 0}, struct{}{})
	dll.SetMin(Integer{N: 3}, struct{}{})

	// Remove a node and traverse again
	fmt.Println("Removing second node 1")
	dll.Remove(Integer{N: 1}) // Remove the second node

	assert.Equal(t, int32(0), dll.head.key.N)
	assert.Equal(t, int32(2), dll.head.next.key.N)

	// insert again a bunch of nodes, even one repeated
	fmt.Println("Inserting 4, 5, 1, 3")
	dll.SetMin(Integer{N: 4}, struct{}{})
	dll.SetMin(Integer{N: 5}, struct{}{})
	dll.SetMin(Integer{N: 1}, struct{}{})
	dll.SetMin(Integer{N: 3}, struct{}{})

	// Remove fist and last
	fmt.Println("Removing first (0) and last (5)")
	dll.Remove(Integer{N: 0})
	dll.Remove(Integer{N: 5})

	assert.Equal(t, int32(1), dll.head.key.N)
}
