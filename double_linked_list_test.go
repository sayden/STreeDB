package streedb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDLLMax(t *testing.T) {
	dll := &DoublyLinkedList[Integer]{}

	dll.SetMax(Integer{N: 1})
	dll.SetMax(Integer{N: 2})
	dll.SetMax(Integer{N: 0})
	dll.SetMax(Integer{N: 3})

	fmt.Println("Traverse Forward:")
	dll.Each(func(i int, v Integer) { fmt.Println(v) })

	// Remove a node and traverse again
	fmt.Println("Removing second node 1")
	dll.Remove(Integer{N: 1}) // Remove the second node

	fmt.Println("After Removal Traverse Forward:")
	dll.Each(func(i int, v Integer) { fmt.Println(v) })

	assert.Equal(t, int32(3), dll.head.value.N)
	assert.Equal(t, int32(2), dll.head.next.value.N)

	// insert again a bunch of nodes, even one repeated
	fmt.Println("Inserting 4, 5, 1, 3")
	dll.SetMax(Integer{N: 4})
	dll.SetMax(Integer{N: 5})
	dll.SetMax(Integer{N: 1})
	dll.SetMax(Integer{N: 3})

	fmt.Println("Traverse Forward:")
	dll.Each(func(i int, v Integer) { fmt.Println(v) })

	// Remove fist and last
	fmt.Println("Removing first (0) and last (5)")
	dll.Remove(Integer{N: 0})
	dll.Remove(Integer{N: 5})

	assert.Equal(t, int32(4), dll.head.value.N)

	fmt.Println("Traverse Forward:")
	dll.Each(func(i int, v Integer) { fmt.Println(v) })
}

func TestDLLMin(t *testing.T) {
	dll := &DoublyLinkedList[Integer]{}

	dll.SetMin(Integer{N: 1})
	dll.SetMin(Integer{N: 2})
	dll.SetMin(Integer{N: 0})
	dll.SetMin(Integer{N: 3})

	fmt.Println("Traverse Forward:")
	dll.Each(func(i int, v Integer) { fmt.Println(v) })

	// Remove a node and traverse again
	fmt.Println("Removing second node 1")
	dll.Remove(Integer{N: 1}) // Remove the second node

	fmt.Println("After Removal Traverse Forward:")
	dll.Each(func(i int, v Integer) { fmt.Println(v) })

	assert.Equal(t, int32(0), dll.head.value.N)
	assert.Equal(t, int32(2), dll.head.next.value.N)

	// insert again a bunch of nodes, even one repeated
	fmt.Println("Inserting 4, 5, 1, 3")
	dll.SetMin(Integer{N: 4})
	dll.SetMin(Integer{N: 5})
	dll.SetMin(Integer{N: 1})
	dll.SetMin(Integer{N: 3})

	fmt.Println("Traverse Forward:")
	dll.Each(func(i int, v Integer) { fmt.Println(v) })

	// Remove fist and last
	fmt.Println("Removing first (0) and last (5)")
	dll.Remove(Integer{N: 0})
	dll.Remove(Integer{N: 5})

	assert.Equal(t, int32(1), dll.head.value.N)

	fmt.Println("Traverse Forward:")
	dll.Each(func(i int, v Integer) { fmt.Println(v) })
}
