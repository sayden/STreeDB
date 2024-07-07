package streedb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func (u *uuidMock) UUID() string {
	return fmt.Sprintf("uuid-%d", u.N)
}

type uuidMock struct {
	N int
}

func TestMapLLMax(t *testing.T) {
	dll := &MapLL[Integer, *uuidMock]{}

	dll.SetMax(Integer{N: 1}, &uuidMock{1})
	dll.SetMax(Integer{N: 2}, &uuidMock{2})
	dll.SetMax(Integer{N: 0}, &uuidMock{0})
	dll.SetMax(Integer{N: 3}, &uuidMock{3})

	// Remove a node and traverse again
	fmt.Println("Removing second node 1")
	dll.Remove(&uuidMock{1}) // Remove the second node

	assert.Equal(t, int32(3), dll.head.key.N)
	assert.Equal(t, int32(2), dll.head.next.key.N)

	// insert again a bunch of nodes, even one repeated
	fmt.Println("Inserting 4, 5, 1, 3")
	dll.SetMax(Integer{N: 4}, &uuidMock{4})
	dll.SetMax(Integer{N: 5}, &uuidMock{5})
	dll.SetMax(Integer{N: 1}, &uuidMock{1})
	dll.SetMax(Integer{N: 3}, &uuidMock{3})

	// Remove fist and last
	fmt.Println("Removing first (0) and last (5)")
	dll.Remove(&uuidMock{0})
	dll.Remove(&uuidMock{5})

	assert.Equal(t, int32(4), dll.head.key.N)
}

func traverseMap(dll *MapLL[Integer, *uuidMock]) {
	fmt.Println("Traverse Forward:")
	dll.Each(func(i int, k Integer, v *uuidMock) { fmt.Printf("%v ", k) })
	fmt.Println()
}

func TestMapLLMin(t *testing.T) {
	dll := &MapLL[Integer, *uuidMock]{}

	dll.SetMin(Integer{N: 1}, &uuidMock{1})
	dll.SetMin(Integer{N: 2}, &uuidMock{2})
	dll.SetMin(Integer{N: 0}, &uuidMock{0})
	dll.SetMin(Integer{N: 3}, &uuidMock{3})

	// Remove a node and traverse again
	fmt.Println("Removing second node 1")
	dll.Remove(&uuidMock{1}) // Remove the second node

	assert.Equal(t, int32(0), dll.head.key.N)
	assert.Equal(t, int32(2), dll.head.next.key.N)

	// insert again a bunch of nodes, even one repeated
	fmt.Println("Inserting 4, 5, 1, 3")
	dll.SetMin(Integer{N: 4}, &uuidMock{4})
	dll.SetMin(Integer{N: 5}, &uuidMock{5})
	dll.SetMin(Integer{N: 1}, &uuidMock{1})
	dll.SetMin(Integer{N: 3}, &uuidMock{3})

	// Remove fist and last
	fmt.Println("Removing first (0) and last (5)")
	dll.Remove(&uuidMock{0})
	dll.Remove(&uuidMock{5})

	assert.Equal(t, int32(1), dll.head.key.N)
}
