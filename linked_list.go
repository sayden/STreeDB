package streedb

import (
	"cmp"
)

// LinkedList is a LL with specific methods to order in ascending or descending order
// it allows also duplicate values
type LinkedList[O cmp.Ordered, T Comparable[O]] struct {
	head *LLNode[O, T]
}

// LLNode represents a LLNode in the doubly linked list
type LLNode[O cmp.Ordered, T Comparable[O]] struct {
	Val  T
	Next *LLNode[O, T]
}

func (dll *LinkedList[O, T]) Head() (*LLNode[O, T], bool) {
	if dll.head == nil {
		return nil, false
	}
	return dll.head, true
}

func (dll *LinkedList[O, T]) Last() (T, bool) {
	if dll.head == nil {
		return *(new(T)), false
	}

	current := dll.head
	for current.Next != nil {
		current = current.Next
	}

	return current.Val, true
}

func (dll *LinkedList[O, T]) SetMax(value T) {
	newNode := &LLNode[O, T]{Val: value}

	if dll.head == nil {
		dll.head = newNode
		return
	}

	var last *LLNode[O, T]
	for current := dll.head; current != nil; current, last = current.Next, current {
		if current.Val.LessThan(value) {
			// less than head value
			if last == nil {
				newNode.Next = current
				dll.head = newNode
				return
			}

			newNode.Next = current
			last.Next = newNode

			return
		}
	}

	last.Next = newNode
}

func (dll *LinkedList[O, T]) SetMin(value T) {
	newNode := &LLNode[O, T]{Val: value}

	if dll.head == nil {
		dll.head = newNode
		return
	}

	var last *LLNode[O, T]
	for current := dll.head; current != nil; current, last = current.Next, current {
		if value.LessThan(current.Val) {
			// less than head value
			if last == nil {
				newNode.Next = current
				dll.head = newNode
				return
			}

			newNode.Next = current
			last.Next = newNode

			return
		}
	}

	if last != nil {
		last.Next = newNode
	} else {
		dll.head = newNode
	}
}

// Remove removes a node from the list
func (dll *LinkedList[O, T]) Remove(value T) {
	var last *LLNode[O, T]
	for current := dll.head; current != nil; current, last = current.Next, current {
		if value.Equals(current.Val) {
			// remove the head
			if last == nil {
				dll.head = current.Next
				return
			}

			// remove the last node
			if current.Next == nil {
				last.Next = nil
				return
			}

			// remove a node in the middle
			last.Next = current.Next

			return
		}
	}
}

// TraverseForward traverses the list from head to tail
func (dll *LinkedList[O, T]) Each(f func(int, T) bool) {
	i := 0
	for current := dll.head; current != nil; current, i = current.Next, i+1 {
		stop := f(i, current.Val)
		if stop {
			return
		}
	}
}
