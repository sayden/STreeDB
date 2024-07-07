package streedb

// MapLL is a LL with specific methods to order in ascending or descending order
// it allows also duplicate values
type MapLL[T Entry, V any] struct {
	head *kvNode[T, V]
}

// kvNode represents a kvNode in the doubly linked list
type kvNode[T Entry, V any] struct {
	key   T
	value V
	next  *kvNode[T, V]
}

func (dll *MapLL[T, V]) Head() (V, bool) {
	if dll.head == nil {
		return *(new(V)), false
	}
	return dll.head.value, true
}

func (dll *MapLL[T, V]) Last() (V, bool) {
	if dll.head == nil {
		return *(new(V)), false
	}

	current := dll.head
	for current.next != nil {
		current = current.next
	}

	return current.value, true
}

func (dll *MapLL[T, V]) SetMax(key T, value V) {
	newkvNode := &kvNode[T, V]{key: key, value: value}

	if dll.head == nil {
		dll.head = newkvNode
		return
	}

	var last *kvNode[T, V]
	for current := dll.head; current != nil; current, last = current.next, current {
		if current.key.LessThan(key) {
			// less than head value
			if last == nil {
				newkvNode.next = current
				dll.head = newkvNode
				return
			}

			newkvNode.next = current
			last.next = newkvNode

			return
		}
	}

	last.next = newkvNode
}

func (dll *MapLL[T, V]) SetMin(key T, value V) {
	newkvNode := &kvNode[T, V]{key: key, value: value}

	if dll.head == nil {
		dll.head = newkvNode
		return
	}

	var last *kvNode[T, V]
	for current := dll.head; current != nil; current, last = current.next, current {
		if key.LessThan(current.key) {
			// less than head value
			if last == nil {
				newkvNode.next = current
				dll.head = newkvNode
				return
			}

			newkvNode.next = current
			last.next = newkvNode

			return
		}
	}

	if last != nil {
		last.next = newkvNode
	} else {
		dll.head = newkvNode
	}
}

// Remove removes a kvNode from the list
func (dll *MapLL[T, V]) Remove(key T) {
	var last *kvNode[T, V]
	for current := dll.head; current != nil; current, last = current.next, current {
		if key.Equals(current.key) {
			// remove the head
			if last == nil {
				dll.head = current.next
				return
			}

			// remove the last kvNode
			if current.next == nil {
				last.next = nil
				return
			}

			// remove a kvNode in the middle
			last.next = current.next

			return
		}
	}
}

// TraverseForward traverses the list from head to tail
func (dll *MapLL[T, V]) Each(f func(int, T, V)) {
	i := 0
	for current := dll.head; current != nil; current, i = current.next, i+1 {
		f(i, current.key, current.value)
	}
}
