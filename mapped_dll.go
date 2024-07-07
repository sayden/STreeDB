package streedb

// MapDLL is a LL with specific methods to order in ascending or descending order
// it allows also duplicate values
type MapDLL[T Entry, V any] struct {
	head *kvDLLNode[T, V]
	tail *kvDLLNode[T, V]
}

// kvDLLNode represents a kvDLLNode in the doubly linked list
type kvDLLNode[T Entry, V any] struct {
	key   T
	value V
	next  *kvDLLNode[T, V]
	prev  *kvDLLNode[T, V]
}

func (dll *MapDLL[T, V]) Head() (*kvDLLNode[T, V], bool) {
	if dll.head == nil {
		return nil, false
	}
	return dll.head, true
}

func (dll *MapDLL[T, V]) Tail() (*kvDLLNode[T, V], bool) {
	if dll.tail == nil {
		return nil, false
	}
	return dll.tail, true
}

func (dll *MapDLL[T, V]) SetMax(key T, value V) {
	newkvDLLNode := &kvDLLNode[T, V]{key: key, value: value}

	if dll.head == nil {
		dll.head = newkvDLLNode
		dll.tail = newkvDLLNode
		return
	}

	var last *kvDLLNode[T, V]
	for current := dll.head; current != nil; current, last = current.next, current {
		if current.key.LessThan(key) {
			// less than head value
			if last == nil {
				newkvDLLNode.next = current
				dll.head = newkvDLLNode
				current.prev = newkvDLLNode
				return
			}

			// somewhere in the middle
			newkvDLLNode.next = current
			newkvDLLNode.prev = last
			last.next = newkvDLLNode
			current.prev = newkvDLLNode

			return
		}
	}

	//last node
	last.next = newkvDLLNode
	newkvDLLNode.prev = last
	dll.tail = newkvDLLNode
}

func (dll *MapDLL[T, V]) SetMin(key T, value V) {
	newkvDLLNode := &kvDLLNode[T, V]{key: key, value: value}

	if dll.head == nil {
		dll.head = newkvDLLNode
		dll.tail = newkvDLLNode
		return
	}

	var last *kvDLLNode[T, V]
	for current := dll.head; current != nil; current, last = current.next, current {
		if key.LessThan(current.key) {
			// less than head value
			if last == nil {
				newkvDLLNode.next = current
				dll.head = newkvDLLNode
				current.prev = newkvDLLNode
				return
			}

			newkvDLLNode.next = current
			newkvDLLNode.prev = last
			last.next = newkvDLLNode
			current.prev = newkvDLLNode

			return
		}
	}

	if last != nil {
		last.next = newkvDLLNode
		newkvDLLNode.prev = last
		dll.tail = newkvDLLNode
	} else {
		dll.head = newkvDLLNode
		dll.tail = newkvDLLNode
	}
}

// Remove removes a kvDLLNode from the list
func (dll *MapDLL[T, V]) Remove(key T) {
	var last *kvDLLNode[T, V]
	for current := dll.head; current != nil; current, last = current.next, current {
		if current.key.UUID() == key.UUID() {
			// remove the head
			if last == nil {
				current.next.prev = nil
				dll.head = current.next
				return
			}

			// remove the last kvDLLNode
			if current.next == nil {
				last.next = nil
				return
			}

			// remove a kvDLLNode in the middle
			last.next = current.next
			current.next.prev = last

			return
		}
	}
}

// TraverseForward traverses the list from head to tail
func (dll *MapDLL[T, V]) Each(f func(int, T, V)) {
	i := 0
	for current := dll.head; current != nil; current, i = current.next, i+1 {
		f(i, current.key, current.value)
	}
}
