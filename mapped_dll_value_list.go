package streedb

// MapDLLVV is a LL with specific methods to order in ascending or descending order
// it allows also duplicate values
type MapDLLVV[T Entry, V UUIdentifiable] struct {
	head *kvListNode[T, V]
	tail *kvListNode[T, V]
}

// kvListNode represents a kvListNode in the doubly linked list
// type kvListNode[T Entry, V any] struct {
// 	key   T
// 	value []V
// 	next  *kvListNode[T, V]
// 	prev  *kvListNode[T, V]
// }

func (dll *MapDLLVV[T, V]) Head() (*kvListNode[T, V], bool) {
	if dll.head == nil {
		return nil, false
	}
	return dll.head, true
}

func (dll *MapDLLVV[T, V]) Tail() (*kvListNode[T, V], bool) {
	if dll.tail == nil {
		return nil, false
	}
	return dll.tail, true
}

func (dll *MapDLLVV[T, V]) SetMax(key T, value V) {
	if dll.head == nil {
		newkvListNode := &kvListNode[T, V]{key: key, values: []V{value}}
		dll.head = newkvListNode
		dll.tail = newkvListNode
		return
	}

	var last *kvListNode[T, V]
	for current := dll.head; current != nil; current, last = current.next, current {
		if current.key.Equals(key) {
			current.values = append(current.values, value)
			return
		}

		if current.key.LessThan(key) {
			newkvListNode := &kvListNode[T, V]{key: key, values: []V{value}}
			// less than head value
			if last == nil {
				newkvListNode.next = current
				dll.head = newkvListNode
				current.prev = newkvListNode
				return
			}

			// somewhere in the middle
			newkvListNode.next = current
			newkvListNode.prev = last
			last.next = newkvListNode
			current.prev = newkvListNode

			return
		}
	}

	//last node
	newkvListNode := &kvListNode[T, V]{key: key, values: []V{value}}
	last.next = newkvListNode
	newkvListNode.prev = last
	dll.tail = newkvListNode
}

func (dll *MapDLLVV[T, V]) SetMin(key T, value V) {

	if dll.head == nil {
		newkvListNode := &kvListNode[T, V]{key: key, values: []V{value}}
		dll.head = newkvListNode
		dll.tail = newkvListNode
		return
	}

	var last *kvListNode[T, V]
	for current := dll.head; current != nil; current, last = current.next, current {
		if current.key.Equals(key) {
			current.values = append(current.values, value)
			return
		}

		if key.LessThan(current.key) {
			newkvListNode := &kvListNode[T, V]{key: key, values: []V{value}}
			// less than head value
			if last == nil {
				newkvListNode.next = current
				dll.head = newkvListNode
				current.prev = newkvListNode
				return
			}

			newkvListNode.next = current
			newkvListNode.prev = last
			last.next = newkvListNode
			current.prev = newkvListNode

			return
		}
	}

	newkvListNode := &kvListNode[T, V]{key: key, values: []V{value}}
	if last != nil {
		last.next = newkvListNode
		newkvListNode.prev = last
		dll.tail = newkvListNode
	} else {
		dll.head = newkvListNode
		dll.tail = newkvListNode
	}
}

// Remove removes a kvListNode from the list
func (dll *MapDLLVV[T, V]) Remove(key T, id V) {
	var last *kvListNode[T, V]
	var node V

	for current := dll.head; current != nil; current, last = current.next, current {
		i := 0
		for i, node = 0, current.values[i]; i < len(current.values); i, node = i+1, current.values[i] {
			if node.UUID() == id.UUID() {
				current.values = append(current.values[:i], current.values[i+1:]...)
				i--
				if len(current.values) == 0 {
					// remove the head
					if last == nil {
						current.next.prev = nil
						dll.head = current.next
						return
					}

					// remove the last kvListNode
					if current.next == nil {
						last.next = nil
						return
					}

					// remove a kvListNode in the middle
					last.next = current.next
					current.next.prev = last

					return
				}
				return
			}
		}
	}
}

// TraverseForward traverses the list from head to tail
func (dll *MapDLLVV[T, V]) Each(f func(int, T, []V)) {
	i := 0
	for current := dll.head; current != nil; current, i = current.next, i+1 {
		f(i, current.key, current.values)
	}
}
