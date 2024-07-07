package streedb

func NewIndex[T Entry, V UUIdentifiable]() Index[T, V] {
	return Index[T, V]{pairs: make(map[string]*kvListNode[T, V]), tree: NewBPlusTreeWithValues[string, T]()}
}

// Index is a LL with specific methods to order in ascending or descending order
// it allows also duplicate values
type Index[T Entry, V UUIdentifiable] struct {
	head  *kvListNode[T, V]
	tail  *kvListNode[T, V]
	tree  *BPlusTreeWithValues[string, T]
	pairs map[string]*kvListNode[T, V]
}

// kvListNode represents a kvListNode in the doubly linked list
type kvListNode[T Entry, V any] struct {
	key    T
	values []V
	idx    int

	next *kvListNode[T, V]
	prev *kvListNode[T, V]
}

func (k *kvListNode[T, V]) Next() (V, bool) {
	if k.idx >= len(k.values) {
		k.idx = 0
		return (*new(V)), false
	}

	value := k.values[k.idx]
	k.idx++
	return value, true
}

func (dll *Index[T, V]) SetMax(key T, value V) {
	if dll.head == nil {
		newkvListNode := dll.newKvListNode(key, value)
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
			newkvListNode := dll.newKvListNode(key, value)
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
	newkvListNode := dll.newKvListNode(key, value)
	last.next = newkvListNode
	newkvListNode.prev = last
	dll.tail = newkvListNode
}

func (dll *Index[T, V]) SetMin(key T, value V) {
	if dll.head == nil {
		newkvListNode := dll.newKvListNode(key, value)
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
			newkvListNode := dll.newKvListNode(key, value)
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

	newkvListNode := dll.newKvListNode(key, value)
	if last != nil {
		last.next = newkvListNode
		newkvListNode.prev = last
		dll.tail = newkvListNode
	} else {
		dll.head = newkvListNode
		dll.tail = newkvListNode
	}
}

func (dll *Index[T, V]) SearchClosest(key UUIdentifiable) ([]string, bool) {
	node, found := dll.tree.SearchClosest(key.UUID())
	return node.keys, found
}

// Remove removes a kvListNode from the list
func (dll *Index[T, V]) Remove(key T, id V) {
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

func (dll *Index[T, V]) Head() (*kvListNode[T, V], bool) {
	if dll.head == nil {
		return nil, false
	}
	return dll.head, true
}

func (dll *Index[T, V]) Tail() (*kvListNode[T, V], bool) {
	if dll.tail == nil {
		return nil, false
	}
	return dll.tail, true
}

func (dll *Index[T, V]) Each(f func(int, T, []V)) {
	i := 0
	for current := dll.head; current != nil; current, i = current.next, i+1 {
		f(i, current.key, current.values)
	}
}

func (dll *Index[T, V]) newKvListNode(key T, value V) *kvListNode[T, V] {
	node := &kvListNode[T, V]{key: key, values: []V{value}}
	dll.pairs[key.UUID()] = node
	dll.tree.Insert(key.UUID(), key)
	return node
}
