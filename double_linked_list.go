package streedb

// DoublyLinkedList is a DLL with specific methods to order in ascending or descending order
// it allows also duplicate values
type DoublyLinkedList[T Entry] struct {
	head *node[T]
	last *node[T]
	len  int
}

// node represents a node in the doubly linked list
type node[T Entry] struct {
	value T
	prev  *node[T]
	next  *node[T]
}

func (dll *DoublyLinkedList[T]) Head() (T, bool) {
	if dll.head == nil {
		return *(new(T)), false
	}
	return dll.head.value, true
}

func (dll *DoublyLinkedList[T]) Last() (T, bool) {
	if dll.head == nil {
		return *(new(T)), false
	}

	current := dll.head
	for current.next != nil {
		current = current.next
	}
	return current.value, true
}

func (dll *DoublyLinkedList[T]) Len() int {
	return dll.len
}

func (dll *DoublyLinkedList[T]) SetMax(value T) {
	dll.len++
	newNode := &node[T]{value: value}

	if dll.head == nil {
		dll.head = newNode
		dll.last = newNode
		return
	}

	var last *node[T]
	for current := dll.head; current != nil; current, last = current.next, current {
		if current.value.LessThan(value) {
			if current.prev == nil {
				newNode.next = current
				current.prev = newNode
				dll.head = newNode
				return
			}

			current.prev.next = newNode
			current.prev = newNode
			newNode.prev = current.prev
			newNode.next = current

			return
		}
	}

	last.next = newNode
	newNode.prev = last
	dll.last = newNode
}

func (dll *DoublyLinkedList[T]) SetMin(value T) {
	dll.len++
	newNode := &node[T]{value: value}

	if dll.head == nil {
		dll.head = newNode
		dll.last = newNode
		return
	}

	var last *node[T]
	for current := dll.head; current != nil; current, last = current.next, current {
		if value.LessThan(current.value) {
			if current.prev == nil {
				newNode.next = current
				current.prev = newNode
				dll.head = newNode
				return
			}

			current.prev.next = newNode
			current.prev = newNode
			newNode.prev = current.prev
			newNode.next = current

			return
		}
	}

	last.next = newNode
	newNode.prev = last
	dll.last = newNode
}

// Remove removes a node from the list
func (dll *DoublyLinkedList[T]) Remove(value T) {
	dll.len--

	for current := dll.head; current != nil; current = current.next {
		if value.Equals(current.value) {
			// in-between
			if current.prev != nil && current.next != nil {
				current.prev.next = current.next
				current.next.prev = current.prev
				return
			}

			// head
			if current.prev == nil && current.next != nil {
				dll.head = current.next
				current.next.prev = nil
				return
			}

			// last
			if current.next == nil && current.prev != nil {
				current.prev.next = nil
				dll.last = current.prev
			} else if current.next == nil && current.prev == nil {
				dll.last = nil
			} else {
				panic("unreachable")
			}

			return
		}
	}
}

// TraverseForward traverses the list from head to tail
func (dll *DoublyLinkedList[T]) Each(f func(int, T)) {
	i := 0
	for current := dll.head; current != nil; current, i = current.next, i+1 {
		f(i, current.value)
	}
}
