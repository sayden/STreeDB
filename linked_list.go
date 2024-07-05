package streedb

// LinkedList is a LL with specific methods to order in ascending or descending order
// it allows also duplicate values
type LinkedList[T Entry] struct {
	head *node[T]
}

// node represents a node in the doubly linked list
type node[T Entry] struct {
	value T
	next  *node[T]
}

func (dll *LinkedList[T]) Head() (T, bool) {
	if dll.head == nil {
		return *(new(T)), false
	}
	return dll.head.value, true
}

func (dll *LinkedList[T]) Last() (T, bool) {
	if dll.head == nil {
		return *(new(T)), false
	}

	current := dll.head
	for current.next != nil {
		current = current.next
	}

	return current.value, true
}

func (dll *LinkedList[T]) SetMax(value T) {
	newNode := &node[T]{value: value}

	if dll.head == nil {
		dll.head = newNode
		return
	}

	var last *node[T]
	for current := dll.head; current != nil; current, last = current.next, current {
		if current.value.LessThan(value) {
			// less than head value
			if last == nil {
				newNode.next = current
				dll.head = newNode
				return
			}

			newNode.next = current
			last.next = newNode

			return
		}
	}

	last.next = newNode
}

func (dll *LinkedList[T]) SetMin(value T) {
	newNode := &node[T]{value: value}

	if dll.head == nil {
		dll.head = newNode
		return
	}

	var last *node[T]
	for current := dll.head; current != nil; current, last = current.next, current {
		if value.LessThan(current.value) {
			// less than head value
			if last == nil {
				newNode.next = current
				dll.head = newNode
				return
			}

			newNode.next = current
			last.next = newNode

			return
		}
	}

	if last != nil {
		last.next = newNode
	} else {
		dll.head = newNode
	}
}

// Remove removes a node from the list
func (dll *LinkedList[T]) Remove(value T) {
	var last *node[T]
	for current := dll.head; current != nil; current, last = current.next, current {
		if value.Equals(current.value) {
			// remove the head
			if last == nil {
				dll.head = current.next
				return
			}

			// remove the last node
			if current.next == nil {
				last.next = nil
				return
			}

			// remove a node in the middle
			last.next = current.next

			return
		}
	}
}

// TraverseForward traverses the list from head to tail
func (dll *LinkedList[T]) Each(f func(int, T)) {
	i := 0
	for current := dll.head; current != nil; current, i = current.next, i+1 {
		f(i, current.value)
	}
}
