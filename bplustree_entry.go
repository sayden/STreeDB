package streedb

import (
	"cmp"
	"fmt"
	"slices"
)

type BPlusTreeWithValues[C cmp.Ordered, E Entry] struct {
	root *NodeWV[C, E]
}

type NodeWV[C cmp.Ordered, E Entry] struct {
	keys     []C
	children []*NodeWV[C, E]
	isLeaf   bool
	next     *NodeWV[C, E] // For leaf nodes, points to the next leaf
	value    E
}

func NewBPlusTreeWithValues[C cmp.Ordered, E Entry]() *BPlusTreeWithValues[C, E] {
	return &BPlusTreeWithValues[C, E]{root: &NodeWV[C, E]{isLeaf: true}}
}

func (t *BPlusTreeWithValues[C, E]) Insert(key C, value E) {
	if t.root == nil {
		t.root = &NodeWV[C, E]{isLeaf: true, value: value}
	}

	if len(t.root.keys) == maxKeys {
		newRoot := &NodeWV[C, E]{isLeaf: false}
		newRoot.children = append(newRoot.children, t.root)
		t.splitChild(newRoot, 0)
		t.root = newRoot
	}

	t.insertNonFull(t.root, key)
}

func (t *BPlusTreeWithValues[C, E]) insertNonFull(n *NodeWV[C, E], key C) {
	if n.isLeaf {
		i, _ := slices.BinarySearch(n.keys, key)
		n.keys = append(n.keys, (*new(C)))
		copy(n.keys[i+1:], n.keys[i:])
		n.keys[i] = key

	} else {
		i := len(n.keys) - 1
		for i >= 0 && key < n.keys[i] {
			i--
		}
		i++

		if len(n.children[i].keys) == maxKeys {
			t.splitChild(n, i)
			if key > n.keys[i] {
				i++
			}
		}

		t.insertNonFull(n.children[i], key)
	}
}

func (t *BPlusTreeWithValues[C, E]) splitChild(parent *NodeWV[C, E], i int) {
	child := parent.children[i]
	newChild := &NodeWV[C, E]{isLeaf: child.isLeaf}
	parent.keys = append(parent.keys, (*new(C)))
	copy(parent.keys[i+1:], parent.keys[i:])
	parent.keys[i] = child.keys[maxKeys/2]
	parent.children = append(parent.children, nil)
	copy(parent.children[i+2:], parent.children[i+1:])
	parent.children[i+1] = newChild
	newChild.keys = append(newChild.keys, child.keys[maxKeys/2+1:]...)
	child.keys = child.keys[:maxKeys/2]

	if !child.isLeaf {
		newChild.children = append(newChild.children, child.children[maxKeys/2+1:]...)
		child.children = child.children[:maxKeys/2+1]
	} else {
		newChild.next = child.next
		child.next = newChild
	}
}

func (t *BPlusTreeWithValues[C, E]) Search(key C) bool {
	return t.searchNode(t.root, key)
}

func (t *BPlusTreeWithValues[C, E]) SearchClosest(key C) (*NodeWV[C, E], bool) {
	found, node := t.searchClosest(t.root, nil, key)
	return node, found
}

func (t *BPlusTreeWithValues[C, E]) Delete(key C) {
	if t.root == nil {
		return
	}
	t.delete(t.root, key)
	if len(t.root.keys) == 0 && !t.root.isLeaf {
		t.root = t.root.children[0]
	}
}

func (t *BPlusTreeWithValues[C, E]) delete(n *NodeWV[C, E], key C) {
	if n.isLeaf {
		t.deleteFromLeaf(n, key)
	} else {
		t.deleteFromInternal(n, key)
	}
}

func (t *BPlusTreeWithValues[C, E]) deleteFromLeaf(n *NodeWV[C, E], key C) {
	i, _ := slices.BinarySearch(n.keys, key)

	if i < len(n.keys) && n.keys[i] == key {
		n.keys = append(n.keys[:i], n.keys[i+1:]...)
	}
}

func (t *BPlusTreeWithValues[C, E]) deleteFromInternal(n *NodeWV[C, E], key C) {
	i, _ := slices.BinarySearch(n.keys, key)

	if i < len(n.keys) && n.keys[i] == key {
		if len(n.children[i].keys) >= minKeys+1 {
			predecessor := t.getPredecessor(n, i)
			n.keys[i] = predecessor
			t.delete(n.children[i], predecessor)
		} else if len(n.children[i+1].keys) >= minKeys+1 {
			successor := t.getSuccessor(n, i)
			n.keys[i] = successor
			t.delete(n.children[i+1], successor)
		} else {
			t.mergeChildren(n, i)
			t.delete(n.children[i], key)
		}
	} else {
		childIndex := i
		if childIndex > len(n.keys) {
			childIndex = len(n.keys)
		}
		child := n.children[childIndex]
		if len(child.keys) == minKeys {
			t.fillChild(n, childIndex)
		}
		if childIndex > len(n.keys) {
			t.delete(n.children[len(n.keys)], key)
		} else {
			t.delete(n.children[childIndex], key)
		}
	}
}

func (t *BPlusTreeWithValues[C, E]) getPredecessor(n *NodeWV[C, E], index int) C {
	current := n.children[index]

	for !current.isLeaf {
		current = current.children[len(current.children)-1]
	}

	return current.keys[len(current.keys)-1]
}

func (t *BPlusTreeWithValues[C, E]) getSuccessor(n *NodeWV[C, E], index int) C {
	current := n.children[index+1]

	for !current.isLeaf {
		current = current.children[0]
	}

	return current.keys[0]
}

func (t *BPlusTreeWithValues[C, E]) fillChild(n *NodeWV[C, E], index int) {
	if index != 0 && len(n.children[index-1].keys) >= minKeys+1 {
		t.borrowFromPrev(n, index)
	} else if index != len(n.keys) && len(n.children[index+1].keys) >= minKeys+1 {
		t.borrowFromNext(n, index)
	} else {
		if index != len(n.keys) {
			t.mergeChildren(n, index)
		} else {
			t.mergeChildren(n, index-1)
		}
	}
}

func (t *BPlusTreeWithValues[C, E]) borrowFromPrev(n *NodeWV[C, E], index int) {
	child := n.children[index]
	sibling := n.children[index-1]

	child.keys = append([]C{n.keys[index-1]}, child.keys...)
	if !child.isLeaf {
		child.children = append([]*NodeWV[C, E]{sibling.children[len(sibling.children)-1]}, child.children...)
	}

	n.keys[index-1] = sibling.keys[len(sibling.keys)-1]

	sibling.keys = sibling.keys[:len(sibling.keys)-1]
	if !sibling.isLeaf {
		sibling.children = sibling.children[:len(sibling.children)-1]
	}
}

func (t *BPlusTreeWithValues[C, E]) borrowFromNext(n *NodeWV[C, E], index int) {
	child := n.children[index]
	sibling := n.children[index+1]

	child.keys = append(child.keys, n.keys[index])
	if !child.isLeaf {
		child.children = append(child.children, sibling.children[0])
	}

	n.keys[index] = sibling.keys[0]

	sibling.keys = sibling.keys[1:]
	if !sibling.isLeaf {
		sibling.children = sibling.children[1:]
	}
}

func (t *BPlusTreeWithValues[C, E]) mergeChildren(n *NodeWV[C, E], index int) {
	child := n.children[index]
	sibling := n.children[index+1]

	child.keys = append(child.keys, n.keys[index])
	child.keys = append(child.keys, sibling.keys...)
	if !child.isLeaf {
		child.children = append(child.children, sibling.children...)
	} else {
		child.next = sibling.next
	}

	n.keys = append(n.keys[:index], n.keys[index+1:]...)
	n.children = append(n.children[:index+1], n.children[index+2:]...)
}

func (t *BPlusTreeWithValues[C, E]) searchClosest(n, prev *NodeWV[C, E], key C) (bool, *NodeWV[C, E]) {
	if n == nil {
		return false, prev
	}

	i, _ := slices.BinarySearch(n.keys, key)

	if i < len(n.keys) && n.keys[i] == key {
		return true, n
	}

	if n.isLeaf {
		return false, prev
	}

	return t.searchClosest(n.children[i], n, key)
}

func (t *BPlusTreeWithValues[C, E]) searchNode(n *NodeWV[C, E], key C) bool {
	if n == nil {
		return false
	}
	i, _ := slices.BinarySearch(n.keys, key)

	if i < len(n.keys) && n.keys[i] == key {
		return true
	}
	if n.isLeaf {
		return false
	}
	return t.searchNode(n.children[i], key)
}

func (t *BPlusTreeWithValues[C, E]) Print() {
	t.printNode(t.root, 0)
}

func (t *BPlusTreeWithValues[C, E]) printNode(n *NodeWV[C, E], level int) {
	if n == nil {
		return
	}
	fmt.Printf("%sKeys: %v\n", getString(level), n.keys)
	if !n.isLeaf {
		for _, child := range n.children {
			t.printNode(child, level+1)
		}
	}
}
