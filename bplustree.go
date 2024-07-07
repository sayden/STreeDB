package streedb

import (
	"fmt"
	"sort"
)

const (
	maxKeys = 3 // Maximum number of keys in a node
	minKeys = maxKeys / 2
)

type BPlusTree struct {
	root *Node
}

type Node struct {
	keys     []int
	children []*Node
	isLeaf   bool
	next     *Node // For leaf nodes, points to the next leaf
}

func NewBPlusTree() *BPlusTree {
	return &BPlusTree{root: &Node{isLeaf: true}}
}

func (t *BPlusTree) Insert(key int) {
	if t.root == nil {
		t.root = &Node{isLeaf: true}
	}

	if len(t.root.keys) == maxKeys {
		newRoot := &Node{isLeaf: false}
		newRoot.children = append(newRoot.children, t.root)
		t.splitChild(newRoot, 0)
		t.root = newRoot
	}

	t.insertNonFull(t.root, key)
}

func (t *BPlusTree) insertNonFull(n *Node, key int) {
	if n.isLeaf {
		i := sort.SearchInts(n.keys, key)
		n.keys = append(n.keys, 0)
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

func (t *BPlusTree) splitChild(parent *Node, i int) {
	child := parent.children[i]
	newChild := &Node{isLeaf: child.isLeaf}
	parent.keys = append(parent.keys, 0)
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

func (t *BPlusTree) Search(key int) bool {
	return t.searchNode(t.root, key)
}

func (t *BPlusTree) SearchClosest(key int) (bool, *Node) {
	return t.searchClosest(t.root, nil, key)
}

func (t *BPlusTree) Delete(key int) {
	if t.root == nil {
		return
	}
	t.delete(t.root, key)
	if len(t.root.keys) == 0 && !t.root.isLeaf {
		t.root = t.root.children[0]
	}
}

func (t *BPlusTree) delete(n *Node, key int) {
	if n.isLeaf {
		t.deleteFromLeaf(n, key)
	} else {
		t.deleteFromInternal(n, key)
	}
}

func (t *BPlusTree) deleteFromLeaf(n *Node, key int) {
	i := sort.SearchInts(n.keys, key)
	if i < len(n.keys) && n.keys[i] == key {
		n.keys = append(n.keys[:i], n.keys[i+1:]...)
	}
}

func (t *BPlusTree) deleteFromInternal(n *Node, key int) {
	i := sort.SearchInts(n.keys, key)

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

func (t *BPlusTree) getPredecessor(n *Node, index int) int {
	current := n.children[index]

	for !current.isLeaf {
		current = current.children[len(current.children)-1]
	}

	return current.keys[len(current.keys)-1]
}

func (t *BPlusTree) getSuccessor(n *Node, index int) int {
	current := n.children[index+1]

	for !current.isLeaf {
		current = current.children[0]
	}

	return current.keys[0]
}

func (t *BPlusTree) fillChild(n *Node, index int) {
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

func (t *BPlusTree) borrowFromPrev(n *Node, index int) {
	child := n.children[index]
	sibling := n.children[index-1]

	child.keys = append([]int{n.keys[index-1]}, child.keys...)
	if !child.isLeaf {
		child.children = append([]*Node{sibling.children[len(sibling.children)-1]}, child.children...)
	}

	n.keys[index-1] = sibling.keys[len(sibling.keys)-1]

	sibling.keys = sibling.keys[:len(sibling.keys)-1]
	if !sibling.isLeaf {
		sibling.children = sibling.children[:len(sibling.children)-1]
	}
}

func (t *BPlusTree) borrowFromNext(n *Node, index int) {
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

func (t *BPlusTree) mergeChildren(n *Node, index int) {
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

func (t *BPlusTree) searchClosest(n, prev *Node, key int) (bool, *Node) {
	if n == nil {
		return false, prev
	}
	i := sort.SearchInts(n.keys, key)
	if i < len(n.keys) && n.keys[i] == key {
		return true, n
	}
	if n.isLeaf {
		return false, prev
	}
	return t.searchClosest(n.children[i], n, key)
}

func (t *BPlusTree) searchNode(n *Node, key int) bool {
	if n == nil {
		return false
	}
	i := sort.SearchInts(n.keys, key)
	if i < len(n.keys) && n.keys[i] == key {
		return true
	}
	if n.isLeaf {
		return false
	}
	return t.searchNode(n.children[i], key)
}

func (t *BPlusTree) Print() {
	t.printNode(t.root, 0)
}

func (t *BPlusTree) printNode(n *Node, level int) {
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

func getString(level int) string {
	s := ""
	for i := 0; i < level; i++ {
		s += "  "
	}
	return s
}
