package streedb

import (
	"cmp"
	"errors"

	"github.com/google/btree"
)

func LLFComp[O cmp.Ordered](a, b *btreeItem[O]) bool {
	return a.key < b.key
}

func NewBtreeIndex[O cmp.Ordered](degree int, less btree.LessFunc[*btreeItem[O]]) *BtreeWrapper[O] {
	return &BtreeWrapper[O]{BTreeG: btree.NewG[*btreeItem[O]](2, LLFComp)}
}

type btreeItem[O cmp.Ordered] struct {
	key O
	val *LinkedList[O, Comparable[O]]
}

type BtreeWrapper[O cmp.Ordered] struct {
	*btree.BTreeG[*btreeItem[O]]
}

func (b *BtreeWrapper[O]) Get(o O) (*LinkedList[O, Comparable[O]], bool) {
	item, found := b.BTreeG.Get(&btreeItem[O]{key: o})
	if item == nil {
		return nil, false
	}
	return item.val, found
}

func (b *BtreeWrapper[O]) Upsert(key O, value Comparable[O]) bool {
	ll := &LinkedList[O, Comparable[O]]{}
	ll.SetMin(value)
	old, found := b.ReplaceOrInsert(&btreeItem[O]{key: key, val: ll})
	if !found {
		return found
	}

	old.val.SetMin(value)
	b.ReplaceOrInsert(old)

	return true
}

type BtreeFilter[O cmp.Ordered] func(Comparable[O]) bool

func PrimaryIndexFilter[O cmp.Ordered](pIdx string) BtreeFilter[O] {
	return func(c Comparable[O]) bool {
		return c.PrimaryIndex() == pIdx
	}
}

func SecondaryIndexFilter[O cmp.Ordered](sIdx string) BtreeFilter[O] {
	return func(c Comparable[O]) bool {
		return c.SecondaryIndex() == sIdx
	}
}

// func (b *BtreeWrapper[O]) AscendRange2(min, max O, filters ...BtreeFilter[O]) (Comparable[O], bool, error) {
// 	b.BTreeG.AscendRange(&btreeItem[O]{key: min}, &btreeItem[O]{key: max}, func(item *btreeItem[O]) bool {

// 	})

// 	return nil, false, errors.New("not implemented")
// }

func (b *BtreeWrapper[O]) AscendRange(pIdx, sIdx string, min, max O) (Comparable[O], bool, error) {
	b.BTreeG.AscendRange(&btreeItem[O]{key: min}, &btreeItem[O]{key: max}, func(item *btreeItem[O]) bool {
		if item.val.head.value.PrimaryIndex() == pIdx && item.val.head.value.SecondaryIndex() == sIdx {
			return false
		}
		return true
	})
	return nil, false, errors.New("not implemented")
}

func (b *BtreeWrapper[O]) Remove(key O, value Comparable[O]) bool {
	btItem, found := b.BTreeG.Get(&btreeItem[O]{key: key})
	if !found {
		return false
	}

	btItem.val.Remove(value)
	if btItem.val.head == nil {
		_, found := b.BTreeG.Delete(btItem)
		return found
	}

	return true
}
