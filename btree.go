package streedb

import (
	"cmp"

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
	val *LinkedList[O, *Fileblock[O]]
}

type BtreeWrapper[O cmp.Ordered] struct {
	*btree.BTreeG[*btreeItem[O]]
}

func (b *BtreeWrapper[O]) Get(o O) (*LinkedList[O, *Fileblock[O]], bool) {
	item, found := b.BTreeG.Get(&btreeItem[O]{key: o})
	if item == nil {
		return nil, false
	}
	return item.val, found
}

func (b *BtreeWrapper[O]) Upsert(key O, value *Fileblock[O]) bool {
	ll := &LinkedList[O, *Fileblock[O]]{}
	ll.SetMin(value)
	old, found := b.ReplaceOrInsert(&btreeItem[O]{key: key, val: ll})
	if !found {
		return found
	}

	old.val.SetMin(value)
	b.ReplaceOrInsert(old)

	return true
}

type BtreeFilter[O cmp.Ordered] func(*Fileblock[O]) bool

func PrimaryIndexFilter[O cmp.Ordered](pIdx string) BtreeFilter[O] {
	return func(c *Fileblock[O]) bool {
		return c.PrimaryIndex() == pIdx
	}
}

func SecondaryIndexFilter[O cmp.Ordered](sIdx string) BtreeFilter[O] {
	return func(c *Fileblock[O]) bool {
		return c.SecondaryIndex() == sIdx
	}
}

func (b *BtreeWrapper[O]) AscendRange(pIdx, sIdx string, min, max O) ([]*Fileblock[O], bool, error) {
	result := make([]*Fileblock[O], 0)

	b.BTreeG.AscendRange(&btreeItem[O]{key: min}, &btreeItem[O]{key: max}, func(item *btreeItem[O]) bool {
		if item.val.head.value.PrimaryIndex() == pIdx {
			for _, c := range item.val.head.value.Rows {
				if c.SecondaryIdx == sIdx {
					result = append(result, item.val.head.value)
					return true
				}
			}

			return false
		}

		return false
	})

	return result, len(result) > 0, nil
}

func (b *BtreeWrapper[O]) Remove(key O, value *Fileblock[O]) bool {
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
