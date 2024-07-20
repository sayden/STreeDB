package streedb

import (
	"cmp"

	"github.com/google/btree"
)

type EntryFilter interface {
	Filter(Indexer) bool
}

func PrimaryIndexFilter(pIdx string) EntryFilter {
	return &primaryIndexFilter{pIdx: pIdx}
}

type primaryIndexFilter struct{ pIdx string }

func (p *primaryIndexFilter) Filter(c Indexer) bool {
	return c.PrimaryIndex() == p.pIdx
}

func SecondaryIndexFilter[O cmp.Ordered](sIdx string) EntryFilter {
	return &secondaryIndexFilter[O]{sIdx: sIdx}
}

type secondaryIndexFilter[O cmp.Ordered] struct{ sIdx string }

func (p *secondaryIndexFilter[O]) Filter(c Indexer) bool {
	switch t := c.(type) {
	case *Fileblock[O]:
		return false
	case Entry[O]:
		return t.SecondaryIndex() == p.sIdx
	}

	return false
}

func LLFComp[O cmp.Ordered](a, b *btreeItem[O]) bool {
	return a.key < b.key
}

func NewBtreeIndex[O cmp.Ordered](degree int, less btree.LessFunc[*btreeItem[O]]) *BtreeIndex[O] {
	return &BtreeIndex[O]{BTreeG: btree.NewG[*btreeItem[O]](2, LLFComp)}
}

type btreeItem[O cmp.Ordered] struct {
	key O
	val *LinkedList[O, *Fileblock[O]]
}

type BtreeIndex[O cmp.Ordered] struct {
	*btree.BTreeG[*btreeItem[O]]
}

func (b *BtreeIndex[O]) Get(o O) (*LinkedList[O, *Fileblock[O]], bool) {
	item, found := b.BTreeG.Get(&btreeItem[O]{key: o})
	if item == nil {
		return nil, false
	}

	return item.val, found
}

func (b *BtreeIndex[O]) Upsert(key O, value *Fileblock[O]) bool {
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

func (b *BtreeIndex[O]) AscendRangeWithFilters(min, max O, filters ...EntryFilter) (EntryIterator[O], bool, error) {
	result, found, err := b.ascendRangeWithFilters(min, max, filters...)
	if err != nil {
		return nil, false, err
	}

	return newIteratorWithFilters(result, filters), found, nil
}

func (b *BtreeIndex[O]) ascendRangeWithFilters(min, max O, filters ...EntryFilter) ([]*Fileblock[O], bool, error) {
	result := make([]*Fileblock[O], 0)

	b.BTreeG.AscendRange(
		&btreeItem[O]{key: min},
		&btreeItem[O]{key: max},
		func(item *btreeItem[O]) bool {
			for next := item.val.head; next != nil; next = next.next {
				fileblock := next.value
				for _, filter := range filters {
					if filter.Filter(fileblock) {
						result = append(result, fileblock)
						continue
					}
				}
			}

			return true
		})

	return result, len(result) > 0, nil
}

func (b *BtreeIndex[O]) AscendRange(pIdx, sIdx string, min, max O) (EntryIterator[O], bool, error) {
	result, found, err := b.ascendRange(pIdx, sIdx, min, max)
	if err != nil {
		return nil, false, err
	}

	return newIteratorWithData(result), found, nil
}

func (b *BtreeIndex[O]) ascendRange(pIdx, sIdx string, min, max O) ([]*Fileblock[O], bool, error) {
	result := make([]*Fileblock[O], 0)

	b.BTreeG.AscendRange(
		&btreeItem[O]{key: min},
		&btreeItem[O]{key: max},
		func(item *btreeItem[O]) bool {
			for next := item.val.head; next != nil; next = next.next {
				if pIdx == "" || next.value.PrimaryIndex() == pIdx {
					for _, c := range next.value.Rows {
						if sIdx == "" || c.SecondaryIdx == sIdx {
							result = append(result, next.value)
							break
						}
					}
				}
			}

			return true
		})

	return result, len(result) > 0, nil
}

func (b *BtreeIndex[O]) Remove(key O, value *Fileblock[O]) bool {
	btItem, found := b.BTreeG.Get(&btreeItem[O]{key: key})
	if !found {
		return false
	}

	btItem.val.Remove(value)
	if btItem.val.head == nil {
		_, found := b.Delete(btItem)
		return found
	}

	return true
}
