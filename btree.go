package streedb

import (
	"cmp"

	"github.com/google/btree"
)

type EntryFilterKind int

const (
	PrimaryIndexFilterKind EntryFilterKind = iota
	SecondaryIndexFilterKind
)

type EntryFilter interface {
	Filter(Indexer) bool
	Kind() EntryFilterKind
}

func AlwaysTrueIndexFilter() EntryFilter {
	return &alwaysTrueIndexFilter{}
}

type alwaysTrueIndexFilter struct{}

func (a *alwaysTrueIndexFilter) Filter(c Indexer) bool {
	return true
}

func (a *alwaysTrueIndexFilter) Kind() EntryFilterKind {
	return PrimaryIndexFilterKind
}

func PrimaryIndexFilter(pIdx string) EntryFilter {
	return &primaryIndexFilter{pIdx: pIdx}
}

type primaryIndexFilter struct{ pIdx string }

func (p *primaryIndexFilter) Filter(c Indexer) bool {
	return p.pIdx == "" || c.PrimaryIndex() == p.pIdx
}

func (p *primaryIndexFilter) Kind() EntryFilterKind {
	return PrimaryIndexFilterKind
}

func SecondaryIndexFilter[O cmp.Ordered](sIdx string) EntryFilter {
	return &secondaryIndexFilter[O]{sIdx: sIdx}
}

type secondaryIndexFilter[O cmp.Ordered] struct{ sIdx string }

func (p *secondaryIndexFilter[O]) Filter(c Indexer) bool {
	return p.sIdx == "" || c.SecondaryIndex() == p.sIdx
}

func (p *secondaryIndexFilter[O]) Kind() EntryFilterKind {
	return SecondaryIndexFilterKind
}

func LLFComp[O cmp.Ordered](a, b *BtreeItem[O]) bool {
	return a.Key < b.Key
}

func NewBtreeIndex[O cmp.Ordered](degree int, less btree.LessFunc[*BtreeItem[O]]) *BtreeIndex[O] {
	return &BtreeIndex[O]{BTreeG: btree.NewG[*BtreeItem[O]](2, LLFComp)}
}

type BtreeItem[O cmp.Ordered] struct {
	Key O
	Val *LinkedList[O, *Fileblock[O]]
}

type BtreeIndex[O cmp.Ordered] struct {
	*btree.BTreeG[*BtreeItem[O]]
}

func (b *BtreeIndex[O]) Get(o O) (*LinkedList[O, *Fileblock[O]], bool) {
	item, found := b.BTreeG.Get(&BtreeItem[O]{Key: o})
	if item == nil {
		return nil, false
	}

	return item.Val, found
}

func (b *BtreeIndex[O]) Upsert(key O, value *Fileblock[O]) bool {
	ll := &LinkedList[O, *Fileblock[O]]{}
	ll.SetMin(value)
	old, found := b.ReplaceOrInsert(&BtreeItem[O]{Key: key, Val: ll})
	if !found {
		return found
	}

	old.Val.SetMin(value)
	b.ReplaceOrInsert(old)

	return true
}

func (b *BtreeIndex[O]) AscendRangeWithFilters(min, max O, filters ...EntryFilter) (EntryIterator[O], bool, error) {
	pFilters := make([]EntryFilter, 0)
	for _, filter := range filters {
		if filter.Kind() == PrimaryIndexFilterKind {
			pFilters = append(pFilters, filter)
		}
	}

	// When no primary index filter is provided, we provice a mock one
	if len(pFilters) == 0 {
		pFilters = append(pFilters, AlwaysTrueIndexFilter())
	}

	result, found, err := b.ascendRangeWithFilters(min, max, pFilters...)
	if err != nil {
		return nil, false, err
	}

	return newIteratorWithFilters(result, filters), found, nil
}

func (b *BtreeIndex[O]) ascendRangeWithFilters(min, max O, filters ...EntryFilter) ([]*Fileblock[O], bool, error) {
	result := make([]*Fileblock[O], 0)

	b.AscendRange(
		&BtreeItem[O]{Key: min},
		&BtreeItem[O]{Key: max},
		func(item *BtreeItem[O]) bool {
			for next := item.Val.head; next != nil; next = next.Next {
				fileblock := next.Val
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

func (b *BtreeIndex[O]) ascendRange(pIdx, sIdx string, min, max O) ([]*Fileblock[O], bool, error) {
	result := make([]*Fileblock[O], 0)

	b.AscendRange(
		&BtreeItem[O]{Key: min},
		&BtreeItem[O]{Key: max},
		func(item *BtreeItem[O]) bool {
			for next := item.Val.head; next != nil; next = next.Next {
				if pIdx == "" || next.Val.PrimaryIndex() == pIdx {
					for _, c := range next.Val.Rows {
						if sIdx == "" || c.SecondaryIdx == sIdx {
							result = append(result, next.Val)
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
	btItem, found := b.BTreeG.Get(&BtreeItem[O]{Key: key})
	if !found {
		return false
	}

	btItem.Val.Remove(value)
	if btItem.Val.head == nil {
		_, found := b.Delete(btItem)
		return found
	}

	return true
}
