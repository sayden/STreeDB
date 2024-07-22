package streedb

import (
	"cmp"
)

type Indexer interface {
	PrimaryIndex() string
	SecondaryIndex() string
}

type Comparable[O cmp.Ordered] interface {
	Indexer
	UUID() string

	Equals(Comparable[O]) bool
	LessThan(Comparable[O]) bool
}

type Entry[O cmp.Ordered] interface {
	Comparable[O]

	Append(Entry[O]) error
	Merge(Entry[O]) error
	SetPrimaryIndex(string)
	Sort()

	Last() O
	Len() int
	Max() O
	Min() O

	Overlap(O, O) (Entry[O], bool)
}

func NewEntriesMap[O cmp.Ordered]() EntriesMap[O] {
	return make(EntriesMap[O])
}

type EntriesMap[O cmp.Ordered] map[string]Entry[O]

func (e EntriesMap[O]) SecondaryIndices() []string {
	indices := make([]string, 0, len(e))
	for k := range e {
		indices = append(indices, k)
	}
	return indices
}

func (em EntriesMap[O]) Append(entry Entry[O]) {
	secondaryIdx := entry.SecondaryIndex()

	if _, ok := em[secondaryIdx]; !ok {
		em[secondaryIdx] = entry
		return
	}

	em[secondaryIdx].Append(entry)
}

func (e EntriesMap[O]) Merge(d EntriesMap[O]) (EntriesMap[O], error) {
	idxs := d.SecondaryIndices()

	for _, idx := range idxs {
		if _, ok := e[idx]; !ok {
			e[idx] = d.Get(idx)
		} else {
			e[idx].Merge(d.Get(idx))
		}
	}

	return e, nil
}

func (em EntriesMap[O]) Min() O {
	if len(em) == 0 {
		panic("no entries")
	}

	var min *O
	for _, e := range em {
		if min == nil {
			m := e.Min()
			min = &m
			continue
		}
		if e.Min() < *min {
			*min = e.Min()
		}
	}

	return *min
}

func (em EntriesMap[O]) Max() O {
	if len(em) == 0 {
		panic("no entries")
	}

	var max *O
	for _, e := range em {
		if max == nil {
			m := e.Max()
			max = &m
			continue
		}
		if e.Max() > *max {
			*max = e.Max()
		}
	}

	return *max
}

func (e EntriesMap[O]) Get(secondary string) Entry[O] {
	return e[secondary]
}

func (em EntriesMap[O]) LenAll() int {
	l := 0

	for _, es := range em {
		l += es.Len()
	}

	return l
}

func (em EntriesMap[O]) PrimaryIndex() string {
	if len(em) == 0 {
		return ""
	}

	for _, es := range em {
		return es.PrimaryIndex()
	}

	panic("unreachable")
}

func (em EntriesMap[O]) SecondaryIndicesLen() int {
	return len(em)
}

func (e EntriesMap[O]) Find(sIdx string, min, max O) (EntryIterator[O], bool) {
	if sIdx == "" {
		entries := make([]Entry[O], 0)
		for _, entry := range e {
			if _, isOverlapped := entry.Overlap(min, max); isOverlapped {
				entries = append(entries, entry)
			}
		}

		return NewListIterator(entries), len(entries) > 0
	}

	if _, ok := e[sIdx]; !ok {
		return nil, false
	}

	res, found := e[sIdx].Overlap(min, max)
	if !found {
		return nil, false
	}

	return NewSingleItemIterator(res), true
}

func NewSliceToMapWithMetadata[O cmp.Ordered, E Entry[O]](e []E, m *MetaFile[O]) EntriesMap[O] {
	em := NewEntriesMap[O]()
	for _, es := range e {
		es.SetPrimaryIndex(m.PrimaryIdx)
		em.Append(es)
	}

	return em
}

func NewSliceToMap[O cmp.Ordered, E Entry[O]](e []E) EntriesMap[O] {
	em := NewEntriesMap[O]()
	for _, es := range e {
		em.Append(es)
	}

	return em
}

func EntryFallsInsideMinMax[O cmp.Ordered](min, max, t O) bool {
	return (min < t || min == t) && (t < max || t == max)
}
