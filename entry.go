package streedb

import (
	"cmp"
)

type Comparable[O cmp.Ordered] interface {
	PrimaryIndex() string
	SecondaryIndex() string

	IsAdjacent(Comparable[O]) bool
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

func NewEntriesMap[O cmp.Ordered, E Entry[O]]() EntriesMap[O, E] {
	return make(EntriesMap[O, E])
}

type EntriesMap[O cmp.Ordered, E Entry[O]] map[string]E

func (e EntriesMap[O, E]) SecondaryIndices() []string {
	indices := make([]string, 0, len(e))
	for k := range e {
		indices = append(indices, k)
	}
	return indices
}

func (em EntriesMap[O, E]) Append(entry E) {
	secondaryIdx := entry.SecondaryIndex()

	if _, ok := em[secondaryIdx]; !ok {
		em[secondaryIdx] = entry
		return
	}

	em[secondaryIdx].Append(entry)
}

func (e EntriesMap[O, E]) Merge(d EntriesMap[O, E]) (EntriesMap[O, E], error) {
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

func (em EntriesMap[O, E]) Min() O {
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

func (em EntriesMap[O, E]) Max() O {
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

func (e EntriesMap[O, E]) Get(secondary string) E {
	return e[secondary]
}

func (em EntriesMap[O, E]) LenAll() int {
	l := 0

	for _, es := range em {
		l += es.Len()
	}

	return l
}

func (em EntriesMap[O, E]) PrimaryIndex() string {
	if len(em) == 0 {
		return ""
	}

	for _, es := range em {
		return es.PrimaryIndex()
	}

	panic("unreachable")
}

func (em EntriesMap[O, E]) SecondaryIndicesLen() int {
	return len(em)
}

func (e EntriesMap[O, E]) Find(sIdx string, min, max O) (E, bool) {
	if _, ok := e[sIdx]; !ok {
		return *new(E), false
	}

	res, found := e[sIdx].Overlap(min, max)
	if !found {
		return *new(E), false
	}

	res_, ok := res.(E)
	return res_, ok
}

func NewSliceToMapWithMetadata[O cmp.Ordered, E Entry[O]](e []E, m *MetaFile[O]) EntriesMap[O, E] {
	em := NewEntriesMap[O, E]()
	for _, es := range e {
		es.SetPrimaryIndex(m.PrimaryIdx)
		em.Append(es)
	}

	return em
}

func NewSliceToMap[O cmp.Ordered, E Entry[O]](e []E) EntriesMap[O, E] {
	em := NewEntriesMap[O, E]()
	for _, es := range e {
		em.Append(es)
	}

	return em
}

func EntryFallsInsideMinMax[O cmp.Ordered](min, max, t O) bool {
	return (min < t || min == t) && (t < max || t == max)
}
