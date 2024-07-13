package streedb

import (
	"cmp"
	"time"
)

type Comparable[O cmp.Ordered] interface {
	PrimaryIndex() string
	SecondaryIndex() string

	Adjacent(Comparable[O]) bool
	Equals(Comparable[O]) bool
	LessThan(Comparable[O]) bool

	Len() int
	Max() O
	Min() O
}

type Entry[O cmp.Ordered] interface {
	Comparable[O]

	Append(Entry[O]) error
	CreationTime() time.Time
	Merge(Entry[O]) error
	SetPrimaryIndex(string)

	Get(int) any
	Last() O
	Len() int
}

type Entries[O cmp.Ordered, E Entry[O]] interface {
	SecondaryIndices() []string
	Get(s string) E
	Last() E
	LenAll() int
	SecondaryIndicesLen() int
	Merge(Entries[O, E]) (Entries[O, E], error)
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

func (em EntriesMap[O, E]) Append(d E) {
	idx := d.SecondaryIndex()

	if _, ok := em[idx]; !ok {
		em[idx] = d
		return
	}

	em[idx].Append(d)
}

func (e EntriesMap[O, E]) Merge(d Entries[O, E]) (Entries[O, E], error) {
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

func (em EntriesMap[O, E]) Last() E {
	if len(em) == 0 {
		panic("no entries")
	}

	var last O
	var lastE E
	if len(em) > 1 {
		for _, es := range em {
			last = es.Last()
			lastE = es
			if len(em) == 1 {
				return es
			}
		}
	}

	for _, e := range em {
		if e.Last() > last {
			last = e.Last()
			lastE = e
		}
	}

	return lastE
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

func (e EntriesMap[O, E]) Find(d E) (E, bool) {
	// es, ok := e[d.SecondaryIndex()]
	// if !ok {
	// 	return (*new(E)), false
	// }
	//
	// return es.Find(d)
	panic("not implemented / unreachable")
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
