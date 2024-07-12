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

	Get(int) any
	Last() O
	Len() int
}

type Entries[O cmp.Ordered, E Entry[O]] interface {
	Get(i int) E
	Last() E
	Len() int
	Merge(Entries[O, E]) (Entries[O, E], error)
}

func NewEntriesMap[O cmp.Ordered, E Entry[O]]() EntriesMap[O, E] {
	return make(EntriesMap[O, E])
}

type EntriesMap[O cmp.Ordered, E Entry[O]] map[string]E

func (e EntriesMap[O, E]) Merge(d Entries[O, E]) (Entries[O, E], error) {
	panic("not implemented / unreachable")
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

func (e EntriesMap[O, E]) Get(i int) E {
	panic("unreachable")
}

func (em EntriesMap[O, E]) Len() int {
	total := 0
	for _, es := range em {
		total += es.Len()
	}
	return total
}

func (em EntriesMap[O, E]) Append(d E) {
	if _, ok := em[d.SecondaryIndex()]; !ok {
		em[d.SecondaryIndex()] = d
		return
	}

	em[d.SecondaryIndex()].Append(d)
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
