package streedb

import (
	"cmp"

	"github.com/puzpuzpuz/xsync/v3"
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

func NewEntriesMap[O cmp.Ordered]() *EntriesMap[O] {
	return &EntriesMap[O]{
		MapOf: xsync.NewMapOf[string, Entry[O]](),
	}
}

type EntriesMap[O cmp.Ordered] struct {
	*xsync.MapOf[string, Entry[O]]
}

func (em *EntriesMap[O]) SecondaryIndices() []string {
	indices := make([]string, 0)
	em.Range(func(key string, value Entry[O]) bool {
		indices = append(indices, key)
		return true
	})

	return indices
}

func (em *EntriesMap[O]) Append(entry Entry[O]) {
	secondaryIdx := entry.SecondaryIndex()

	oldEntry, found := em.LoadOrStore(secondaryIdx, entry)
	if !found {
		return
	}

	err := oldEntry.Append(entry)
	if err != nil {
		panic(err)
	}

	// em.Store(secondaryIdx, oldEntry)
}

// FIXME: This is doing nothing at the moment
func (em *EntriesMap[O]) Merge(d *EntriesMap[O]) (*EntriesMap[O], error) {
	dest := NewEntriesMap[O]()

	d.Range(func(key string, value Entry[O]) bool {
		dest.Append(value)
		return true
	})

	em.Range(func(key string, value Entry[O]) bool {
		dest.Append(value)
		return true
	})

	return dest, nil
}

func (em *EntriesMap[O]) Min() O {
	var min *O
	em.Range(func(key string, value Entry[O]) bool {
		if min == nil {
			m := value.Min()
			min = &m
			return true
		}

		if value.Min() < *min {
			*min = value.Min()
		}

		return true
	})

	return *min
}

func (em *EntriesMap[O]) Max() O {
	var max *O
	em.Range(func(key string, val Entry[O]) bool {
		if max == nil {
			m := val.Max()
			max = &m
			return true
		}
		if val.Max() > *max {
			*max = val.Max()
		}

		return true
	})

	return *max
}

func (em *EntriesMap[O]) Get(secondary string) Entry[O] {
	val, _ := em.Load(secondary)
	return val
}

func (em *EntriesMap[O]) LenAll() int {
	l := 0

	em.Range(func(key string, value Entry[O]) bool {
		l += value.Len()
		return true
	})

	return l
}

func (em *EntriesMap[O]) PrimaryIndex() string {
	res := ""
	em.Range(func(key string, value Entry[O]) bool {
		res = value.PrimaryIndex()
		return false
	})

	return res
}

func (em *EntriesMap[O]) SecondaryIndicesLen() int {
	count := 0
	em.Range(func(key string, value Entry[O]) bool {
		count++
		return true
	})
	return count
}

func (em *EntriesMap[O]) Find(sIdx string, min, max O) (EntryIterator[O], bool) {

	if sIdx == "" {
		entries := make([]Entry[O], 0)
		em.Range(func(key string, entry Entry[O]) bool {
			if _, isOverlapped := entry.Overlap(min, max); isOverlapped {
				entries = append(entries, entry)
			}
			return true
		})

		return NewListIterator(entries), len(entries) > 0
	}

	data, found := em.Load(sIdx)
	if !found {
		return nil, false
	}

	res, found := data.Overlap(min, max)
	if !found {
		return nil, false
	}

	return NewSingleItemIterator(res), true
}

func NewSliceToMapWithMetadata[O cmp.Ordered, E Entry[O]](e []E, m *MetaFile[O]) *EntriesMap[O] {
	em := NewEntriesMap[O]()
	for _, es := range e {
		es.SetPrimaryIndex(m.PrimaryIdx)
		em.Append(es)
	}

	return em
}

func NewSliceToMap[O cmp.Ordered, E Entry[O]](e []E) *EntriesMap[O] {
	em := NewEntriesMap[O]()
	for _, es := range e {
		em.Append(es)
	}

	return em
}

func EntryFallsInsideMinMax[O cmp.Ordered](min, max, t O) bool {
	return (min < t || min == t) && (t < max || t == max)
}
