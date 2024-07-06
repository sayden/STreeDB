package streedb

type Entry interface {
	Cmp(a, b Entry) int
	LessThan(Entry) bool
	Equals(Entry) bool
	Adjacent(Entry) bool
}

type Entries[T Entry] []T

func (e Entries[T]) Find(d Entry) (Entry, bool) {
	lo, hi := 0, len(e)-1
	for lo <= hi {
		mid := lo + (hi-lo)/2
		if e[mid].Equals(d) {
			return e[mid], true
		} else if e[mid].LessThan(d) {
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}

	return nil, false
}

func (t Entries[T]) Len() int {
	return len(t)
}

func (t Entries[T]) Less(i, j int) bool {
	return t[i].LessThan(t[j])
}

func (t Entries[T]) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

func (t Entries[T]) Min() Entry {
	return t[0]
}

func (t Entries[T]) Max() Entry {
	return t[len(t)-1]
}

func EntryFallsInsideMinMax(min, max, t Entry) bool {
	return (min.LessThan(t) || min.Equals(t)) && (t.LessThan(max) || t.Equals(max))
}

func EntryCmp[T Entry](a, b T) int {
	if a.Equals(b) {
		return 0
	}

	if a.LessThan(b) {
		return -1
	}

	return 1
}
