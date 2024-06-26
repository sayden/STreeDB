package main

type localBlockJSON[T Entry] struct {
	Block[T]
}

func (l *localBlockJSON[T]) Find(v Entry) (Entry, bool, error) {
	if !entryFallsInside[T](l, v) {
		return nil, false, nil
	}

	return nil, false, nil
}

func (l *localBlockJSON[T]) Compact() error {
	panic("implement me")
}

func (l *localBlockJSON[T]) GetMin() Entry {
	return l.MinVal
}

func (l *localBlockJSON[T]) GetMax() Entry {
	return l.MaxVal
}

func (l *localBlockJSON[T]) Close() error {
	l.BlockWriters.Close()
	return nil
}

func (l *localBlockJSON[T]) GetEntries() (Entries[T], error) {
	panic("implement me")
}

func (l *localBlockJSON[T]) Merge(a Metadata[T]) (Metadata[T], error) {
	panic("implement me")
}

func (l *localBlockJSON[T]) Remove() error {
	panic("implement me")
}
