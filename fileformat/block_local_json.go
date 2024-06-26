package fileformat

import "github.com/sayden/streedb"

type localBlockJSON[T streedb.Entry] struct {
	streedb.Block[T]
}

func (l *localBlockJSON[T]) Find(v streedb.Entry) (streedb.Entry, bool, error) {
	if !streedb.EntryFallsInside[T](l, v) {
		return nil, false, nil
	}

	return nil, false, nil
}

func (l *localBlockJSON[T]) Compact() error {
	panic("implement me")
}

func (l *localBlockJSON[T]) GetMin() streedb.Entry {
	return l.MinVal
}

func (l *localBlockJSON[T]) GetMax() streedb.Entry {
	return l.MaxVal
}

func (l *localBlockJSON[T]) Close() error {
	l.BlockWriters.Close()
	return nil
}

func (l *localBlockJSON[T]) GetEntries() (streedb.Entries[T], error) {
	panic("implement me")
}

func (l *localBlockJSON[T]) Merge(a streedb.Metadata[T]) (streedb.Metadata[T], error) {
	panic("implement me")
}

func (l *localBlockJSON[T]) Remove() error {
	panic("implement me")
}
