package fslocal

import (
	"errors"

	"github.com/sayden/streedb"
)

func NewLocalFileblock[T streedb.Entry](cfg *streedb.Config, meta *streedb.MetaFile[T], f streedb.Filesystem[T]) streedb.Fileblock[T] {
	return &localFileblock[T]{
		MetaFile: *meta,
		fs:       f,
		cfg:      cfg,
	}
}

type localFileblock[T streedb.Entry] struct {
	streedb.MetaFile[T]

	fs  streedb.Filesystem[T]
	cfg *streedb.Config
}

func (l *localFileblock[T]) Load() (streedb.Entries[T], error) {
	return l.fs.Load(l)
}

func (l *localFileblock[T]) Find(v streedb.Entry) (streedb.Entry, bool, error) {
	if !streedb.EntryFallsInsideMinMax(l.Metadata().Min, l.Metadata().Max, v) {
		return nil, false, nil
	}

	entries, err := l.Load()
	if err != nil {
		return nil, false, errors.Join(errors.New("error loading block"), err)
	}

	entry, found := entries.Find(v)
	return entry, found, nil
}

func (l *localFileblock[T]) Metadata() *streedb.MetaFile[T] {
	return &l.MetaFile
}

func (l *localFileblock[T]) SetFilesystem(fs streedb.Filesystem[T]) {
	l.fs = fs
}

func (l *localFileblock[T]) Close() error {
	return nil
}

func (l *localFileblock[T]) UUID() string {
	return l.Metadata().Uuid
}
