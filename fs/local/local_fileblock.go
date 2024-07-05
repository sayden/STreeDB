package fslocal

import (
	db "github.com/sayden/streedb"
)

func NewLocalFileblock[T db.Entry](cfg *db.Config, meta *db.MetaFile[T], filesystem db.Filesystem[T]) db.Fileblock[T] {
	return &localFileblock[T]{
		MetaFile:   *meta,
		cfg:        cfg,
		filesystem: filesystem,
	}
}

type localFileblock[T db.Entry] struct {
	db.MetaFile[T]
	cfg        *db.Config
	filesystem db.Filesystem[T]
}

func (l *localFileblock[T]) Load() (db.Entries[T], error) {
	return l.filesystem.Load(l)
}

func (l *localFileblock[T]) Find(v db.Entry) bool {
	return db.EntryFallsInsideMinMax(l.Min, l.Max, v)
}

func (l *localFileblock[T]) Metadata() *db.MetaFile[T] {
	return &l.MetaFile
}

func (l *localFileblock[T]) Close() error {
	return nil
}

func (l *localFileblock[T]) UUID() string {
	return l.Metadata().Uuid
}
