package fss3

import (
	"errors"

	"github.com/sayden/streedb"
)

func NewS3Fileblock[T streedb.Entry](cfg *streedb.Config, meta *streedb.MetaFile[T], fs streedb.Filesystem[T]) streedb.Fileblock[T] {
	return &s3Fileblock[T]{
		cfg:      cfg,
		fs:       fs,
		MetaFile: *meta,
	}
}

type s3Fileblock[T streedb.Entry] struct {
	streedb.MetaFile[T]

	cfg *streedb.Config
	fs  streedb.Filesystem[T]
}

func (l *s3Fileblock[T]) Close() error {
	return nil
}

func (l *s3Fileblock[T]) Find(v streedb.Entry) (streedb.Entry, bool, error) {
	if !streedb.EntryFallsInsideMinMax(l.Min, l.Max, v) {
		return nil, false, nil
	}

	entries, err := l.Load()
	if err != nil {
		return nil, false, errors.Join(errors.New("error loading block"), err)
	}

	entry, found := entries.Find(v)

	return entry, found, nil
}

func (l *s3Fileblock[T]) Load() (streedb.Entries[T], error) {
	return l.fs.Load(l)
}

func (l *s3Fileblock[T]) Metadata() *streedb.MetaFile[T] {
	return &l.MetaFile
}

func (l *s3Fileblock[T]) SetFilesystem(fs streedb.Filesystem[T]) {
	l.fs = fs
}

func (l *s3Fileblock[T]) UUID() string {
	return l.Uuid
}
