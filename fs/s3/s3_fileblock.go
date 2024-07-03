package fss3

import (
	db "github.com/sayden/streedb"
)

func NewS3Fileblock[T db.Entry](cfg *db.Config, meta *db.MetaFile[T], filesystem db.Filesystem[T]) db.Fileblock[T] {
	return &s3Fileblock[T]{
		cfg:      cfg,
		MetaFile: *meta,
		fs:       filesystem,
	}
}

type s3Fileblock[T db.Entry] struct {
	db.MetaFile[T]

	cfg *db.Config
	fs  db.Filesystem[T]
}

func (l *s3Fileblock[T]) Close() error {
	return nil
}

func (l *s3Fileblock[T]) Find(v db.Entry) bool {
	return !db.EntryFallsInsideMinMax(l.Min, l.Max, v)
}

func (l *s3Fileblock[T]) Load() (db.Entries[T], error) {
	return l.fs.Load(l)
}

func (l *s3Fileblock[T]) Metadata() *db.MetaFile[T] {
	return &l.MetaFile
}

func (l *s3Fileblock[T]) UUID() string {
	return l.Uuid
}
