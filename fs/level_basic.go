package fs

import (
	"errors"
	"fmt"

	db "github.com/sayden/streedb"
)

func NewBasicLevel[T db.Entry](cfg *db.Config, fs db.Filesystem[T]) db.Level[T] {
	level := &BasicLevel[T]{
		cfg:        cfg,
		Filesystem: fs,
		fileblocks: make([]db.Fileblock[T], 0, 10),
		min:        db.LinkedList[T]{},
		max:        db.LinkedList[T]{},
	}
	err := fs.OpenMetaFilesInLevel(level)
	if err != nil {
		panic(err)
	}

	return level
}

type BasicLevel[T db.Entry] struct {
	db.Filesystem[T]

	min db.LinkedList[T]
	max db.LinkedList[T]

	cfg *db.Config

	// TODO: Use a Btree instead of a slice
	fileblocks []db.Fileblock[T]
}

func (b *BasicLevel[T]) AppendFileblock(f db.Fileblock[T]) error {
	// when appending a block, we need to update the min and max
	meta := f.Metadata()
	b.updateMinMax(meta)

	b.fileblocks = append(b.fileblocks, f)

	return nil
}

func (b *BasicLevel[T]) Create(es db.Entries[T], meta *db.MetadataBuilder[T]) error {
	// Add filesystem related information to the metadata
	metadata, err := b.Filesystem.FillMetadataBuilder(meta).Build()
	if err != nil {
		return err
	}

	fileblock, err := b.Filesystem.Create(b.cfg, es, metadata)
	if err != nil {
		return err
	}

	// when appending a block, we need to update the min and max
	b.updateMinMax(&meta.MetaFile)

	b.fileblocks = append(b.fileblocks, fileblock)

	return nil
}

func (b *BasicLevel[T]) RemoveFile(f db.Fileblock[T]) error {
	idx := 0

	meta := f.Metadata()
	// search for block
	for i, block := range b.fileblocks {
		if block.Metadata().Uuid == meta.Uuid {
			// remove block
			if err := b.Remove(block); err != nil {
				return err
			}
			b.removeMinMax(block.Metadata())
			idx = i
			break
		}
	}

	// remove block from slice
	b.fileblocks = append(b.fileblocks[:idx], b.fileblocks[idx+1:]...)

	return nil
}

func (b *BasicLevel[T]) entryFallsInside(d T) bool {
	if minV, found := b.min.Head(); !found {
		return false
	} else if d.LessThan(minV) {
		return false
	}

	if maxV, foundMax := b.max.Head(); !foundMax {
		return false
	} else if !d.LessThan(maxV) {
		return false
	}

	return true
}

func (b *BasicLevel[T]) Find(d T) (db.Entry, bool, error) {
	if !b.entryFallsInside(d) {
		return nil, false, nil
	}

	// iterate through each block
	var (
		entry     db.Entry
		fileblock db.Fileblock[T]
		entries   db.Entries[T]
		err       error
		found     bool
	)

	for _, fileblock = range b.fileblocks {
		if found = fileblock.Find(d); found {
			if entries, err = fileblock.Load(); err != nil {
				return nil, false, errors.Join(fmt.Errorf("error finding value %v in block: ", d), err)
			}

			if entry, found = entries.Find(d); found {
				return entry, found, nil
			}
		}
	}

	return nil, false, nil
}

func (b *BasicLevel[T]) Close() error {
	for _, block := range b.fileblocks {
		block.Close()
	}

	return nil
}

func (b *BasicLevel[T]) Fileblocks() []db.Fileblock[T] {
	return b.fileblocks
}

func (b *BasicLevel[T]) updateMinMax(in *db.MetaFile[T]) {
	b.min.SetMin(in.Min)
	b.max.SetMax(in.Max)
}

func (b *BasicLevel[T]) removeMinMax(meta *db.MetaFile[T]) {
	b.min.Remove(meta.Min)
	b.max.Remove(meta.Max)
}
