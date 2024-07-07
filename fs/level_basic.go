package fs

import (
	"errors"
	"fmt"

	db "github.com/sayden/streedb"
)

func NewBasicLevel[T db.Entry](cfg *db.Config, fs db.Filesystem[T], levels db.Levels[T]) db.Level[T] {
	level := &BasicLevel[T]{
		cfg:                cfg,
		filesystem:         fs,
		fileblocks:         make([]*db.Fileblock[T], 0, 10),
		min:                db.LinkedList[T]{},
		max:                db.LinkedList[T]{},
		fileblockListeners: []db.FileblockListener[T]{levels},
	}
	// add self to the listeners
	level.fileblockListeners = append(level.fileblockListeners, level)

	err := fs.OpenMetaFilesInLevel(level.fileblockListeners)
	if err != nil {
		panic(err)
	}

	return level
}

type BasicLevel[T db.Entry] struct {
	filesystem db.Filesystem[T]

	min db.LinkedList[T]
	max db.LinkedList[T]

	cfg                *db.Config
	fileblockListeners []db.FileblockListener[T]

	// TODO: Use a Btree instead of a slice
	fileblocks []*db.Fileblock[T]
}

func (b *BasicLevel[T]) OnFileblockRemoved(block *db.Fileblock[T]) {
	idx := 0

	meta := block.Metadata()
	// search for block
	for i, block := range b.fileblocks {
		if block.Metadata().Uuid == meta.Uuid {
			// remove block
			b.removeMinMax(block.Metadata())
			idx = i
			break
		}
	}

	// remove block from slice
	b.fileblocks = append(b.fileblocks[:idx], b.fileblocks[idx+1:]...)
}

func (b *BasicLevel[T]) OnNewFileblock(f *db.Fileblock[T]) {
	meta := f.Metadata()
	b.updateMinMax(meta)

	b.fileblocks = append(b.fileblocks, f)
}

func (b *BasicLevel[T]) Create(es db.Entries[T], meta *db.MetadataBuilder[T]) (*db.Fileblock[T], error) {
	// Add filesystem related information to the metadata
	metadata, err := b.filesystem.FillMetadataBuilder(meta).Build()
	if err != nil {
		return nil, err
	}

	fileblock, err := b.filesystem.Create(b.cfg, es, metadata, b.fileblockListeners)
	if err != nil {
		return nil, err
	}

	return fileblock, nil
}

func (b *BasicLevel[T]) RemoveFile(f *db.Fileblock[T]) error {
	return b.filesystem.Remove(f, b.fileblockListeners)
}

func (b *BasicLevel[T]) FindFileblock(d T) (*db.Fileblock[T], bool, error) {
	if !b.entryFallsInside(d) {
		return nil, false, nil
	}

	for _, fileblock := range b.fileblocks {
		if found := fileblock.Find(d); found {
			return fileblock, found, nil
		}
	}

	return nil, false, nil
}

func (b *BasicLevel[T]) Find(d T) (db.Entry, bool, error) {
	if !b.entryFallsInside(d) {
		return nil, false, nil
	}

	// iterate through each block
	var (
		entry     db.Entry
		fileblock *db.Fileblock[T]
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

func (b *BasicLevel[T]) Fileblocks() []*db.Fileblock[T] {
	return b.fileblocks
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

func (b *BasicLevel[T]) updateMinMax(in *db.MetaFile[T]) {
	b.min.SetMin(in.Min)
	b.max.SetMax(in.Max)
}

func (b *BasicLevel[T]) removeMinMax(meta *db.MetaFile[T]) {
	b.min.Remove(meta.Min)
	b.max.Remove(meta.Max)
}
