package fs

import (
	"cmp"
	"errors"
	"fmt"

	db "github.com/sayden/streedb"
)

func NewBasicLevel[O cmp.Ordered, E db.Entry[O]](cfg *db.Config, fs db.Filesystem[O, E], levels *MultiFsLevels[O, E]) *BasicLevel[O, E] {
	level := &BasicLevel[O, E]{
		cfg:                cfg,
		filesystem:         fs,
		fileblocks:         make([]*db.Fileblock[O, E], 0, 10),
		min:                db.LinkedList[O, E]{},
		max:                db.LinkedList[O, E]{},
		fileblockListeners: []db.FileblockListener[O, E]{levels},
	}
	// add self to the listeners
	level.fileblockListeners = append(level.fileblockListeners, level)

	err := fs.OpenMetaFilesInLevel(level.fileblockListeners)
	if err != nil {
		panic(err)
	}

	return level
}

type BasicLevel[O cmp.Ordered, E db.Entry[O]] struct {
	filesystem db.Filesystem[O, E]

	min db.LinkedList[O, E]
	max db.LinkedList[O, E]

	cfg                *db.Config
	fileblockListeners []db.FileblockListener[O, E]

	// TODO: Use a Btree instead of a slice
	fileblocks []*db.Fileblock[O, E]
}

func (b *BasicLevel[O, T]) OnFileblockRemoved(block *db.Fileblock[O, T]) {
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

func (b *BasicLevel[O, T]) OnFileblockCreated(f *db.Fileblock[O, T]) {
	meta := f.Metadata()
	b.updateMinMax(meta)

	b.fileblocks = append(b.fileblocks, f)
}

func (b *BasicLevel[O, T]) Create(es db.Entries[O, T], builder *db.MetadataBuilder[O]) (*db.Fileblock[O, T], error) {
	fileblock, err := b.filesystem.Create(b.cfg, es, builder, b.fileblockListeners)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("error creating block at level: "), err)
	}

	return fileblock, nil
}

func (b *BasicLevel[O, T]) RemoveFile(f *db.Fileblock[O, T]) error {
	return b.filesystem.Remove(f, b.fileblockListeners)
}

func (b *BasicLevel[O, T]) FindFileblock(d T) (*db.Fileblock[O, T], bool, error) {
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

func (b *BasicLevel[O, T]) Find(d T) (db.Entry[O], bool, error) {
	if !b.entryFallsInside(d) {
		return nil, false, nil
	}

	// iterate through each block
	var (
		// entry     db.Entry[O]
		fileblock *db.Fileblock[O, T]
		entries   db.Entries[O, T]
		err       error
		found     bool
	)

	for _, fileblock = range b.fileblocks {
		if found = fileblock.Find(d); found {
			if entries, err = fileblock.Load(); err != nil {
				return nil, false, errors.Join(fmt.Errorf("error finding value %v in block: ", d), err)
			}
			_ = entries

			// if entry, found = entries.Find(d); found {
			// 	return entry, found, nil
			// }
		}
	}

	return nil, false, nil
}

func (b *BasicLevel[O, T]) Close() error {
	for _, block := range b.fileblocks {
		block.Close()
	}

	return nil
}

func (b *BasicLevel[O, T]) Fileblocks() []*db.Fileblock[O, T] {
	return b.fileblocks
}

func (b *BasicLevel[O, T]) entryFallsInside(d T) bool {
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

func (b *BasicLevel[O, T]) updateMinMax(in *db.MetaFile[O]) {
	// b.min.SetMin(in.Min)
	// b.max.SetMax(in.Max())
}

func (b *BasicLevel[O, T]) removeMinMax(meta *db.MetaFile[O]) {
	// b.min.Remove(meta.Min())
	// b.max.Remove(meta.Max())
}
