package fs

import (
	"cmp"
	"errors"
	"fmt"

	db "github.com/sayden/streedb"
)

func NewBasicLevel[O cmp.Ordered](cfg *db.Config, fs db.Filesystem[O], levels *MultiFsLevels[O]) *BasicLevel[O] {
	level := &BasicLevel[O]{
		cfg:                cfg,
		filesystem:         fs,
		fileblocks:         make([]*db.Fileblock[O], 0, 10),
		fileblockListeners: []db.FileblockListener[O]{levels},
	}
	// add self to the listeners
	level.fileblockListeners = append(level.fileblockListeners, level)

	err := fs.OpenMetaFilesInLevel(level.fileblockListeners)
	if err != nil {
		panic(err)
	}

	return level
}

type BasicLevel[O cmp.Ordered] struct {
	filesystem db.Filesystem[O]

	cfg                *db.Config
	fileblockListeners []db.FileblockListener[O]

	// TODO: Use a Btree instead of a slice
	fileblocks []*db.Fileblock[O]
}

func (b *BasicLevel[O]) OnFileblockRemoved(block *db.Fileblock[O]) {
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

func (b *BasicLevel[O]) OnFileblockCreated(f *db.Fileblock[O]) {
	meta := f.Metadata()
	b.updateMinMax(meta)

	b.fileblocks = append(b.fileblocks, f)
}

func (b *BasicLevel[O]) Create(es db.EntriesMap[O], builder *db.MetadataBuilder[O]) (*db.Fileblock[O], error) {
	fileblock, err := b.filesystem.Create(b.cfg, es, builder, b.fileblockListeners)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("error creating block at level: "), err)
	}

	return fileblock, nil
}

func (b *BasicLevel[O]) RemoveFile(f *db.Fileblock[O]) error {
	return b.filesystem.Remove(f, b.fileblockListeners)
}

func (b *BasicLevel[O]) FindFileblock(d db.Entry[O]) (*db.Fileblock[O], bool, error) {
	// if !b.entryFallsInside(d) {
	// 	return nil, false, nil
	// }
	//
	// for _, fileblock := range b.fileblocks {
	// 	if found := fileblock.Find(d); found {
	// 		return fileblock, found, nil
	// 	}
	// }
	//
	return nil, false, nil
}

func (b *BasicLevel[O]) Find(pIdx, sIdx string, min, max O) (db.Entry[O], bool, error) {
	panic("implement me")
	// if !b.entryFallsInside(d) {
	// 	return nil, false, nil
	// }
	//
	// // iterate through each block
	// var (
	// 	// entry     db.Entry[O]
	// 	fileblock *db.Fileblock[O]
	// 	entries   db.EntriesMap[O, E]
	// 	err       error
	// 	found     bool
	// )
	//
	// for _, fileblock = range b.fileblocks {
	// 	if found = fileblock.Find(d); found {
	// 		if entries, err = fileblock.Load(); err != nil {
	// 			return nil, false, errors.Join(fmt.Errorf("error finding value %v in block: ", d), err)
	// 		}
	// 		_ = entries
	//
	// 		// if entry, found = entries.Find(d); found {
	// 		// 	return entry, found, nil
	// 		// }
	// 	}
	// }
	//
	// return nil, false, nil
}

func (b *BasicLevel[O]) Close() error {
	for _, block := range b.fileblocks {
		block.Close()
	}

	return nil
}

func (b *BasicLevel[O]) Fileblocks() []*db.Fileblock[O] {
	return b.fileblocks
}

// func (b *BasicLevel[O, T]) entryFallsInside(d T) bool {
// 	if minV, found := b.min.Head(); !found {
// 		return false
// 	} else if d.LessThan(minV) {
// 		return false
// 	}
//
// 	if maxV, foundMax := b.max.Head(); !foundMax {
// 		return false
// 	} else if !d.LessThan(maxV) {
// 		return false
// 	}
//
// 	return true
// }

func (b *BasicLevel[O]) updateMinMax(in *db.MetaFile[O]) {
	// b.min.SetMin(in.Min)
	// b.max.SetMax(in.Max())
}

func (b *BasicLevel[O]) removeMinMax(meta *db.MetaFile[O]) {
	// b.min.Remove(meta.Min())
	// b.max.Remove(meta.Max())
}
