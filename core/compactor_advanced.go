package core

import (
	"cmp"
	"errors"
	"fmt"

	"github.com/emirpasic/gods/v2/lists/arraylist"
	db "github.com/sayden/streedb"
	"github.com/sayden/streedb/fs"
)

func NewOnePassCompactor[O cmp.Ordered, E db.Entry[O]](cfg *db.Config, levels *fs.MultiFsLevels[O], mergers ...db.CompactionStrategy[O]) (db.Compactor[O], error) {
	return &onePassCompactor[O, E]{
		cfg:                cfg,
		levels:             levels,
		compactionStrategy: mergers,
		PIndex:             levels.PrimaryIndex,
		TIndex:             levels.Index,
	}, nil
}

type onePassCompactor[O cmp.Ordered, E db.Entry[O]] struct {
	cfg                *db.Config
	levels             *fs.MultiFsLevels[O]
	compactionStrategy []db.CompactionStrategy[O]
	PIndex             *db.BtreeIndex[O, string]
	TIndex             *db.BtreeIndex[O, O]
}

func (o *onePassCompactor[O, E]) Compact(fileblocks []*db.Fileblock[O]) error {
	var err error
	o.PIndex.AscendGreaterOrEqual(&db.BtreeItem[O, string]{Key: ""}, func(item *db.BtreeItem[O, string]) bool {
		primaryIdx := item.Key
		fmt.Println(primaryIdx)

		// A primary index might need to merge all its blocks, some of them or none of them
		// Store all candidates in fbs
		fbs := arraylist.New[*db.Fileblock[O]]()

		fBlocksLL := item.Val
		fBlocksLL.Each(func(fb *db.Fileblock[O]) bool {
			fmt.Printf("\t%s\n", fb.MetaFile.UUID())
			if fbs.Size() == 0 {
				fbs.Add(fb)
				return true
			}

			// Now if any of the fileblocks in fbs can be merged with fb, then add it to fbs
			// If not, continue looking
			toAdd := arraylist.New[*db.Fileblock[O]]()
			fbs.Each(func(i int, fbi *db.Fileblock[O]) {
				for _, merger := range o.compactionStrategy {
					if merger.ShouldMerge(&fbi.MetaFile, &fb.MetaFile) {
						toAdd.Add(fb)
						return
					}
				}
			})

			fbs.Add(toAdd.Values()...)

			return true
		})

		fbs.Each(func(i int, fb *db.Fileblock[O]) {
			fmt.Printf("(%s-%s) Min: %v, Max: %v\n", fb.PrimaryIdx, fb.SecondaryIndex(), *fb.Min, *fb.Max)
		})
		if fbs.Size() < 2 {
			return true
		}
		values := fbs.Values()

		var builder *db.MetadataBuilder[O]
		var em *db.EntriesMap[O]
		builder, em, err = db.Merge(values[0], values[1:]...)
		if err != nil {
			panic(err)
		}

		if err = o.levels.NewFileblock(em, builder); err != nil {
			err = errors.Join(errors.New("failed to create new fileblock"), err)
			return false
		}

		for _, fb := range values {
			if err = o.levels.RemoveFile(fb); err != nil {
				err = errors.Join(errors.New("error deleting block during compaction"), err)
				return false
			}
		}

		return true
	})

	return err
}
