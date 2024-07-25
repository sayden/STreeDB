package fs

import (
	"cmp"
	"errors"

	db "github.com/sayden/streedb"
	local "github.com/sayden/streedb/fs/local"
	memory "github.com/sayden/streedb/fs/memory"
	fss3 "github.com/sayden/streedb/fs/s3"
)

func NewLeveledFilesystem[O cmp.Ordered, E db.Entry[O]](cfg *db.Config, promoter ...db.LevelPromoter[O]) (*MultiFsLevels[O], error) {
	levels := &MultiFsLevels[O]{
		cfg:                cfg,
		promoters:          promoter,
		fileblockListeners: make([]db.FileblockListener[O], 0, 10),
		Index:              db.NewBtreeIndex(2, db.LLFComp[O]),
	}

	// add self to the listeners
	levels.fileblockListeners = append(levels.fileblockListeners, levels)

	result := make(map[int]*BasicLevel[O])

	if len(cfg.LevelFilesystems) == 0 && cfg.Filesystem == "" {
		panic("'LevelFilesystems' must have at least one entry or 'Filesystem' must be set in the config")
	}

	if len(cfg.LevelFilesystems) == 0 {
		cfg.LevelFilesystems = make([]string, cfg.MaxLevels)
		for i := 0; i < cfg.MaxLevels; i++ {
			cfg.LevelFilesystems[i] = cfg.Filesystem
		}
	}

	if cfg.MaxLevels != len(cfg.LevelFilesystems) {
		panic("MaxLevels number and LevelFilesystems lenght must be the same")
	}

	var err error

	for levelIdx, level := range cfg.LevelFilesystems {
		var fs db.Filesystem[O]

		switch db.FilesystemTypeReverseMap[level] {
		case db.FILESYSTEM_TYPE_LOCAL:
			if fs, err = local.InitParquetLocal[O, E](cfg, levelIdx); err != nil {
				return nil, err
			}
			result[levelIdx] = NewBasicLevel(cfg, fs, levels)

		case db.FILESYSTEM_TYPE_S3:
			if fs, err = fss3.InitParquetS3[O, E](cfg, levelIdx); err != nil {
				return nil, err
			}
			result[levelIdx] = NewBasicLevel(cfg, fs, levels)
		case db.FILESYSTEM_TYPE_MEMORY:
			fs = memory.NewMemoryFs[O](cfg)
			result[levelIdx] = NewBasicLevel(cfg, fs, levels)
		default:
			return nil, db.ErrUnknownFilesystemType
		}
	}

	levels.levels = result
	return levels, nil
}

type MultiFsLevels[O cmp.Ordered] struct {
	cfg                *db.Config
	promoters          []db.LevelPromoter[O]
	levels             map[int]*BasicLevel[O]
	Index              *db.BtreeIndex[O]
	fileblockListeners []db.FileblockListener[O]
}

func (b *MultiFsLevels[O]) OnFileblockCreated(block *db.Fileblock[O]) {
	b.Index.Upsert(*block.Metadata().Min, block)
}

func (b *MultiFsLevels[O]) OnFileblockRemoved(block *db.Fileblock[O]) {
	b.Index.Remove(*block.Metadata().Min, block)
}

func (b *MultiFsLevels[O]) NewFileblock(es *db.EntriesMap[O], builder *db.MetadataBuilder[O]) error {
	for _, secIdx := range es.SecondaryIndices() {
		entry := es.Get(secIdx)
		if entry.Len() == 0 {
			continue
		}
		entry.Sort()
		builder.WithEntry(entry)
	}

	for _, promoter := range b.promoters {
		if err := promoter.Promote(builder); err != nil {
			return errors.Join(errors.New("failed during promotion"), err)
		}
	}

	_, err := b.levels[builder.Level].Create(es, builder)
	if err != nil {
		return errors.Join(errors.New("failed to create new fileblock in mfs"), err)
	}

	return nil
}

func (b *MultiFsLevels[O]) RemoveFile(a *db.Fileblock[O]) error {
	meta := a.Metadata()
	level := meta.Level
	return b.levels[level].RemoveFile(a)
}

func (b *MultiFsLevels[O]) Open(p string) (*db.Fileblock[O], error) {
	return nil, errors.New("unreachable")
}

func (b *MultiFsLevels[O]) Create(es *db.EntriesMap[O], meta *db.MetadataBuilder[O]) (*db.Fileblock[O], error) {
	return nil, errors.New("unreachable")
}

func (b *MultiFsLevels[O]) FindSingle(pIdx, sIdx string, min, max O) (db.EntryIterator[O], bool, error) {
	return b.Index.AscendRangeWithFilters(min, max, db.PrimaryIndexFilter(pIdx), db.SecondaryIndexFilter[O](sIdx))
}

func (b *MultiFsLevels[O]) Fileblocks() []*db.Fileblock[O] {
	var blocks []*db.Fileblock[O]

	b.Index.Ascend(func(i *db.BtreeItem[O]) bool {
		ll := i.Val
		for next, found := ll.Head(); next != nil && found; next = next.Next {
			blocks = append(blocks, next.Val)
		}
		return true
	})

	return blocks
}

func (b *MultiFsLevels[O]) Level(i int) *BasicLevel[O] {
	return b.levels[i]
}

func (b *MultiFsLevels[O]) Close() error {
	for _, level := range b.levels {
		if err := level.Close(); err != nil {
			return err
		}
	}

	return nil
}
