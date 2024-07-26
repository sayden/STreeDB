package core

import (
	"cmp"
	"time"

	db "github.com/sayden/streedb"
)

// TODO: Add growth factor for exponential growth
// TODO: Add checks for config values
func newTimeLimitPromoter[O cmp.Ordered, E db.Entry[O]](cfg *db.Config) db.LevelPromoter[O] {
	levels := make([]int64, 0, cfg.MaxLevels)
	for i := 0; i < cfg.MaxLevels; i++ {
		levels = append(levels, (cfg.Compaction.Promoters.TimeLimit.MaxTimeMs/cfg.Compaction.Promoters.TimeLimit.MinTimeMs)*int64(i+1))
	}

	return &timeLimitPromoter[O, E]{
		timeLevels: levels,
	}
}

// timeLimitPromoter promotes based on the time elapsed since the fileblock was created
type timeLimitPromoter[O cmp.Ordered, E db.Entry[O]] struct {
	timeLevels []int64
}

func (t *timeLimitPromoter[O, E]) Promote(builder *db.MetadataBuilder[O]) error {
	elapsed := time.Since(builder.CreatedAt).Milliseconds()
	for i, level := range t.timeLevels {
		if elapsed >= level {
			builder.WithLevel(i + 1)
		} else {
			break
		}
	}

	return nil
}

// TODO: Add checks for config values
func newSizeLimitPromoter[O cmp.Ordered, E db.Entry[O]](cfg *db.Config) db.LevelPromoter[O] {
	slp := &sizeLimitPromoter[O, E]{cfg: cfg}

	slp.calculateBlockSizes()

	return slp
}

// sizeLimitPromoter promotes a fileblock based on the size of the fileblock
type sizeLimitPromoter[O cmp.Ordered, E db.Entry[O]] struct {
	cfg        *db.Config
	blockSizes []int64
}

func (s *sizeLimitPromoter[O, E]) Promote(builder *db.MetadataBuilder[O]) error {
	blockSize := builder.Size
	for i, size := range s.blockSizes {
		if blockSize >= size {
			builder.WithLevel(i + 1)
		} else {
			break
		}
	}

	return nil
}

func (s *sizeLimitPromoter[O, E]) calculateBlockSizes() {
	blockSizes := make([]int64, 0, s.cfg.MaxLevels)

	for i := 0; i < s.cfg.MaxLevels; i++ {
		var blockSize int64
		if i == 0 {
			blockSize = int64(s.cfg.Compaction.Promoters.SizeLimit.FirstBlockSizeBytes) // 1 MB
		} else {
			blockSize = blockSizes[i-1] * int64(s.cfg.Compaction.Promoters.SizeLimit.GrowthFactor)
			if blockSize > int64(s.cfg.Compaction.Promoters.SizeLimit.MaxBlockSizeBytes) {
				blockSize = int64(s.cfg.Compaction.Promoters.SizeLimit.MaxBlockSizeBytes)
			}
		}

		blockSizes = append(blockSizes, blockSize)
	}

	s.blockSizes = blockSizes
}

// TODO: Add growth factor for exponential growth
// TODO: Add checks for config values
func newItemLimitPromoter[O cmp.Ordered, E db.Entry[O]](cfg *db.Config) db.LevelPromoter[O] {
	promoter := &itemLimitPromoter[O, E]{
		cfg: cfg,
	}
	promoter.calculateBlockSizes()

	return promoter
}

// itemLimitPromoter promotes a fileblock based on the number of items in the wal
type itemLimitPromoter[O cmp.Ordered, E db.Entry[O]] struct {
	cfg        *db.Config
	blockSizes []int64
}

func (s *itemLimitPromoter[O, E]) calculateBlockSizes() {
	blockSizes := make([]int64, 0, s.cfg.MaxLevels)

	for i := 0; i < s.cfg.MaxLevels; i++ {
		var blockSize int64
		if i == 0 {
			blockSize = int64(s.cfg.Compaction.Promoters.ItemLimit.FirstBlockItemCount) // 1 MB
		} else {
			blockSize = blockSizes[i-1] * int64(s.cfg.Compaction.Promoters.ItemLimit.GrowthFactor)
			if blockSize > int64(s.cfg.Compaction.Promoters.ItemLimit.MaxItems) {
				blockSize = int64(s.cfg.Compaction.Promoters.ItemLimit.MaxItems)
			}
		}

		blockSizes = append(blockSizes, blockSize)
	}

	s.blockSizes = blockSizes
}

func (l *itemLimitPromoter[O, E]) Promote(builder *db.MetadataBuilder[O]) error {
	// realLevel := builder.ItemCount / l.cfg.Compaction.Promoters.ItemLimit.MaxItems

	for i, size := range l.blockSizes {
		if int64(builder.ItemCount) >= size {
			builder.WithLevel(i + 1)
		} else {
			break
		}
	}

	return nil
}
