package core

import (
	"time"

	db "github.com/sayden/streedb"
)

// TODO: Add growth factor for exponential growth
// TODO: Add checks for config values
func newTimeLimitPromoter[E db.Entry](cfg *db.Config) db.LevelPromoter[E] {
	levels := make([]int64, 0, cfg.MaxLevels)
	for i := 0; i < cfg.MaxLevels; i++ {
		levels = append(levels, (cfg.Compaction.Promoters.TimeLimit.MaxTimeMs/cfg.Compaction.Promoters.TimeLimit.MinTimeMs)*int64(i+1))
	}

	return &timeLimitPromoter[E]{
		timeLevels: levels,
	}
}

type timeLimitPromoter[E db.Entry] struct {
	timeLevels []int64
}

func (t *timeLimitPromoter[E]) Promote(builder *db.MetadataBuilder[E]) error {
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
func newSizeLimitPromoter[E db.Entry](cfg *db.Config) db.LevelPromoter[E] {
	slp := &sizeLimitPromoter[E]{cfg: cfg}

	slp.calculateBlockSizes()

	return slp
}

type sizeLimitPromoter[E db.Entry] struct {
	cfg        *db.Config
	blockSizes []int64
}

func (s *sizeLimitPromoter[E]) Promote(builder *db.MetadataBuilder[E]) error {
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

func (s *sizeLimitPromoter[E]) calculateBlockSizes() {
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
func newItemLimitPromoter[E db.Entry](cfg *db.Config) db.LevelPromoter[E] {
	return &itemLimitPromoter[E]{
		cfg: cfg,
	}
}

type itemLimitPromoter[E db.Entry] struct {
	cfg *db.Config
}

func (i *itemLimitPromoter[E]) Promote(builder *db.MetadataBuilder[E]) error {
	realLevel := builder.ItemCount / i.cfg.Compaction.Promoters.ItemLimit.MaxItems
	builder.WithLevel(realLevel)
	return nil
}
