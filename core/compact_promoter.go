package core

import (
	db "github.com/sayden/streedb"
)

func newSizeLimitPromoter[E db.Entry](maxLevels int) db.LevelPromoter[E] {
	slp := &sizeLimitPromoter[E]{
		maxLevels: maxLevels,
	}
	slp.calculateBlockSizes()
	return slp
}

type sizeLimitPromoter[E db.Entry] struct {
	maxLevels  int
	blockSizes []int64
}

func (s *sizeLimitPromoter[E]) Promote(builder *db.MetadataBuilder[E]) error {
	blockSize := builder.Size
	for i, size := range s.blockSizes {
		if blockSize >= size {
			builder.WithLevel(i + 1)
		}
	}

	return nil
}

func (s *sizeLimitPromoter[E]) calculateBlockSizes() {
	const (
		LEVEL_GROWTH_FACTOR = 16
		MAX_BLOCK_SIZE      = 1 << 30 // 1 GB
		FIRST_BLOCK_SIZE    = 4096 * 8
	)

	blockSizes := make([]int64, 0, s.maxLevels)

	for i := 0; i < s.maxLevels; i++ {
		var blockSize int64
		if i == 0 {
			blockSize = FIRST_BLOCK_SIZE // 1 MB
		} else {
			blockSize = blockSizes[i-1] * LEVEL_GROWTH_FACTOR
			if blockSize > MAX_BLOCK_SIZE {
				blockSize = MAX_BLOCK_SIZE
			}
		}

		blockSizes = append(blockSizes, blockSize)
	}

	s.blockSizes = blockSizes
}

func newItemLimitPromoter[E db.Entry](maxItems, maxLevels int) db.LevelPromoter[E] {
	return &itemLimitPromoter[E]{
		maxItems: maxItems,
		maxLevel: maxLevels,
	}
}

type itemLimitPromoter[E db.Entry] struct {
	maxItems int
	maxLevel int
}

func (i *itemLimitPromoter[E]) Promote(builder *db.MetadataBuilder[E]) error {
	realLevel := builder.ItemCount / i.maxItems
	builder.Level = realLevel
	if builder.Level > i.maxLevel {
		builder.Level = i.maxLevel
	}

	return nil
}
