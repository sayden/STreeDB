package core

import (
	db "github.com/sayden/streedb"
)

func newSizeLimitPromoter[E db.Entry](maxLevels, growthFactor, firstBlockSize, maxBlockSize int) db.LevelPromoter[E] {
	slp := &sizeLimitPromoter[E]{
		maxLevels:      maxLevels,
		growthFactor:   growthFactor,
		firstBlockSize: firstBlockSize,
		maxBlockSize:   maxBlockSize,
	}
	slp.calculateBlockSizes()
	return slp
}

type sizeLimitPromoter[E db.Entry] struct {
	maxLevels      int
	growthFactor   int
	firstBlockSize int
	maxBlockSize   int
	blockSizes     []int64
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
	blockSizes := make([]int64, 0, s.maxLevels)

	for i := 0; i < s.maxLevels; i++ {
		var blockSize int64
		if i == 0 {
			blockSize = int64(s.firstBlockSize) // 1 MB
		} else {
			blockSize = blockSizes[i-1] * int64(s.growthFactor)
			if blockSize > int64(s.maxBlockSize) {
				blockSize = int64(s.maxBlockSize)
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
	builder.WithLevel(realLevel)
	return nil
}
