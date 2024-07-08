package core

import (
	"errors"

	db "github.com/sayden/streedb"
)

func newTimeLimitPromoter[E db.Entry](maxSizeBytes, maxLevels int) db.LevelPromoter[E] {
	return &timeLimitPromoter[E]{
		maxSizeBytes: maxSizeBytes,
		maxLevel:     maxLevels,
	}
}

type timeLimitPromoter[E db.Entry] struct {
	maxSizeBytes int
	maxLevel     int
}

func (s *timeLimitPromoter[E]) Promote(builder *db.MetadataBuilder[E]) error {
	return errors.New("not implemented")
}

func newSizeLimitPromoter[E db.Entry](maxSizeBytes, maxLevels int) db.LevelPromoter[E] {
	return &sizeLimitPromoter[E]{
		maxSizeBytes: maxSizeBytes,
		maxLevel:     maxLevels,
	}
}

type sizeLimitPromoter[E db.Entry] struct {
	maxSizeBytes int
	maxLevel     int
}

func (s *sizeLimitPromoter[E]) Promote(builder *db.MetadataBuilder[E]) error {
	// realLevel := builder.ItemCount / i.maxItems
	// builder.Level = realLevel
	// if builder.Level > i.maxLevel {
	// 	builder.Level = i.maxLevel
	// }
	return nil
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
