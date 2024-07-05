package core

import db "github.com/sayden/streedb"

func NewItemLimitPromoter[T db.Entry](maxItems, maxLevels int) db.LevelPromoter[T] {
	return &ItemLimitPromoter[T]{
		maxItems: maxItems,
	}
}

type ItemLimitPromoter[T db.Entry] struct {
	maxItems int
	maxLevel int
}

func (i *ItemLimitPromoter[T]) Promote(builder *db.MetadataBuilder[T]) error {
	realLevel := builder.ItemCount / i.maxItems
	builder.Level = realLevel
	if builder.Level > i.maxLevel {
		builder.Level = i.maxLevel
	}

	return nil
}
