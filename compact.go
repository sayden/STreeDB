package streedb

import "cmp"

type Compactor[O cmp.Ordered] interface {
	Compact(block []*Fileblock[O]) error
}

type LevelPromoter[O cmp.Ordered] interface {
	Promote(metaBuilder *MetadataBuilder[O]) error
}

type CompactionStrategy[O cmp.Ordered] interface {
	ShouldMerge(a, b *MetaFile[O]) bool
}
