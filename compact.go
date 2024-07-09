package streedb

type Compactor[T Entry] interface {
	Compact(block []*Fileblock[T]) error
}

type LevelPromoter[T Entry] interface {
	Promote(metaBuilder *MetadataBuilder[T]) error
}
