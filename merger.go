package streedb

type Merger[E Entry] interface {
	ShouldMerge(a, b *MetaFile[E]) bool
}
