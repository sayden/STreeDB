package core

import (
	db "github.com/sayden/streedb"
)

type samePrimaryIndexMerger[E db.Entry] struct {
	and db.Merger[E]
}

func (o *samePrimaryIndexMerger[E]) ShouldMerge(a, b *db.MetaFile[E]) bool {
	areSameIndex := a.Min.PrimaryIndex() == b.Min.PrimaryIndex()
	if !areSameIndex {
		return false
	}

	if o.and != nil {
		return o.and.ShouldMerge(a, b)
	}

	return areSameIndex
}

type overlappingMerger[E db.Entry] struct {
	and db.Merger[E]
}

func (o *overlappingMerger[E]) ShouldMerge(a, b *db.MetaFile[E]) bool {
	isOverlapping := o.isOverlap(a, b) || o.isAdjacent(a, b)
	if !isOverlapping {
		return false
	}

	if o.and != nil {
		return o.and.ShouldMerge(a, b)
	}

	return isOverlapping
}

func (o *overlappingMerger[E]) isOverlap(a, b *db.MetaFile[E]) bool {
	if a.Min.PrimaryIndex() != "" && a.Min.PrimaryIndex() != b.Min.PrimaryIndex() {
		return false
	}

	return ((b.Min.LessThan(a.Max) || b.Min.Equals(a.Max)) &&
		(a.Min.LessThan(b.Max) || a.Min.Equals(b.Max)))
}

func (o *overlappingMerger[E]) isAdjacent(a, b *db.MetaFile[E]) bool {
	if a.Min.PrimaryIndex() != "" && a.Min.PrimaryIndex() != b.Min.PrimaryIndex() {
		return false
	}

	return a.Max.Adjacent(b.Min) || b.Max.Adjacent(a.Min)

}

func newOrMerger[E db.Entry](mergers ...db.Merger[E]) db.Merger[E] {
	return &orMerger[E]{mergers: mergers}
}

type orMerger[E db.Entry] struct {
	mergers []db.Merger[E]
}

func (o *orMerger[E]) ShouldMerge(a, b *db.MetaFile[E]) bool {
	for _, merger := range o.mergers {
		if merger.ShouldMerge(a, b) {
			return true
		}
	}

	return false
}
