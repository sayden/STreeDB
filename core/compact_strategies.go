package core

import (
	"cmp"

	db "github.com/sayden/streedb"
)

type samePrimaryIndexCompactionStrategy[O cmp.Ordered] struct {
	and db.CompactionStrategy[O]
}

func (o *samePrimaryIndexCompactionStrategy[O]) ShouldMerge(a, b *db.MetaFile[O]) bool {
	for _, rga := range a.Rows {
		for _, rgb := range b.Rows {
			if rga.SecondaryIdx == rgb.SecondaryIdx {
				if o.and != nil {
					return o.and.ShouldMerge(a, b)
				}
			}
		}
	}

	return false
}

// overlappingCompactionStrategy merges if two fileblocks overlap or are adjacent.
// Adjacent means that the max of one fileblock is one "step" away from the min of
// the other.
type overlappingCompactionStrategy[O cmp.Ordered] struct {
	and db.CompactionStrategy[O]
}

func (o *overlappingCompactionStrategy[O]) ShouldMerge(a, b *db.MetaFile[O]) bool {
	if a.PrimaryIdx != b.PrimaryIdx {
		return false
	}

	for _, rga := range a.Rows {
		for _, rgb := range b.Rows {
			if rga.SecondaryIdx == rgb.SecondaryIdx {
				if o.isOverlap(&rga, &rgb) {
					if o.and != nil {
						return o.and.ShouldMerge(a, b)
					}

					return true
				}
			}
		}
	}

	if o.and != nil {
		return o.and.ShouldMerge(a, b)
	}

	return false
}

func (o *overlappingCompactionStrategy[O]) isOverlap(rga, rgb *db.Row[O]) bool {
	return (rgb.Min < rga.Max || rgb.Min == rga.Max) &&
		(rga.Min < rgb.Max || rga.Min == rgb.Max)
}

func newOrCompactionStrategy[O cmp.Ordered, E db.Entry[O]](mergers ...db.CompactionStrategy[O]) db.CompactionStrategy[O] {
	return &orCompactionStrategy[O, E]{compactionStrategies: mergers}
}

// orCompactionStrategy merges if any of the compaction strategies are true.
type orCompactionStrategy[O cmp.Ordered, E db.Entry[O]] struct {
	compactionStrategies []db.CompactionStrategy[O]
}

func (o *orCompactionStrategy[O, E]) ShouldMerge(a, b *db.MetaFile[O]) bool {
	for _, merger := range o.compactionStrategies {
		if merger.ShouldMerge(a, b) {
			return true
		}
	}

	return false
}
