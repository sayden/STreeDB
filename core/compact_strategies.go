package core

import (
	"cmp"
	"math"

	db "github.com/sayden/streedb"
)

type samePrimaryIndexCompactionStrategy[O cmp.Ordered] struct {
	and db.CompactionStrategy[O]
}

func (o *samePrimaryIndexCompactionStrategy[O]) ShouldMerge(a, b *db.MetaFile[O]) bool {
	if a.PrimaryIdx != b.PrimaryIdx {
		return false
	}

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

func (o *overlappingCompactionStrategy[O]) isOverlap(r1, r2 *db.Row[O]) bool {
	// Check if r1 overlaps with r2
	if r1.Min <= r2.Max && r2.Min <= r1.Max {
		return true
	}

	// Check if r1 is immediately before r2 or r2 is immediately before r1
	return isAdjacent(r1.Max, r2.Min)
}

func isAdjacent[T cmp.Ordered](a, b T) bool {
	switch a_ := any(a).(type) {
	case int:
		return math.Abs(float64(a_-any(b).(int))) == 1
	case int8:
		return math.Abs(float64(a_-any(b).(int8))) == 1
	case int16:
		return math.Abs(float64(a_-any(b).(int16))) == 1
	case int32:
		return math.Abs(float64(a_-any(b).(int32))) == 1
	case int64:
		return math.Abs(float64(a_-any(b).(int64))) == 1
	case uint:
		return math.Abs(float64(a_-any(b).(uint))) == 1
	case uint8:
		return math.Abs(float64(a_-any(b).(uint8))) == 1
	case uint16:
		return math.Abs(float64(a_-any(b).(uint16))) == 1
	case uint32:
		return math.Abs(float64(a_-any(b).(uint32))) == 1
	case uint64:
		return math.Abs(float64(a_-any(b).(uint64))) == 1
	case uintptr:
		return math.Abs(float64(a_-any(b).(uintptr))) == 1
	case float32:
		return math.Abs(float64(a_-any(b).(float32))) == 1
	case float64:
		return math.Abs(float64(a_-any(b).(float64))) == 1
	case string:
		return false
	}

	return false
}

func newOrCompactionStrategy[O cmp.Ordered, E db.Entry[O]](mergers ...db.CompactionStrategy[O]) db.CompactionStrategy[O] {
	return &orCompactionStrategy[O, E]{compactionStrategies: mergers}
}

// orCompactionStrategy merges if any of the compaction strategies are true.
type orCompactionStrategy[O cmp.Ordered, E db.Entry[O]] struct {
	compactionStrategies []db.CompactionStrategy[O]
}

func (o *orCompactionStrategy[O, E]) ShouldMerge(a, b *db.MetaFile[O]) bool {
	if a.PrimaryIdx != b.PrimaryIdx {
		return false
	}

	for _, merger := range o.compactionStrategies {
		if merger.ShouldMerge(a, b) {
			return true
		}
	}

	return false
}
