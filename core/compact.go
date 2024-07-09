package core

import (
	db "github.com/sayden/streedb"
)

func isAdjacent[T db.Entry](a, b *db.MetaFile[T]) bool {
	if a.Min.PrimaryIndex() != "" && a.Min.PrimaryIndex() != b.Min.PrimaryIndex() {
		return false
	}

	return a.Max.Adjacent(b.Min) || b.Max.Adjacent(a.Min)
}

func HasOverlap[T db.Entry](a, b *db.MetaFile[T]) bool {
	if a.Min.PrimaryIndex() != "" && a.Min.PrimaryIndex() != b.Min.PrimaryIndex() {
		return false
	}

	return ((b.Min.LessThan(a.Max) || b.Min.Equals(a.Max)) && (a.Min.LessThan(b.Max) || a.Min.Equals(b.Max)))
}
