package core

import (
	"time"

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

func IsSizeExceeded[T db.Entry](b *db.MetaFile[T], level int) bool {
	return b.Size > MAX_LEVEL_0_BLOCK_SIZE*int64(level+1)
}

func isTooOld[T db.Entry](b db.MetaFile[T], level int) bool {
	switch level {
	case 0:
		return time.Since(b.CreatedAt) > MAX_LEVEL_0_BLOCK_AGE
	case 1:
		return time.Since(b.CreatedAt) > MAX_LEVEL_1_BLOCK_AGE
	case 2:
		return time.Since(b.CreatedAt) > MAX_LEVEL_2_BLOCK_AGE
	case 3:
		return time.Since(b.CreatedAt) > MAX_LEVEL_3_BLOCK_AGE
	case 4:
		return time.Since(b.CreatedAt) > MAX_LEVEL_4_BLOCK_AGE
	default:
		return false
	}
}
