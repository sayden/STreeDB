package streedb

import (
	"time"
)

type MetaFile[T Entry] struct {
	CreatedAt time.Time
	ItemCount int
	Size      int64
	Level     int
	Min       T
	Max       T
	Uuid      string

	DataFilepath string
	MetaFilepath string
}

func EntryFallsInsideMinMax(min, max, t Entry) bool {
	return (min.LessThan(t) || min.Equals(t)) && (t.LessThan(max) || t.Equals(max))
}
