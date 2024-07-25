package core

import (
	"cmp"
	"time"
	"unsafe"

	db "github.com/sayden/streedb"
)

func newItemLimitWalFlushStrategy[O cmp.Ordered](limit int) db.WalFlushStrategy[O] {
	return &itemLimitWalFlushStrategy[O]{limit: limit}
}

type itemLimitWalFlushStrategy[O cmp.Ordered] struct{ limit int }

func (s *itemLimitWalFlushStrategy[O]) ShouldFlush(es *db.EntriesMap[O]) bool {
	if es.SecondaryIndicesLen() == 0 {
		return false
	}

	return es.LenAll() >= s.limit
}

func newSizeLimitWalFlushStrategy[O cmp.Ordered](s int) db.WalFlushStrategy[O] {
	return &sizeLimitWalFlushStrategy[O]{maxSize: s}
}

type sizeLimitWalFlushStrategy[O cmp.Ordered] struct {
	maxSize int
}

func (s *sizeLimitWalFlushStrategy[O]) ShouldFlush(es *db.EntriesMap[O]) bool {
	if es.SecondaryIndicesLen() == 0 {
		return false
	}

	// TODO: Optimistic way to get the size of a struct
	size := int(unsafe.Sizeof(es))

	return size*es.SecondaryIndicesLen() >= s.maxSize
}

// Flush Wal after a time duration
//
// Deprecated: There is a need to a wrapper in the Wal to use a time based strategy
func newTimeLimitWalFlushStrategy(d time.Duration) db.WalFlushStrategy[int64] {
	return &timeLimitWalFlushStrategy{duration: d}
}

type timeLimitWalFlushStrategy struct {
	duration time.Duration
}

func (s *timeLimitWalFlushStrategy) ShouldFlush(es *db.EntriesMap[int64]) bool {
	if es.SecondaryIndicesLen() == 0 {
		return false
	}

	inMs := es.Min()
	inTimeMs := time.UnixMilli(inMs)
	since := time.Since(inTimeMs)

	return since > s.duration
}
