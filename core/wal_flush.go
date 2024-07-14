package core

import (
	"cmp"
	"time"
	"unsafe"

	db "github.com/sayden/streedb"
)

func newItemLimitWalFlushStrategy[O cmp.Ordered, E db.Entry[O]](limit int) db.WalFlushStrategy[O, E] {
	return &itemLimitWalFlushStrategy[O, E]{limit: limit}
}

type itemLimitWalFlushStrategy[O cmp.Ordered, E db.Entry[O]] struct{ limit int }

func (s *itemLimitWalFlushStrategy[O, E]) ShouldFlush(es db.EntriesMap[O, E]) bool {
	if es.SecondaryIndicesLen() == 0 {
		return false
	}
	return es.LenAll() >= s.limit
}

func newTimeLimitWalFlushStrategy[E db.Entry[int64]](d time.Duration) db.WalFlushStrategy[int64, E] {
	return &timeLimitWalFlushStrategy[E]{duration: d}
}

type timeLimitWalFlushStrategy[E db.Entry[int64]] struct {
	duration time.Duration
}

func (s *timeLimitWalFlushStrategy[E]) ShouldFlush(es db.EntriesMap[int64, E]) bool {
	if es.SecondaryIndicesLen() == 0 {
		return false
	}

	inMs := es.Min()
	inTimeMs := time.UnixMilli(inMs)
	since := time.Since(inTimeMs)

	return since > s.duration
}

func newSizeLimitWalFlushStrategy[O cmp.Ordered, E db.Entry[O]](s int) db.WalFlushStrategy[O, E] {
	return &sizeLimitWalFlushStrategy[O, E]{maxSize: s}
}

type sizeLimitWalFlushStrategy[O cmp.Ordered, E db.Entry[O]] struct {
	maxSize int
}

func (s *sizeLimitWalFlushStrategy[O, E]) ShouldFlush(es db.EntriesMap[O, E]) bool {
	if es.SecondaryIndicesLen() == 0 {
		return false
	}

	// TODO: Optimistic way to get the size of a struct
	size := int(unsafe.Sizeof(es))

	return size*es.SecondaryIndicesLen() >= s.maxSize
}
