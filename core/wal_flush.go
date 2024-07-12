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

func (s *itemLimitWalFlushStrategy[O, E]) ShouldFlush(es db.Entries[O, E]) bool {
	if es.Len() == 0 {
		return false
	}
	return es.Len() >= s.limit
}

// FIXME: This function is not used in the codebase
func newTimeLimitWalFlushStrategy[O cmp.Ordered, E db.Entry[O]](d time.Duration) db.WalFlushStrategy[O, E] {
	return &timeLimitWalFlushStrategy[O, E]{duration: d}
}

type timeLimitWalFlushStrategy[O cmp.Ordered, E db.Entry[O]] struct {
	duration time.Duration
}

func (s *timeLimitWalFlushStrategy[O, E]) ShouldFlush(es db.Entries[O, E]) bool {
	if es.Len() == 0 {
		return false
	}
	return time.Since(es.Last().CreationTime()) > s.duration
}

func newSizeLimitWalFlushStrategy[O cmp.Ordered, E db.Entry[O]](s int) db.WalFlushStrategy[O, E] {
	return &sizeLimitWalFlushStrategy[O, E]{maxSize: s}
}

type sizeLimitWalFlushStrategy[O cmp.Ordered, E db.Entry[O]] struct {
	maxSize int
}

func (s *sizeLimitWalFlushStrategy[O, E]) ShouldFlush(es db.Entries[O, E]) bool {
	if es.Len() == 0 {
		return false
	}

	// TODO: Optimistic way to get the size of a struct
	size := int(unsafe.Sizeof(es.Get(0)))

	return size*es.Len() >= s.maxSize
}
