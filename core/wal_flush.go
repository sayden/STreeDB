package core

import (
	"time"
	"unsafe"

	db "github.com/sayden/streedb"
)

func newItemLimitWalFlushStrategy[E db.Entry](limit int) db.WalFlushStrategy[E] {
	return &itemLimitWalFlushStrategy[E]{limit: limit}
}

type itemLimitWalFlushStrategy[E db.Entry] struct{ limit int }

func (s *itemLimitWalFlushStrategy[E]) ShouldFlush(es db.Entries[E]) bool {
	if len(es) == 0 {
		return false
	}
	return len(es) >= s.limit
}

func newTimeLimitWalFlushStrategy[E db.Entry](d time.Duration) db.WalFlushStrategy[E] {
	return &timeLimitWalFlushStrategy[E]{duration: d}
}

type timeLimitWalFlushStrategy[E db.Entry] struct {
	duration time.Duration
}

func (s *timeLimitWalFlushStrategy[E]) ShouldFlush(es db.Entries[E]) bool {
	if len(es) == 0 {
		return false
	}
	return time.Since(es[len(es)-1].CreationTime()) > s.duration
}

func newSizeLimitWalFlushStrategy[E db.Entry](s int) db.WalFlushStrategy[E] {
	return &sizeLimitWalFlushStrategy[E]{maxSize: s}
}

type sizeLimitWalFlushStrategy[E db.Entry] struct {
	maxSize int
}

func (s *sizeLimitWalFlushStrategy[E]) ShouldFlush(es db.Entries[E]) bool {
	if len(es) == 0 {
		return false
	}

	// TODO: Optimistic way to get the size of a struct
	size := int(unsafe.Sizeof(es[0]))

	return size*len(es) >= s.maxSize
}
