package core

import (
	"sort"

	db "github.com/sayden/streedb"
)

type memoryWalEntryList[E db.Entry] struct {
	entries    db.Entries[E]
	totalItems int
}

type nmMemoryWal[E db.Entry] struct {
	entries          map[string]*memoryWalEntryList[E]
	cfg              *db.Config
	maxEntries       int
	fileblockCreator db.FileblockCreator[E]
}

func newNMMemoryWal[E db.Entry](cfg *db.Config, fbc db.FileblockCreator[E]) db.Wal[E] {
	return &nmMemoryWal[E]{
		entries:          make(map[string]*memoryWalEntryList[E]),
		maxEntries:       cfg.WalMaxItems,
		cfg:              cfg,
		fileblockCreator: fbc,
	}
}

func (w *nmMemoryWal[E]) Append(d E) bool {
	entryList := w.entries[d.PrimaryIndex()]
	if entryList == nil {
		entryList = &memoryWalEntryList[E]{
			entries: make(db.Entries[E], 0, w.cfg.WalMaxItems),
		}
		w.entries[d.PrimaryIndex()] = entryList
	}

	entryList.entries = append(entryList.entries, d)
	entryList.totalItems++

	isFull := entryList.totalItems == w.maxEntries
	if isFull {
		sort.Sort(entryList.entries)
		w.fileblockCreator.NewFileblock(entryList.entries, 0)
		delete(w.entries, d.PrimaryIndex())
	}

	return isFull
}

// TODO: Implement Find into the WAL
func (w *nmMemoryWal[E]) Find(d E) (E, bool) {
	// for _, v := range w.entries {
	// 	if v.Equals(d) {
	// 		return v, true
	// 	}
	// }

	return (*new(E)), false
}

func (w *nmMemoryWal[E]) Close() error {
	for _, es := range w.entries {
		if len(es.entries) > 0 {
			w.fileblockCreator.NewFileblock(es.entries, 0)
		}
	}
	return nil
}
