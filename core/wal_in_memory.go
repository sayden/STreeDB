package core

import (
	"cmp"
	"time"

	db "github.com/sayden/streedb"
)

func newNMMemoryWal[O cmp.Ordered, E db.Entry[O]](cfg *db.Config, fbc db.FileblockCreator[O, E], persistStrategies ...db.WalFlushStrategy[O, E]) db.Wal[O, E] {
	return &nmMemoryWal[O, E]{
		entries:          make(map[string]db.EntriesMap[O, E]),
		cfg:              cfg,
		fileblockCreator: fbc,
		flushStrategies:  persistStrategies,
	}
}

// nmInMemoryWal is a write-ahead log that stores entries in memory.
// That means that the entries are not persisted to disk until the
// a persist strategies is met or the WAL is closed (usually when
// closing the database)
type nmMemoryWal[O cmp.Ordered, E db.Entry[O]] struct {
	entries          map[string]db.EntriesMap[O, E]
	cfg              *db.Config
	fileblockCreator db.FileblockCreator[O, E]
	flushStrategies  []db.WalFlushStrategy[O, E]
}

func (w *nmMemoryWal[O, E]) Append(d E) (err error) {
	fileEntries := w.entries[d.PrimaryIndex()]

	if fileEntries == nil {
		fileEntries = db.NewEntriesMap[O, E]()
		w.entries[d.PrimaryIndex()] = fileEntries
	}
	fileEntries.Append(d)

	for _, s := range w.flushStrategies {
		if s.ShouldFlush(fileEntries) {
			builder := db.NewMetadataBuilder[O](w.cfg).
				WithPrimaryIndex(d.PrimaryIndex()).
				WithLevel(0).
				WithCreatedAt(time.Now())

			if err = w.fileblockCreator.NewFileblock(fileEntries, builder); err != nil {
				return err
			}

			delete(w.entries, d.PrimaryIndex())

			return nil
		}
	}

	return nil
}

func (w *nmMemoryWal[O, E]) Find(pIdx, sIdx string, min, max O) (E, bool) {
	fileEntries := w.entries[pIdx]
	if fileEntries == nil {
		return *new(E), false
	}

	return fileEntries.Find(sIdx, min, max)
}

func (w *nmMemoryWal[O, E]) Close() error {
	for _, fileEntries := range w.entries {
		if fileEntries.SecondaryIndicesLen() == 0 {
			return nil
		}
		pIdx := fileEntries.PrimaryIndex()
		if pIdx == "" {
			continue
		}

		builder := db.NewMetadataBuilder[O](w.cfg).
			WithLevel(0).
			WithPrimaryIndex(pIdx).
			WithCreatedAt(time.Now())

		if err := w.fileblockCreator.NewFileblock(fileEntries, builder); err != nil {
			return err
		}
	}

	return nil
}
