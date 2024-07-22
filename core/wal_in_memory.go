package core

import (
	"cmp"
	"time"

	db "github.com/sayden/streedb"
)

func newNMMemoryWal[O cmp.Ordered](cfg *db.Config, fbc db.FileblockCreator[O], persistStrategies ...db.WalFlushStrategy[O]) db.Wal[O] {
	return &nmMemoryWal[O]{
		entries:          make(map[string]db.EntriesMap[O]),
		cfg:              cfg,
		fileblockCreator: fbc,
		flushStrategies:  persistStrategies,
	}
}

// nmInMemoryWal is a write-ahead log that stores entries in memory.
// That means that the entries are not persisted to disk until the
// a persist strategies is met or the WAL is closed (usually when
// closing the database)
type nmMemoryWal[O cmp.Ordered] struct {
	entries          map[string]db.EntriesMap[O]
	cfg              *db.Config
	fileblockCreator db.FileblockCreator[O]
	flushStrategies  []db.WalFlushStrategy[O]
}

func (w *nmMemoryWal[O]) Append(d db.Entry[O]) (err error) {
	fileEntries := w.entries[d.PrimaryIndex()]

	if fileEntries == nil {
		fileEntries = db.NewEntriesMap[O]()
		w.entries[d.PrimaryIndex()] = fileEntries
	}
	fileEntries.Append(d)

	for _, strategy := range w.flushStrategies {
		if strategy.ShouldFlush(fileEntries) {
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

func (w *nmMemoryWal[O]) Find(pIdx, sIdx string, min, max O) (db.EntryIterator[O], bool) {
	if pIdx == "" {
		entries := make([]db.Entry[O], 0)
		for _, fileEntries := range w.entries {
			for _, entry := range fileEntries {
				if sIdx == "" || entry.SecondaryIndex() == sIdx {
					entry.Sort()
					entries = append(entries, entry)
				}
			}
		}

		return db.NewListIterator(entries), len(entries) > 0
	}

	fileEntries := w.entries[pIdx]
	if fileEntries == nil {
		return nil, false
	}

	return fileEntries.Find(sIdx, min, max)
}

func (w *nmMemoryWal[O]) Close() error {
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
