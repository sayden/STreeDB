package fsmemory

import (
	"cmp"
	"errors"

	"github.com/puzpuzpuz/xsync/v3"
	db "github.com/sayden/streedb"
)

func NewMemoryFs[O cmp.Ordered](cfg *db.Config) db.Filesystem[O] {
	return &memoryFs[O]{
		cfg:  cfg,
		data: xsync.NewMapOf[string, *db.EntriesMap[O]](),
	}
}

type memoryFs[O cmp.Ordered] struct {
	cfg *db.Config
	// data map[string]*db.EntriesMap[O]
	data *xsync.MapOf[string, *db.EntriesMap[O]]
}

func (m *memoryFs[O]) Create(cfg *db.Config, es *db.EntriesMap[O], builder *db.MetadataBuilder[O], ls []db.FileblockListener[O]) (*db.Fileblock[O], error) {
	builder = m.FillMetadataBuilder(builder)
	meta, err := builder.Build()
	if err != nil {
		return nil, errors.Join(errors.New("error building metadata"), err)
	}
	if meta.Level == m.cfg.MaxLevels {
		// just delete the entries
		return nil, nil
	}

	m.data.Store(meta.Uuid, es)
	block := db.NewFileblock(m.cfg, meta, m)

	for _, listener := range ls {
		listener.OnFileblockCreated(block)
	}

	return block, nil
}

func (m *memoryFs[O]) FillMetadataBuilder(meta *db.MetadataBuilder[O]) *db.MetadataBuilder[O] {
	return meta.WithExtension(".memory")
}

func (m *memoryFs[O]) Load(fb *db.Fileblock[O]) (*db.EntriesMap[O], error) {
	val, found := m.data.Load(fb.Metadata().Uuid)
	if !found {
		return nil, errors.New("fileblock not found")
	}

	return val, nil
}

func (m *memoryFs[O]) OpenMetaFilesInLevel([]db.FileblockListener[O]) error {
	return nil
}

func (m *memoryFs[O]) Remove(fb *db.Fileblock[O], listeners []db.FileblockListener[O]) error {
	m.data.Delete(fb.Metadata().Uuid)

	for _, listener := range listeners {
		listener.OnFileblockRemoved(fb)
	}

	return nil
}

func (m *memoryFs[O]) UpdateMetadata(fb *db.Fileblock[O]) error {
	return nil
}
