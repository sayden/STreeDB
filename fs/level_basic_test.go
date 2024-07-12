package fs

import (
	"cmp"
	"testing"

	db "github.com/sayden/streedb"
	"github.com/stretchr/testify/assert"
)

type mockFilesystem[O cmp.Ordered, E db.Entry[O]] struct {
	extra struct {
		create               int
		fillMetadataBuilder  int
		load                 int
		openMetaFilesInLevel int
		remove               int
		updateMetadata       int
	}

	es        db.Entries[O, E]
	builder   *db.MetadataBuilder[O]
	listeners []db.FileblockListener[O, E]
}

func (m *mockFilesystem[O, E]) Create(cfg *db.Config, es db.Entries[O, E], b *db.MetadataBuilder[O], ls []db.FileblockListener[O, E]) (*db.Fileblock[O, E], error) {
	m.es = es
	m.builder = b
	m.listeners = ls
	m.extra.create++

	meta, err := b.Build()
	if err != nil {
		return nil, err
	}

	return db.NewFileblock(cfg, meta, m), nil
}
func (m *mockFilesystem[O, E]) FillMetadataBuilder(meta *db.MetadataBuilder[O]) *db.MetadataBuilder[O] {
	m.extra.fillMetadataBuilder++
	return nil
}
func (m *mockFilesystem[O, E]) Load(*db.Fileblock[O, E]) (db.Entries[O, E], error) {
	m.extra.load++
	return nil, nil
}
func (m *mockFilesystem[O, E]) OpenMetaFilesInLevel([]db.FileblockListener[O, E]) error {
	m.extra.openMetaFilesInLevel++
	return nil
}
func (m *mockFilesystem[O, E]) Remove(*db.Fileblock[O, E], []db.FileblockListener[O, E]) error {
	m.extra.remove++
	return nil
}
func (m *mockFilesystem[O, E]) UpdateMetadata(*db.Fileblock[O, E]) error {
	m.extra.updateMetadata++
	return nil
}

func TestLevelBasic(t *testing.T) {
	cfg := db.NewDefaultConfig()
	fs := mockFilesystem[int32, *db.Kv]{}

	levels, err := NewLeveledFilesystem[int32, *db.Kv](cfg)
	assert.NoError(t, err)

	level := NewBasicLevel(cfg, &fs, levels)

	t.Run("Create", func(t *testing.T) {
		temp := make([]*db.Kv, 0)
		data := db.NewSliceToMap(temp)
		k1 := db.NewKv("key", []int32{1}, "idx")
		k2 := db.NewKv("key2", []int32{2}, "idx")
		data.Append(k1)
		data.Append(k2)

		builder := db.NewMetadataBuilder[int32](cfg).
			WithEntry(k1).
			WithEntry(k2).
			WithLevel(1).
			WithFilenamePrefix("01").
			WithRootPath("/tmp/db/json").
			WithExtension("ext")

		fb, err := level.Create(data, builder)
		assert.NoError(t, err)
		assert.NotNil(t, fb)
		assert.Equal(t, 1, fs.extra.create)

		t.Run("Remove", func(t *testing.T) {
			err := level.RemoveFile(fb)
			assert.NoError(t, err)
			assert.Equal(t, 1, fs.extra.remove)
		})
	})

}
