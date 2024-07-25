package fs

import (
	"cmp"
	"testing"

	db "github.com/sayden/streedb"
	"github.com/stretchr/testify/assert"
)

type mockFilesystem[O cmp.Ordered] struct {
	extra struct {
		create               int
		fillMetadataBuilder  int
		load                 int
		openMetaFilesInLevel int
		remove               int
		updateMetadata       int
	}

	es        *db.EntriesMap[O]
	builder   *db.MetadataBuilder[O]
	listeners []db.FileblockListener[O]
}

func (m *mockFilesystem[O]) Create(cfg *db.Config, es *db.EntriesMap[O], b *db.MetadataBuilder[O], ls []db.FileblockListener[O]) (*db.Fileblock[O], error) {
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
func (m *mockFilesystem[O]) FillMetadataBuilder(meta *db.MetadataBuilder[O]) *db.MetadataBuilder[O] {
	m.extra.fillMetadataBuilder++
	return nil
}
func (m *mockFilesystem[O]) Load(*db.Fileblock[O]) (*db.EntriesMap[O], error) {
	m.extra.load++
	return nil, nil
}
func (m *mockFilesystem[O]) OpenMetaFilesInLevel([]db.FileblockListener[O]) error {
	m.extra.openMetaFilesInLevel++
	return nil
}
func (m *mockFilesystem[O]) Remove(*db.Fileblock[O], []db.FileblockListener[O]) error {
	m.extra.remove++
	return nil
}
func (m *mockFilesystem[O]) UpdateMetadata(*db.Fileblock[O]) error {
	m.extra.updateMetadata++
	return nil
}

func TestLevelBasic(t *testing.T) {
	cfg := db.NewDefaultConfig()
	fs := mockFilesystem[int64]{}

	levels, err := NewLeveledFilesystem[int64, *db.Kv](cfg)
	assert.NoError(t, err)

	level := NewBasicLevel(cfg, &fs, levels)

	ts := []int64{1, 2, 3, 4}
	t.Run("Create", func(t *testing.T) {
		temp := make([]*db.Kv, 0)
		data := db.NewSliceToMap(temp)
		k1 := db.NewKv("key", "idx", ts[0:1], []int32{1})
		k2 := db.NewKv("key2", "idx", ts[0:1], []int32{2})
		data.Append(k1)
		data.Append(k2)

		builder := db.NewMetadataBuilder[int64](cfg).
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
