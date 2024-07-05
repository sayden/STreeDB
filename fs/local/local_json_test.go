package fslocal

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	db "github.com/sayden/streedb"
	"github.com/stretchr/testify/assert"
)

func TestInitJSONLocal(t *testing.T) {
	t.Cleanup(func() {
		os.RemoveAll("/tmp/db")
	})

	cfg := &db.Config{DbPath: "/tmp", Filesystem: "local", Format: "json"}
	fs, err := InitJSONLocal[db.Integer](cfg, 0)
	assert.NoError(t, err)
	assert.NotNil(t, fs)

	t.Run("Create", func(t *testing.T) {
		t.Run("EmptyEntries", func(t *testing.T) {
			meta, err := db.NewMetadataBuilder[db.Integer]().Build()
			assert.NoError(t, err)
			es := db.Entries[db.Integer]{}
			fileblock, err := fs.Create(cfg, es, meta)
			assert.Error(t, err)
			assert.Nil(t, fileblock)
		})

		t.Run("2 entries", func(t *testing.T) {
			es := db.Entries[db.Integer]{db.Integer{N: 1}, db.Integer{N: 2}}
			assert.Equal(t, 2, len(es))

			meta, err := db.NewMetadataBuilder[db.Integer]().WithEntries(es).Build()
			assert.NoError(t, err)
			fileblock, err := fs.Create(cfg, es, meta)
			assert.NoError(t, err)
			assert.NotNil(t, fileblock)

			f1, err := os.Open(meta.MetaFilepath)
			assert.NoError(t, err)
			defer f1.Close()

			f2, err := os.Open(meta.DataFilepath)
			assert.NoError(t, err)
			defer f2.Close()
			es2 := db.Entries[db.Integer]{}
			err = json.NewDecoder(f2).Decode(&es2)
			assert.NoError(t, err)
			assert.Equal(t, 2, len(es2))
			assert.Equal(t, int32(1), es2[0].N)

			assert.Equal(t, int64(18), fileblock.Metadata().Size)
			assert.Equal(t, 2, fileblock.Metadata().ItemCount)
			assert.Equal(t, int32(1), fileblock.Metadata().Min.N)
			assert.Equal(t, int32(2), fileblock.Metadata().Max.N)
			assert.Equal(t, 0, fileblock.Metadata().Level)
			assert.NotEmpty(t, fileblock.Metadata().DataFilepath)
			assert.NotEmpty(t, fileblock.Metadata().MetaFilepath)
			assert.NotEmpty(t, fileblock.Metadata().Uuid)
			assert.NotEqual(t, time.Time{}, fileblock.Metadata().CreatedAt)

			t.Run("load the data now", func(t *testing.T) {
				es, err := fs.Load(fileblock)
				assert.NoError(t, err)
				assert.NotNil(t, meta)
				assert.Equal(t, 2, len(es))
				assert.Equal(t, int32(1), es[0].N)
				assert.Equal(t, int32(2), es[1].N)
			})

			t.Run("remove the fileblock", func(t *testing.T) {
				err := fs.Remove(fileblock)
				assert.NoError(t, err)
				_, err = os.Open(meta.MetaFilepath)
				assert.Error(t, err)
				_, err = os.Open(meta.DataFilepath)
				assert.Error(t, err)
			})
		})

	})
}
