package core

import (
	"os"
	"testing"

	db "github.com/sayden/streedb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStreetDB(t *testing.T) {
	t.Cleanup(func() {
		os.RemoveAll("/tmp/db")
	})

	defaultCfg := db.NewDefaultConfig()

	cfg := &db.Config{
		Wal:        defaultCfg.Wal,
		Filesystem: db.FilesystemTypeMap[db.FILESYSTEM_TYPE_LOCAL],
		MaxLevels:  5,
		DbPath:     "/tmp/db/parquet",
		Compaction: defaultCfg.Compaction,
	}

	lsmtree, err := NewLsmTree[int64, *db.Kv](cfg)
	require.NoError(t, err)
	defer lsmtree.Close()

	err = lsmtree.Close()
	require.NoError(t, err)

	err = lsmtree.Compact()
	require.NoError(t, err)

	val, found, err := lsmtree.Find("instance1", "cpu", 0, 4)
	require.NoError(t, err)
	assert.True(t, found)
	require.NotNil(t, val)
}
