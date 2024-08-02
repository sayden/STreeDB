package core

import (
	"testing"

	db "github.com/sayden/streedb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStreeDB(t *testing.T) {
	// t.Cleanup(func() {
	// 	os.RemoveAll("/tmp/db")
	// })

	cfg := db.NewDefaultConfig()
	cfg.Filesystem = db.FilesystemTypeMap[db.FILESYSTEM_TYPE_LOCAL]

	lsmtree, err := NewLsmTree[int64, *db.Kv](cfg)
	require.NoError(t, err)
	defer lsmtree.Close()

	ts := make([]int64, 0, 1000)
	vals := make([]int32, 0, 1000)
	for i := 0; i < 31536000; i++ {
		ts = append(ts, int64(i))
		vals = append(vals, int32(i))
		if i != 0 && i%1000 == 0 {
			err = lsmtree.Append(db.NewKv("instance1", "cpu", ts, vals))
			require.NoError(t, err)
			ts = ts[:0]
			vals = vals[:0]
		}

		if i != 0 && i%100000 == 0 {
			err = lsmtree.Compact()
			assert.NoError(t, err)
			t.Logf("i: %d", i)
		}
	}
	err = lsmtree.Append(db.NewKv("instance1", "cpu", ts, vals))
	require.NoError(t, err)

	iter, found, err := lsmtree.Find("instance1", "cpu", 12345, 12399)
	require.NoError(t, err)
	assert.True(t, found)
	require.NotNil(t, iter)
}
