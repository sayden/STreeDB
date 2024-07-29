package fs

import (
	"testing"

	db "github.com/sayden/streedb"
	"github.com/stretchr/testify/assert"
)

// func (m mockFilesystem) OpenMetaFilesInLevel(listeners []db.FileblockListener) error {
// 	return nil
// }

func TestMultiLevelFs(t *testing.T) {
	cfg := db.NewDefaultConfig()
	// fs := mockFilesystem[int32, *db.Kv]{}

	levels, err := NewLeveledFilesystem[int64, *db.Kv](cfg, nil)
	assert.NoError(t, err)
	_ = levels
}
