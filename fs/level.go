package fs

import (
	"cmp"
	"errors"
	"fmt"

	db "github.com/sayden/streedb"
)

func NewBasicLevel[O cmp.Ordered](cfg *db.Config, fs db.Filesystem[O], levels *MultiFsLevels[O]) *BasicLevel[O] {
	level := &BasicLevel[O]{
		cfg:                cfg,
		filesystem:         fs,
		fileblockListeners: []db.FileblockListener[O]{levels},
	}

	err := fs.OpenMetaFilesInLevel(level.fileblockListeners)
	if err != nil {
		panic(err)
	}

	return level
}

type BasicLevel[O cmp.Ordered] struct {
	filesystem db.Filesystem[O]
	cfg        *db.Config

	// we store the listeners here to pass them down the stack to the filesystems operations
	fileblockListeners []db.FileblockListener[O]
}

func (b *BasicLevel[O]) Create(es db.EntriesMap[O], builder *db.MetadataBuilder[O]) (*db.Fileblock[O], error) {
	fileblock, err := b.filesystem.Create(b.cfg, es, builder, b.fileblockListeners)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("error creating block at level: "), err)
	}

	return fileblock, nil
}

func (b *BasicLevel[O]) RemoveFile(f *db.Fileblock[O]) error {
	return b.filesystem.Remove(f, b.fileblockListeners)
}

func (b *BasicLevel[O]) Close() error {
	return nil
}
