package core

import (
	"fmt"
	"testing"

	"github.com/sayden/streedb"
	"github.com/stretchr/testify/assert"
	"github.com/thehivecorporation/log"
)

func TestLsmTreestreedb(t *testing.T) {
	walSize := 5 //items

	lsmtree, err := NewLsmTree[streedb.Integer]("/tmp/integer", walSize)
	if err != nil {
		t.Fatal(err)
	}
	defer lsmtree.Close()

	// for i := 0; i < 100; i++ {
	// 	lsmtree.Append(streedb.Integer{int32(i)})
	// }

	val, found, err := lsmtree.Find(streedb.Integer{N: int32(6)})
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("value not found")
	}
	assert.Equal(t, streedb.Integer{N: int32(6)}, val)

}
func TestLsmTreeKv(t *testing.T) {
	log.SetLevel(log.LevelInfo)

	walSize := 50 //items

	lsmtree2, err := NewLsmTree[streedb.LexicographicKv]("/tmp/kv", walSize)
	if err != nil {
		t.Fatal(err)
	}
	defer lsmtree2.Close()
	compact := false

	compact = true
	var i int32
	for i < 100 {
		lsmtree2.Append(streedb.LexicographicKv{Key: fmt.Sprintf("hello %02d", i), Val: i})
		i++
	}

	entry := streedb.NewLexicographicKv("hello 06", 0)
	val, found, err := lsmtree2.Find(*entry)
	assert.NoError(t, err)
	if !found {
		t.Fatal("value not found")
	}
	assert.Equal(t, int32(6), val.(streedb.LexicographicKv).Val)
	if compact {
		err = lsmtree2.Compact()
		assert.NoError(t, err)
	}
}
