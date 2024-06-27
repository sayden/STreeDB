package core

import (
	"testing"

	"github.com/sayden/streedb"
	"github.com/sayden/streedb/destfs"
	"github.com/stretchr/testify/assert"
	"github.com/thehivecorporation/log"
)

func TestLsmTreestreedb(t *testing.T) {
	walSize := 5 //items

	lsmtree, err := NewLsmTree[streedb.Integer]("/tmp/integer", destfs.DEST_FS_LOCAL, walSize)
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

	walSize := 5 //items

	lsmtree, err := NewLsmTree[streedb.Kv]("/tmp/kv", destfs.DEST_FS_LOCAL, walSize)
	if err != nil {
		t.Fatal(err)
	}
	defer lsmtree.Close()
	compact := false

	// compact = true
	// r := rand.New(rand.NewSource(42))
	// n := r.Int31()
	// 	// lsmtree.Append(streedb.Kv{Key: fmt.Sprintf("hello %02d", n), Val: n})

	// var i int32
	// for i < 25 {
	// 	lsmtree.Append(streedb.Kv{Key: fmt.Sprintf("hello %02d", i), Val: i})
	// 	i++
	// }

	// tree := btree.NewG(2,
	// 	func(a, b streedb.MetaFile[streedb.Kv]) bool {
	// 		return a.MinVal.LessThan(b.MinVal)
	// 	})
	//
	// for _, block := range lsmtree.levels.GetLevel(0) {
	// 	b := block.(*fileformat.LocalBlockJSON[streedb.Kv])
	// 	tree.ReplaceOrInsert(b.MetaFile)
	// }
	//
	// min := streedb.MetaFile[streedb.Kv]{
	// 	MinVal: streedb.Kv{Key: "hello 06", Val: 0},
	// }
	// max := streedb.MetaFile[streedb.Kv]{
	// 	MinVal: streedb.Kv{Key: "hello 19", Val: 0},
	// }
	//
	// tree.DescendLessOrEqual(min, func(item streedb.MetaFile[streedb.Kv]) bool {
	// 	fmt.Printf("item: %#v, %#v\n", item.MinVal, item.MaxVal)
	// 	return false
	// })
	//
	// tree.AscendRange(min, max, func(item streedb.MetaFile[streedb.Kv]) bool {
	// 	fmt.Printf("item: %#v, %#v\n", item.MinVal, item.MaxVal)
	// 	return true
	// })

	if compact {
		err = lsmtree.Compact()
		assert.NoError(t, err)
	}

	entry := streedb.NewLexicographicKv("hello 06", 0)
	val, found, err := lsmtree.Find(*entry)
	assert.NoError(t, err)
	if !found {
		t.Fatal("value not found")
	}
	assert.Equal(t, int32(6), val.(streedb.Kv).Val)
}

func TestLsmS3(t *testing.T) {
	log.SetLevel(log.LevelDebug)

	walSize := 5 //items

	lsmtree, err := NewLsmTree[streedb.Kv]("my-bucket", destfs.DEST_FS_S3, walSize)
	if err != nil {
		t.Fatal(err)
	}
	defer lsmtree.Close()

	compact := false

	// compact = true
	// r := rand.New(rand.NewSource(42))
	// n := r.Int31()

	// var i int32
	// for i < 25 {
	// lsmtree.Append(streedb.Kv{Key: fmt.Sprintf("hello %02d", n), Val: n})
	// 	lsmtree.Append(streedb.Kv{Key: fmt.Sprintf("hello %02d", i), Val: i})
	// 	i++
	// }

	if compact {
		err = lsmtree.Compact()
		assert.NoError(t, err)
	}

	entry := streedb.NewLexicographicKv("hello 06", 0)
	val, found, err := lsmtree.Find(*entry)
	assert.NoError(t, err)
	if !found {
		t.Fatal("value not found")
	}
	assert.Equal(t, int32(6), val.(streedb.Kv).Val)
}
