package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/thehivecorporation/log"
)

func TestLsmTreeInteger(t *testing.T) {
	walSize := 5 //items

	lsmtree, err := NewLsmTree[Integer]("/tmp/integer", walSize)
	if err != nil {
		t.Fatal(err)
	}
	defer lsmtree.Close()

	// for i := 0; i < 100; i++ {
	// 	lsmtree.Append(Integer{int32(i)})
	// }

	val, found, err := lsmtree.Find(Integer{int32(6)})
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("value not found")
	}
	assert.Equal(t, Integer{int32(6)}, val)

}
func TestLsmTreeKv(t *testing.T) {
	log.SetLevel(log.LevelInfo)

	walSize := 50 //items

	lsmtree2, err := NewLsmTree[LexicographicKv]("/tmp/kv", walSize)
	if err != nil {
		t.Fatal(err)
	}
	defer lsmtree2.Close()
	compact := false

	// compact = true
	// var i int32
	// for i < 100 {
	// 	lsmtree2.Append(LexicographicKv{Key: fmt.Sprintf("hello %02d", i), Val: i})
	// 	i++
	// }

	entry := NewLexicographicKv("hello 06", 0)
	val, found, err := lsmtree2.Find(*entry)
	assert.NoError(t, err)
	if !found {
		t.Fatal("value not found")
	}
	assert.Equal(t, int32(6), val.(LexicographicKv).Val)
	if compact {
		err = lsmtree2.Compact()
		assert.NoError(t, err)
	}
}
