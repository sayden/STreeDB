package main

import (
	"time"

	db "github.com/sayden/streedb"
	"github.com/sayden/streedb/core"
	"github.com/sayden/streedb/metrics"
)

func main() {
	cfg := db.NewDefaultConfig()
	coreDb, err := core.NewLsmTree[int64, *db.Kv](cfg)
	if err != nil {
		panic(err)
	}
	defer coreDb.Close()

	metrics, err := metrics.New[int64, *db.Kv](coreDb)
	if err != nil {
		panic(err)
	}
	defer metrics.Close()

	// Create one kv
	ts := make([]int64, 0, 100)
	vals := make([]int32, 0, 100)
	for i := 0; i < 100; i++ {
		ts = append(ts, time.Now().UnixMilli())
		vals = append(vals, int32(i))
		time.Sleep(time.Millisecond)
	}

	kv := db.NewKv("instance1", "cpu", ts, vals)
	if err = metrics.Append(kv); err != nil {
		panic(err)
	}
}
