package main

import (
	"time"

	"github.com/gin-gonic/gin"
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

	dbWrapper, err := metrics.New[int64, *db.Kv](coreDb)
	if err != nil {
		panic(err)
	}
	defer dbWrapper.Close()

	go func() {
		// Create one kv
		ts := make([]int64, 0, 100)
		vals := make([]int32, 0, 100)
		for i := 0; i < 100; i++ {
			ts = append(ts, time.Now().UnixMilli())
			vals = append(vals, int32(i))
			time.Sleep(time.Millisecond)
		}

		kv := db.NewKv("instance1", "cpu", ts, vals)
		if err = dbWrapper.Append(kv); err != nil {
			panic(err)
		}

		kv = db.NewKv("instance1", "mem", ts, vals)
		if err = dbWrapper.Append(kv); err != nil {
			panic(err)
		}
	}()

	// Start the metrics server
	metricsServer := &ServerMetrics[int64, *db.Kv]{db: dbWrapper}

	router := gin.Default()

	router.GET("/ping", metricsServer.Ping)
	router.GET("/api/metrics", metricsServer.GETMetricsAPI)
	router.GET("/api/:pIdx", metricsServer.GETPrimaryAndSecondaryIndex)
	router.GET("/api/:pIdx/:sIdx", metricsServer.GETPrimaryAndSecondaryIndex)
	router.GET("/", metricsServer.GETIndex)

	router.Run()
}
