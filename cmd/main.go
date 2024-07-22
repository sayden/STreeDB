package main

import (
	"net/http"
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

	r := gin.Default()

	r.GET("/ping", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"message": "pong",
		})
	})

	r.GET("/metrics", metricsServer.GetMetrics)

	r.GET("/:pIdx", func(c *gin.Context) {
		pIdx := c.Param("pIdx")
		iter, found, err := dbWrapper.Find(pIdx, "", 0, time.Now().UnixMilli())
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		if !found {
			c.JSON(http.StatusNotFound, gin.H{"error": "metrics not found"})
			return
		}

		// Accumulate the metrics using the iterator
		em := db.NewEntriesMap[int64]()
		for entry, found, err := iter.Next(); entry != nil && found && err == nil; entry, found, err = iter.Next() {
			em.Append(entry)
		}

		c.JSON(http.StatusOK, em)
	})

	r.GET("/:pIdx/:sIdx", func(c *gin.Context) {
		pIdx := c.Param("pIdx")
		sIdx := c.Param("sIdx")
		iter, found, err := dbWrapper.Find(pIdx, sIdx, 0, time.Now().UnixMilli())
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		if !found {
			c.JSON(http.StatusNotFound, gin.H{"error": "metrics not found"})
			return
		}

		// Accumulate the metrics using the iterator
		em := db.NewEntriesMap[int64]()
		for entry, found, err := iter.Next(); entry != nil && found && err == nil; entry, found, err = iter.Next() {
			em.Append(entry)
		}

		c.JSON(http.StatusOK, em)
	})

	r.Run()
}
