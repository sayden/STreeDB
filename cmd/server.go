package main

import (
	"cmp"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	db "github.com/sayden/streedb"
	"github.com/sayden/streedb/metrics"
)

type ServerMetrics[O cmp.Ordered, E db.Entry[O]] struct {
	db *metrics.LSMMetrics[O, E]
}

func (s *ServerMetrics[_, _]) GETIndex(c *gin.Context) {
	em, found, err := s.getMetrics()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if !found {
		c.JSON(http.StatusNotFound, gin.H{"error": "metrics not found"})
		return
	}

	c.HTML(http.StatusOK, "index.html", em)
}

func (s *ServerMetrics[O, _]) GETPrimaryAndSecondaryIndex(c *gin.Context) {
	pIdx := c.Param("pIdx")
	sIdx := c.Param("sIdx")
	now := time.Now().UnixMilli()

	// FIXME: This is a hack to get the min and max values
	min := O(0)
	max := O(now)

	iter, found, err := s.db.Find(pIdx, sIdx, min, max)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if !found {
		c.JSON(http.StatusNotFound, gin.H{"error": "metrics not found"})
		return
	}

	// Accumulate the metrics using the iterator
	em := db.NewEntriesMap[O]()
	for entry, found, err := iter.Next(); entry != nil && found && err == nil; entry, found, err = iter.Next() {
		em.Append(entry)
	}

	c.JSON(http.StatusOK, em)
}

func (s *ServerMetrics[_, _]) GETMetricsAPI(c *gin.Context) {
	em, found, err := s.getMetrics()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if !found {
		c.JSON(http.StatusNotFound, gin.H{"error": "metrics not found"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"metrics": em,
	})
}

func (s *ServerMetrics[O, E]) Ping(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"message": "pong",
	})
}

func (s *ServerMetrics[O, _]) getMetrics() (db.EntriesMap[int64], bool, error) {
	iter, found, err := s.db.GetMetrics()
	if err != nil {
		return nil, found, err
	}

	if !found {
		return nil, found, nil
	}

	// Accumulate the metrics using the iterator
	em := db.NewEntriesMap[int64]()
	var entry db.Entry[int64]
	for entry, found, err = iter.Next(); entry != nil && found && err == nil; entry, found, err = iter.Next() {
		em.Append(entry)
	}

	return em, true, nil
}
