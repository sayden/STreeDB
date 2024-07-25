package main

import (
	"cmp"
	"net/http"

	"github.com/gin-gonic/gin"
	db "github.com/sayden/streedb"
	"github.com/sayden/streedb/metrics"
)

type FromTo[O cmp.Ordered] struct {
	From O `json:"from"`
	To   O `json:"to"`
}

type ServerMetrics[O cmp.Ordered, E db.Entry[O]] struct {
	db *metrics.LSMMetrics[O, E]
}

func (s *ServerMetrics[O, _]) GETPrimaryAndSecondaryIndex(c *gin.Context) {
	pIdx := c.Param("pIdx")
	sIdx := c.Param("sIdx")

	fromTo := FromTo[O]{}
	if err := c.ShouldBindQuery(&fromTo); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	}

	// FIXME: This is a hack to get the min and max values
	min := fromTo.From
	max := fromTo.To

	iter, found, err := s.db.Find(pIdx, sIdx, min, max)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error(), "primary_index": pIdx, "secondary_index": sIdx, "from": min, "to": max})
		return
	}

	if !found {
		c.JSON(http.StatusNotFound, gin.H{"error": "data not found", "primary_index": pIdx, "secondary_index": sIdx, "from": min, "to": max})
		return
	}

	// Accumulate the metrics using the iterator
	em := db.NewEntriesMap[O]()
	for entry, found, err := iter.Next(); entry != nil && found && err == nil; entry, found, err = iter.Next() {
		em.Append(entry)
	}

	if sIdx == "" {
		c.JSON(http.StatusOK, em)
		return
	}

	c.JSON(http.StatusOK, em.Get(sIdx))
}

func (s *ServerMetrics[O, _]) GETMetricsAPI(c *gin.Context) {
	pIdx := c.Param("pIdx")
	sIdx := c.Param("sIdx")

	fromTo := FromTo[int64]{}
	if err := c.ShouldBindQuery(&fromTo); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	}

	min := fromTo.From
	max := fromTo.To

	iter, found, err := s.db.Metrics.Find(pIdx, sIdx, min, max)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error(), "primary_index": pIdx, "secondary_index": sIdx, "from": min, "to": max})
		return
	}

	if !found {
		c.JSON(http.StatusNotFound, gin.H{"error": "data not found", "primary_index": pIdx, "secondary_index": sIdx, "from": min, "to": max})
		return
	}

	em := db.NewEntriesMap[int64]()
	for entry, found, err := iter.Next(); entry != nil && found && err == nil; entry, found, err = iter.Next() {
		em.Append(entry)
	}

	if sIdx == "" {
		c.JSON(http.StatusOK, em)
		return
	}

	c.JSON(http.StatusOK, em.Get(sIdx))
}

func (s *ServerMetrics[O, E]) Ping(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"message": "pong",
	})
}

func (s *ServerMetrics[O, _]) getMetrics() (*db.EntriesMap[int64], bool, error) {
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
