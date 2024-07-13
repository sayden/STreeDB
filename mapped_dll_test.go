package streedb

import "testing"

func TestMappedDLL(t *testing.T) {
	dll := MapDLL[int32, Entry[int32], *Kv]{}
	dll.SetMax(NewKv("b", "idx1", []int32{}), NewKv("b", "idx1", []int32{}))
	dll.SetMax(NewKv("b", "idx1", []int32{}), NewKv("b", "idx1", []int32{}))
	dll.SetMax(NewKv("b", "idx1", []int32{}), NewKv("b", "idx1", []int32{}))
	dll.SetMax(NewKv("b", "idx1", []int32{}), NewKv("b", "idx1", []int32{}))
	dll.SetMax(NewKv("b", "idx1", []int32{}), NewKv("b", "idx1", []int32{}))
	dll.Remove(NewKv("b", "idx1", []int32{}))
	dll.Remove(NewKv("b", "idx1", []int32{}))

	dll = MapDLL[int32, Entry[int32], *Kv]{}
	dll.SetMin(NewKv("b", "idx1", []int32{}), NewKv("b", "idx1", []int32{}))
	dll.SetMin(NewKv("b", "idx1", []int32{}), NewKv("b", "idx1", []int32{}))
	dll.SetMin(NewKv("b", "idx1", []int32{}), NewKv("b", "idx1", []int32{}))
	dll.SetMin(NewKv("b", "idx1", []int32{}), NewKv("b", "idx1", []int32{}))
}
