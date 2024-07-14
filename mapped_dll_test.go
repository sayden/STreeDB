package streedb

import "testing"

func TestMappedDLL(t *testing.T) {
	dll := MapDLL[int64, Entry[int64], *Kv]{}
	emptyTs := []int64{}
	dll.SetMax(NewKv("b", "idx1", emptyTs, []int32{}), NewKv("b", "idx1", emptyTs, []int32{}))
	dll.SetMax(NewKv("b", "idx1", emptyTs, []int32{}), NewKv("b", "idx1", emptyTs, []int32{}))
	dll.SetMax(NewKv("b", "idx1", emptyTs, []int32{}), NewKv("b", "idx1", emptyTs, []int32{}))
	dll.SetMax(NewKv("b", "idx1", emptyTs, []int32{}), NewKv("b", "idx1", emptyTs, []int32{}))
	dll.SetMax(NewKv("b", "idx1", emptyTs, []int32{}), NewKv("b", "idx1", emptyTs, []int32{}))
	dll.Remove(NewKv("b", "idx1", emptyTs, []int32{}))
	dll.Remove(NewKv("b", "idx1", emptyTs, []int32{}))

	dll = MapDLL[int64, Entry[int64], *Kv]{}
	dll.SetMin(NewKv("b", "idx1", emptyTs, []int32{}), NewKv("b", "idx1", emptyTs, []int32{}))
	dll.SetMin(NewKv("b", "idx1", emptyTs, []int32{}), NewKv("b", "idx1", emptyTs, []int32{}))
	dll.SetMin(NewKv("b", "idx1", emptyTs, []int32{}), NewKv("b", "idx1", emptyTs, []int32{}))
	dll.SetMin(NewKv("b", "idx1", emptyTs, []int32{}), NewKv("b", "idx1", emptyTs, []int32{}))
}
