package streedb

import "testing"

func TestMappedDLL(t *testing.T) {
	dll := MapDLL[int32, Entry[int32], *Kv]{}
	dll.SetMax(NewKv("b", []int32{}, "idx1"), NewKv("b", []int32{}, "idx1"))
	dll.SetMax(NewKv("b", []int32{}, "idx1"), NewKv("b", []int32{}, "idx1"))
	dll.SetMax(NewKv("b", []int32{}, "idx1"), NewKv("b", []int32{}, "idx1"))
	dll.SetMax(NewKv("b", []int32{}, "idx1"), NewKv("b", []int32{}, "idx1"))
	dll.SetMax(NewKv("b", []int32{}, "idx1"), NewKv("b", []int32{}, "idx1"))
	dll.Remove(NewKv("b", []int32{}, "idx1"))
	dll.Remove(NewKv("b", []int32{}, "idx1"))

	dll = MapDLL[int32, Entry[int32], *Kv]{}
	dll.SetMin(NewKv("b", []int32{}, "idx1"), NewKv("b", []int32{}, "idx1"))
	dll.SetMin(NewKv("b", []int32{}, "idx1"), NewKv("b", []int32{}, "idx1"))
	dll.SetMin(NewKv("b", []int32{}, "idx1"), NewKv("b", []int32{}, "idx1"))
	dll.SetMin(NewKv("b", []int32{}, "idx1"), NewKv("b", []int32{}, "idx1"))
}
