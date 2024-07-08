package streedb

import "testing"

func TestMappedDLL(t *testing.T) {
	dll := MapDLL[Entry, Integer]{}
	dll.SetMax(NewInteger(1, "a", "b"), NewInteger(1, "a", "b"))
	dll.SetMax(NewInteger(4, "a", "b"), NewInteger(4, "a", "b"))
	dll.SetMax(NewInteger(3, "a", "b"), NewInteger(3, "a", "b"))
	dll.SetMax(NewInteger(2, "a", "b"), NewInteger(2, "a", "b"))
	dll.SetMax(NewInteger(0, "a", "b"), NewInteger(0, "a", "b"))
	dll.Remove(NewInteger(3, "a", "b"))
	dll.Remove(NewInteger(0, "a", "b"))

	dll = MapDLL[Entry, Integer]{}
	dll.SetMin(NewInteger(1, "a", "b"), NewInteger(1, "a", "b"))
	dll.SetMin(NewInteger(4, "a", "b"), NewInteger(4, "a", "b"))
	dll.SetMin(NewInteger(3, "a", "b"), NewInteger(3, "a", "b"))
	dll.SetMin(NewInteger(2, "a", "b"), NewInteger(2, "a", "b"))
}
