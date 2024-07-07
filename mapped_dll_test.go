package streedb

import "testing"

func TestMappedDLL(t *testing.T) {
	dll := MapDLL[Entry, UUIdentifiable]{}
	dll.SetMax(Integer{1}, Integer{1})
	dll.SetMax(Integer{4}, Integer{4})
	dll.SetMax(Integer{3}, Integer{3})
	dll.SetMax(Integer{2}, Integer{2})
	dll.SetMax(Integer{0}, Integer{0})
	dll.Remove(Integer{3})
	dll.Remove(Integer{0})

	dll = MapDLL[Entry, UUIdentifiable]{}
	dll.SetMin(Integer{1}, Integer{1})
	dll.SetMin(Integer{4}, Integer{4})
	dll.SetMin(Integer{3}, Integer{3})
	dll.SetMin(Integer{2}, Integer{2})
}
