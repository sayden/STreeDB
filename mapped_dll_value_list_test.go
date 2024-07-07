package streedb

import "testing"

func TestDLLVL(t *testing.T) {
	idx := MapDLLVV[Integer, Integer]{}
	idx.SetMin(Integer{1}, Integer{1})
	idx.SetMin(Integer{4}, Integer{4})
	idx.SetMin(Integer{3}, Integer{3})
	idx.SetMin(Integer{1}, Integer{2})
	idx.SetMin(Integer{0}, Integer{0})

	idx.Remove(Integer{3}, Integer{3})
	idx.Remove(Integer{1}, Integer{1})
	idx.Remove(Integer{1}, Integer{2})
	idx.Remove(Integer{4}, Integer{3})
}
