package main

type Integer struct {
	N int32 `parquet:"name=n, type=INT32"`
}

func (i Integer) LessThan(a Entry) bool {
	return i.N < a.(Integer).N
}

func (i Integer) Equals(a Entry) bool {
	return i.N == a.(Integer).N
}
