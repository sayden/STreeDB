package streedb

type Level[T Entry] interface {
	AppendFile(b Fileblock[T])
	RemoveFiles(r map[int]struct{})
	Find(d T) (Entry, bool, error)
	Fileblocks() []Fileblock[T]
	Close() error
}

type Levels[T Entry] interface {
	GetLevel(i int) []Fileblock[T]
	AppendFile(b Fileblock[T])
	RemoveFile(b Fileblock[T]) error
}

// NewLevels is redundant atm because there is only one implementation of Levels, but facilitates
// refactor
func NewLevels[T Entry](c *Config) Levels[T] {
	l := make(BasicLevels[T], c.MaxLevels+1)

	for i := 0; i < c.MaxLevels+1; i++ {
		l[i] = make([]Fileblock[T], 0)
	}

	return l
}
