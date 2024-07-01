package streedb

type Level[T Entry] interface {
	AppendFile(b Fileblock[T])
	RemoveFile(b Fileblock[T]) error
	Find(d T) (Entry, bool, error)
	Fileblocks() []Fileblock[T]
	Close() error
}

type Levels[T Entry] interface {
	GetLevel(i int) Level[T]
	AppendFile(b Fileblock[T])
	AppendLevel(l Level[T], level int)
	RemoveFile(b Fileblock[T]) error
	Close() error
}

func NewLevel[T Entry](data []Fileblock[T]) Level[T] {
	// find min and max
	meta := data[0].Metadata()
	min := meta.Min
	max := meta.Max
	for _, block := range data {
		meta = block.Metadata()
		if meta.Min.LessThan(min) {
			min = meta.Min
		}
		if max.LessThan(meta.Max) {
			max = meta.Max
		}
	}

	return &BasicLevel[T]{fileblocks: data, min: min, max: max}
}

// NewLevels is redundant atm because there is only one implementation of Levels, but facilitates
// refactor
func NewLevels[T Entry](c *Config, fs Filesystem[T]) Levels[T] {
	return NewBasicLevels(c, fs)
}
