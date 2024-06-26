package streedb

const (
	MAX_LEVELS = 5
)

type Level[T Entry] interface {
	AppendBlock(b Metadata[T])
	RemoveBlocks(r map[int]struct{})
	Find(d T) (Entry, bool, error)
	Close()
}

type Levels[T Entry] interface {
	GetLevel(i int) []Metadata[T]
	AppendBlock(b Metadata[T])
	RemoveBlock(b Metadata[T]) error
}

func NewLevels[T Entry](maxLevels int) Levels[T] {
	l := make(BasicLevels[T], maxLevels+1)

	for i := 0; i < maxLevels+1; i++ {
		l[i] = make([]Metadata[T], 0)
	}

	return l
}

type BasicLevels[T Entry] map[int][]Metadata[T]

func (l BasicLevels[T]) GetLevel(i int) []Metadata[T] {
	return l[i]
}

func (l BasicLevels[T]) AppendBlock(b Metadata[T]) {
	l[b.GetLevel()] = append(l[b.GetLevel()], b)
}

func (l BasicLevels[T]) RemoveBlock(b Metadata[T]) error {
	idx := 0

	// search for block
	for i, block := range l[b.GetLevel()] {
		if block.GetID() == b.GetID() {
			// remove block
			if err := b.Remove(); err != nil {
				return err
			}
			idx = i
			break
		}
	}

	// remove block from slice
	l[b.GetLevel()] = append(l[b.GetLevel()][:idx], l[b.GetLevel()][idx+1:]...)

	return nil
}
