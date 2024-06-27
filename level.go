package streedb

const (
	MAX_LEVELS = 5
)

type Level[T Entry] interface {
	AppendFile(b Fileblock[T])
	RemoveFiles(r map[int]struct{})
	Find(d T) (Entry, bool, error)
	Close()
}

type Levels[T Entry] interface {
	GetLevel(i int) []Fileblock[T]
	AppendFile(b Fileblock[T])
	RemoveFile(b Fileblock[T]) error
}

func NewLevels[T Entry](maxLevels int) Levels[T] {
	l := make(BasicLevels[T], maxLevels+1)

	for i := 0; i < maxLevels+1; i++ {
		l[i] = make([]Fileblock[T], 0)
	}

	return l
}

type BasicLevels[T Entry] map[int][]Fileblock[T]

func (l BasicLevels[T]) GetLevel(i int) []Fileblock[T] {
	return l[i]
}

func (l BasicLevels[T]) AppendFile(b Fileblock[T]) {
	l[b.GetLevel()] = append(l[b.GetLevel()], b)
}

func (l BasicLevels[T]) RemoveFile(b Fileblock[T]) error {
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
