package streedb

type BasicLevels[T Entry] map[int][]Fileblock[T]

func (l BasicLevels[T]) GetLevel(i int) []Fileblock[T] {
	return l[i]
}

func (l BasicLevels[T]) AppendFile(b Fileblock[T]) {
	level := b.Metadata().Level
	l[level] = append(l[level], b)
}

func (l BasicLevels[T]) RemoveFile(b Fileblock[T]) error {
	idx := 0

	meta := b.Metadata()
	level := meta.Level
	// search for block
	for i, block := range l[level] {
		if block.Metadata().Uuid == meta.Uuid {
			// remove block
			if err := b.Remove(); err != nil {
				return err
			}
			idx = i
			break
		}
	}

	// remove block from slice
	l[level] = append(l[level][:idx], l[level][idx+1:]...)

	return nil
}
