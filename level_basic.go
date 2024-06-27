package streedb

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
