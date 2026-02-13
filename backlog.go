package main

type CommandBacklog struct {
	capacity     uint64
	size         uint64
	writeHead    uint64
	backlogStart uint64
	Backlog      []byte
}

func (b *CommandBacklog) AddCommandBytes(buf []byte) {
	index := (b.writeHead + b.size) % b.capacity
	for _, v := range buf {
		b.Backlog[index] = v
		if index == b.writeHead && b.size == b.capacity {
			b.writeHead = (b.writeHead + 1) % b.capacity
			b.backlogStart++
		}
		b.size++
		index = (index + 1) % b.capacity
	}
}

func (b *CommandBacklog) ExtractNeededBytes(replOffset uint64, masterOffset uint64) ([]byte, bool) {
	// if replOffset < b.backlogStart {
	// 	return nil, false
	// }

	var ptr uint64
	for range replOffset - b.backlogStart + 1 {
		ptr = (ptr + 1) % b.capacity
	}

	var buf []byte
	for range masterOffset - replOffset + 1 {
		buf = append(buf, b.Backlog[ptr])
		ptr = (ptr + 1) % b.capacity
	}

	return buf, true
}

func (b *CommandBacklog) HasNeededBytes(replOffset uint64) bool {
	return b.backlogStart <= replOffset
}
