package main

import "strconv"

type Command struct {
	Name string
	Args [][]byte
}
type Parser struct{}

func (p Parser) ReadLine(buf []byte) ([]byte, int, bool) {
	var line []byte
	for i, v := range buf {
		if v == '\r' && (i+1) < len(buf) && buf[i+1] == '\n' { // delimiter found...
			line = buf[:i]           //extract chunk of slice up to but not including delimter
			return line, i + 2, true //TODO may need to return i + 1 not i + 2!!!
		}
	}
	return nil, 0, false
}

func (p Parser) TryParsingCommand(buf []byte) (Command, int, bool) {
	var cmd Command
	offset := 0
	line, consumed, ok := p.ReadLine(buf[offset:])
	if !ok {
		return Command{}, 0, false
	}

	offset += consumed
	cmdArrayLen := p.ReadPrefixLength(line)

	for i := 0; i < cmdArrayLen; i++ {
		line, consumed, ok := p.ReadLine(buf[offset:])
		if !ok {
			return Command{}, 0, false
		}
		offset += consumed
		prefixLen := p.ReadPrefixLength(line)
		if len(buf[offset:]) < prefixLen+2 {
			return Command{}, 0, false
		}

		bulkString := buf[offset : offset+prefixLen]
		offset += prefixLen + 2

		if i == 0 {
			cmd.Name = string(bulkString)
		} else {
			cmd.Args = append(cmd.Args, bulkString)
		}

	}
	return cmd, offset, true
}

func (p Parser) ReadPrefixLength(b []byte) int {

	var numElements int
	multiplier := 1

	for i := len(b) - 1; i >= 0; i-- {
		digit, err := strconv.Atoi(string(b[i]))
		if err != nil {
			continue
		}
		numElements += (digit * multiplier)
		multiplier *= 10
	}
	return numElements
}
