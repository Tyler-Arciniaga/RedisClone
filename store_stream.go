package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"time"
)

type EntryID struct {
	Base        []byte
	SequenceNum uint16
}

type StreamEntry struct {
	ID      EntryID
	KvPairs map[string][]byte
}

func (s *Store) StreamAdd(sr StreamAddRequest) ([]byte, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	stream, ok, err := s.GetAsStream(sr.Key)
	if !ok {
		stream = StreamData{RadixTree: RadixTree{root: &TrieNode{children: make(map[byte]*TrieNode)}}}
	} else {
		if err != nil {
			return nil, err
		}
	}

	// assign stream entry a new ID
	var entry StreamEntry
	entry.ID, err = s.GenerateStreamEntryID(sr.Id, stream.PrevEntryTime)
	if err != nil {
		return nil, err
	}

	//insert new stream entry into the Radix Tree
	stream.RadixTree.Insert(&entry)

	//update store with new stream
	s.store[sr.Key] = RedisObject{NativeType: Stream, Data: stream}

	//return the complete entry ID
	completeID := s.FormatEntryID(entry.ID)
	return completeID, nil
}

func (s *Store) FormatEntryID(e EntryID) []byte {
	//decode binary timestamp
	ms := binary.BigEndian.Uint64(e.Base)
	seq := e.SequenceNum

	return []byte(fmt.Sprintf("%d-%d", ms, seq))
}

func (s *Store) GenerateStreamEntryID(ChosenID []byte, prevEntry int64) (EntryID, error) {
	var id EntryID

	if ChosenID[0] != '*' {
		idx := bytes.Index(ChosenID, []byte{'-'})
		if idx == -1 {
			return EntryID{}, errors.New("ERR Invalid number of arguements passed to XADD command")
		}

		baseStr := ChosenID[:idx]
		baseInt, err := strconv.ParseUint(string(baseStr), 10, 64)
		if err != nil {
			return EntryID{}, errors.New("ERR invalid stream ID base")
		}

		// normalize base into 8-byte binary big-endian
		var base [8]byte
		binary.BigEndian.PutUint64(base[:], baseInt)

		// parse sequence num
		seqStr := ChosenID[idx+1:]
		seqInt, err := strconv.ParseUint(string(seqStr), 10, 16)
		if err != nil {
			return EntryID{}, errors.New("ERR invalid stream ID sequence")
		}

		id.Base = base[:] // always 8 bytes
		id.SequenceNum = uint16(seqInt)
	} else {
		ms := time.Now().Local().UnixMilli()
		ms = max(ms, prevEntry)

		id.Base = binary.BigEndian.AppendUint64(id.Base, uint64(ms)) //stores the 8 bytes encoding of ms into the base ID
	}

	return id, nil
}
