package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"
)

func (s *Store) GetStreamLen(key string) (int, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	stream, ok, err := s.GetAsStream(key)
	if !ok {
		return 0, nil
	} else {
		if err != nil {
			return 0, err
		}
	}

	return stream.Len, nil
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
	stream.Len++
	s.store[sr.Key] = RedisObject{NativeType: Stream, Data: stream}

	//return the complete entry ID
	completeID := s.FormatEntryID(entry.ID)
	return completeID, nil
}

func (s *Store) StreamRange(sr StreamRangeRequest) ([][]byte, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	stream, ok, err := s.GetAsStream(sr.Key)
	if !ok {
		return nil, nil //TODO might need to return empty array specifically
	} else {
		if err != nil {
			return nil, err
		}
	}

	startID, err1 := s.NormalizeEntryID(sr.StartID, true)
	endID, err2 := s.NormalizeEntryID(sr.EndID, false)
	if err1 != nil || err2 != nil {
		return nil, err1
	}

	fmt.Println(startID)
	fmt.Println(endID)

	bytes, err := stream.RadixTree.Traverse(startID, endID)
	if err != nil {
		return nil, err
	}

	return bytes, nil
}

func (s *Store) NormalizeEntryID(id []byte, isStart bool) (EntryID, error) {
	var e EntryID
	var base [8]byte
	switch id[0] {
	case '-':
		binary.BigEndian.PutUint64(base[:], 0)
		e.Base = base[:]
		e.SequenceNum = 0
	case '+':
		binary.BigEndian.PutUint64(base[:], math.MaxInt64)
		e.Base = base[:]
		e.SequenceNum = math.MaxUint64
	default:
		//Handle specified entry IDs (may be partially incomplete though and missing sequence number)
		idx := bytes.Index(id, []byte{'-'})
		if idx != -1 {
			baseStr := id[:idx]
			baseInt, err := strconv.ParseUint(string(baseStr), 10, 64)
			if err != nil {
				return EntryID{}, errors.New("ERR invalid stream ID base")
			}

			// normalize base into 8-byte binary big-endian
			binary.BigEndian.PutUint64(base[:], baseInt)
			e.Base = base[:]

			// parse sequence num
			seqStr := id[idx+1:]
			seqInt, err := strconv.ParseUint(string(seqStr), 10, 16)
			if err != nil {
				return EntryID{}, errors.New("ERR invalid stream ID sequence")
			}
			e.SequenceNum = seqInt

		} else {
			//sequence number was not specified...
			baseStr := id[:]
			baseInt, err := strconv.ParseUint(string(baseStr), 10, 64)
			if err != nil {
				return EntryID{}, errors.New("ERR invalid stream ID base")
			}

			// normalize base into 8-byte binary big-endian
			binary.BigEndian.PutUint64(base[:], baseInt)
			e.Base = base[:]

			if isStart {
				e.SequenceNum = 0
			} else {
				e.SequenceNum = math.MaxUint64
			}
		}
	}

	return e, nil
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
		id.SequenceNum = seqInt
	} else {
		ms := time.Now().Local().UnixMilli()
		ms = max(ms, prevEntry)

		id.Base = binary.BigEndian.AppendUint64(id.Base, uint64(ms)) //stores the 8 bytes encoding of ms into the base ID
	}

	return id, nil
}
