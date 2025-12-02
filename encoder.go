package main

import (
	"strconv"
)

type Encoder struct {
	EncodingMap map[string][]byte
}

func (e *Encoder) InitalizeEncodingMap() {
	e.EncodingMap = make(map[string][]byte)
	e.EncodingMap["OK"] = e.GenerateSimpleString([]byte("OK"))
	e.EncodingMap["nil"] = e.GenerateNilBulkString()

}

func (e *Encoder) GetSimpleStringOk() []byte {
	return e.EncodingMap["OK"]
}

func (e *Encoder) GetNilBulkString() []byte {
	return e.EncodingMap["nil"]
}

func (e *Encoder) GenerateBulkString(bytes []byte) []byte {
	out := make([]byte, 0, len(bytes)+32)
	out = append(out, '$')
	out = strconv.AppendInt(out, int64(len(bytes)), 10)
	out = append(out, '\r', '\n')
	out = append(out, bytes...)
	out = append(out, '\r', '\n')
	return out
}

func (e *Encoder) GenerateInt(i int) []byte {
	out := make([]byte, 0, 32)
	out = append(out, ':')
	out = strconv.AppendInt(out, int64(i), 10)
	out = append(out, '\r', '\n')
	return out
}

func (e *Encoder) GenerateArray(array [][]byte) []byte {
	out := make([]byte, 0, len(array)+32)
	out = append(out, '*')
	out = strconv.AppendInt(out, int64(len(array)), 10)
	out = append(out, '\r', '\n')
	for _, v := range array {
		out = append(out, e.GenerateBulkString(v)...)
	}

	return out

}

func (e *Encoder) GenerateNilBulkString() []byte {
	out := make([]byte, 0)
	out = append(out, '$')
	out = strconv.AppendInt(out, -1, 10)
	out = append(out, '\r', '\n')
	return out
}
func (e *Encoder) GenerateSimpleString(bytes []byte) []byte {
	out := make([]byte, 0, len(bytes)+32)
	out = append(out, '+')
	out = append(out, bytes...)
	out = append(out, '\r', '\n')
	return out
}

func (e *Encoder) GenerateSimpleError(err string) []byte {
	bytes := []byte(err)
	out := make([]byte, 0, len(bytes)+32)
	out = append(out, '-')
	out = append(out, bytes...)
	out = append(out, '\r', '\n')
	return out
}
