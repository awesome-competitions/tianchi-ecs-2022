package util

import (
	"bytes"
	"encoding/binary"
	"hash/fnv"
)

func NextString(buf *bytes.Buffer) (string, error) {
	strLenBytes := make([]byte, 4)
	_, err := buf.Read(strLenBytes)
	if err != nil {
		return "", err
	}
	strLen := binary.BigEndian.Uint32(strLenBytes)
	strBytes := make([]byte, strLen)
	_, err = buf.Read(strBytes)
	if err != nil {
		return "", err
	}
	return string(strBytes), nil
}

func NextInt(buf *bytes.Buffer) (int, error) {
	nBytes := make([]byte, 4)
	_, err := buf.Read(nBytes)
	if err != nil {
		return 0, err
	}
	n := binary.BigEndian.Uint32(nBytes)
	return int(n), nil
}

func Hash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32())
}

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
