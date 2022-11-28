package cluster

import (
	"bufio"
	"encoding/binary"
	"io"
)

type BufReader struct {
	*bufio.Reader
	uint32Bytes []byte
}

func NewBufReader(reader io.Reader) *BufReader {
	return &BufReader{
		Reader:      bufio.NewReaderSize(reader, 65535),
		uint32Bytes: make([]byte, 4),
	}
}

func (r *BufReader) Byte() byte {
	b, err := r.ReadByte()
	if err != nil {
		panic(err)
	}
	return b
}

func (r *BufReader) Int() int {
	_, err := io.ReadFull(r.Reader, r.uint32Bytes)
	if err != nil {
		panic(err)
	}
	return int(binary.BigEndian.Uint32(r.uint32Bytes))
}

func (r *BufReader) String(len int) string {
	bs := make([]byte, len)
	_, err := io.ReadFull(r.Reader, bs)
	if err != nil {
		panic(err)
	}
	return string(bs)
}
