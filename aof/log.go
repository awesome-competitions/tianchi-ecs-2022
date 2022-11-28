package aof

import (
	"bytes"
	"dkv/consts"
	"dkv/util"
	"io"
	"io/ioutil"
	"os"
	"sync"
)

type Logger struct {
	sync.Mutex

	f  *os.File
	wb *bytes.Buffer
	rb *bytes.Buffer
}

func New() (*Logger, error) {
	f, err := os.OpenFile(consts.DataDir+"aof", os.O_CREATE|os.O_RDWR|os.O_APPEND, os.FileMode(0766))
	if err != nil {
		return nil, err
	}
	return &Logger{
		f:  f,
		wb: &bytes.Buffer{},
		rb: &bytes.Buffer{},
	}, nil
}

func (l *Logger) Write(op consts.OP, args ...interface{}) {
	l.Lock()
	defer l.Unlock()

	l.wb.Reset()
	l.wb.WriteByte(byte(op))
	for _, arg := range args {
		if str, ok := arg.(string); ok {
			sl := len(str)
			l.wb.WriteByte(byte(sl >> 24))
			l.wb.WriteByte(byte(sl >> 16))
			l.wb.WriteByte(byte(sl >> 8))
			l.wb.WriteByte(byte(sl))
			l.wb.Write([]byte(str))
		} else if n, ok := arg.(int); ok {
			l.wb.WriteByte(byte(n >> 24))
			l.wb.WriteByte(byte(n >> 16))
			l.wb.WriteByte(byte(n >> 8))
			l.wb.WriteByte(byte(n))
		}
	}

	_, err := l.f.Write(l.wb.Bytes())
	if err != nil {
		util.Print("aof write err:%v", err)
		panic(err)
	}
	err = l.f.Sync()
	if err != nil {
		util.Print("aof sync err:%v", err)
		panic(err)
	}
}

func (l *Logger) Recover(f func(op consts.OP, args ...interface{})) error {
	_, err := l.f.Seek(0, io.SeekStart)
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return err
	}
	data, err := ioutil.ReadAll(l.f)
	if err != nil {
		panic(err)
	}
	rb := l.rb
	rb.Reset()
	rb.Write(data)
	for {
		op, err := rb.ReadByte()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		switch consts.OP(op) {
		case consts.Add:
			key, err := util.NextString(rb)
			if err != nil {
				return err
			}
			val, err := util.NextString(rb)
			if err != nil {
				return err
			}
			f(consts.OP(op), key, val)
		case consts.Del:
			key, err := util.NextString(rb)
			if err != nil {
				return err
			}
			f(consts.OP(op), key)
		case consts.ZAdd:
			key, err := util.NextString(rb)
			if err != nil {
				return err
			}
			score, err := util.NextInt(rb)
			if err != nil {
				return err
			}
			val, err := util.NextString(rb)
			if err != nil {
				return err
			}
			f(consts.OP(op), key, score, val)
		case consts.ZRmv:
			key, err := util.NextString(rb)
			if err != nil {
				return err
			}
			val, err := util.NextString(rb)
			if err != nil {
				return err
			}
			f(consts.OP(op), key, val)
		}
	}
}
