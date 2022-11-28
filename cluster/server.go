package cluster

import (
	"bytes"
	"dkv/storage"
	"dkv/util"
	"net"
)

type Server struct {
	addr string
	s    storage.Storage
}

func NewServer(addr string, s storage.Storage) *Server {
	return &Server{addr: addr, s: s}
}

func (s *Server) Serve() error {
	lis, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	util.Print("cluster server start at: %s", s.addr)
	for {
		conn, err := lis.Accept()
		if err != nil {
			return err
		}
		go func() {
			err := s.handle(conn)
			if err != nil {
				util.Print("handle err: %v", err)
			}
		}()
	}
}

func (s *Server) handle(conn net.Conn) error {
	r := NewBufReader(conn)
	wb := bytes.Buffer{}
	for {
		op := r.Byte()
		switch op {
		case 1: //add
			k := r.String(int(r.Byte()))
			v := r.String(int(r.Byte()))
			s.s.Add(k, v)
		case 2: //zadd
			k := r.String(int(r.Byte()))
			score := r.Int()
			v := r.String(int(r.Byte()))
			s.s.ZAdd(k, score, v)
		case 3: //del
			k := r.String(int(r.Byte()))
			s.s.Del(k)
		case 4:
			k := r.String(int(r.Byte()))
			v := r.String(int(r.Byte()))
			s.s.ZRmv(k, v)
		case 5: //batch
			l := r.Int()
			for i := 0; i < l; i++ {
				k := r.String(int(r.Byte()))
				v := r.String(int(r.Byte()))
				s.s.Add(k, v)
			}
		case 6: //query
			k := r.String(int(r.Byte()))
			v, ok := s.s.Get(k)
			wb.Reset()
			if ok {
				wb.WriteByte(1)
				wb.WriteByte(byte(len(v)))
				wb.WriteString(v)
			} else {
				wb.WriteByte(0)
			}
			_, err := conn.Write(wb.Bytes())
			if err != nil {
				util.Print("server write err: %v", err)
				break
			}
		}
	}
}
