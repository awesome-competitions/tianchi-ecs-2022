package cluster

import (
	"bufio"
	"bytes"
	"dkv/model"
	"dkv/util"
	"net"
	"sync"
	"time"
)

type Client struct {
	sync.Mutex
	addr   string
	c      net.Conn
	w      *bufio.Writer
	r      *BufReader
	buffer *bytes.Buffer
}

func NewClient(addr string) (*Client, error) {
	conn, err := reconnect(addr, 200)
	if err != nil {
		return nil, err
	}
	return &Client{
		addr:   addr,
		c:      conn,
		w:      bufio.NewWriterSize(conn, 65535),
		r:      NewBufReader(conn),
		buffer: &bytes.Buffer{},
	}, nil
}

func connect(addr string) (net.Conn, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return nil, err
	}
	return net.DialTCP("tcp", nil, tcpAddr)
}

func reconnect(addr string, times int) (conn net.Conn, err error) {
	for i := 0; i < times; i++ {
		conn, err = connect(addr)
		if err == nil {
			return conn, nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return nil, err
}

func (c *Client) Write(bytes []byte) error {
	_, err := c.w.Write(bytes)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) WriteAndFlush(bytes []byte) error {
	err := c.Write(bytes)
	if err != nil {
		return err
	}
	return c.Flush()
}

func (c *Client) Flush() error {
	return c.w.Flush()
}

func (c *Client) AutoFlush(d time.Duration) {
	go func() {
		for {
			time.Sleep(d)
			err := c.Flush()
			if err != nil {
				util.Print("client flush err: %v", err)
			}
		}
	}()
}

func (c *Client) Add(key, val string) error {
	c.Lock()
	defer c.Unlock()
	buff := c.buffer
	buff.Reset()
	buff.WriteByte(1)
	buff.WriteByte(byte(len(key)))
	buff.WriteString(key)
	buff.WriteByte(byte(len(val)))
	buff.WriteString(val)
	return c.WriteAndFlush(buff.Bytes())
}

func (c *Client) Del(key string) error {
	c.Lock()
	defer c.Unlock()
	buff := c.buffer
	buff.Reset()
	buff.WriteByte(3)
	buff.WriteByte(byte(len(key)))
	buff.WriteString(key)
	return c.WriteAndFlush(buff.Bytes())
}

func (c *Client) ZAdd(key string, score int, val string) error {
	c.Lock()
	defer c.Unlock()
	buff := c.buffer
	buff.Reset()
	buff.WriteByte(2)
	buff.WriteByte(byte(len(key)))
	buff.WriteString(key)
	buff.WriteByte(byte(score >> 24))
	buff.WriteByte(byte(score >> 16))
	buff.WriteByte(byte(score >> 8))
	buff.WriteByte(byte(score))
	buff.WriteByte(byte(len(val)))
	buff.WriteString(val)
	return c.WriteAndFlush(buff.Bytes())
}

func (c *Client) ZRmv(key, val string) error {
	c.Lock()
	defer c.Unlock()
	buff := c.buffer
	buff.Reset()
	buff.WriteByte(4)
	buff.WriteByte(byte(len(key)))
	buff.WriteString(key)
	buff.WriteByte(byte(len(val)))
	buff.WriteString(val)
	return c.WriteAndFlush(buff.Bytes())
}

func (c *Client) Batch(entities []model.Entry) error {
	c.Lock()
	defer c.Unlock()
	l := len(entities)
	buff := c.buffer
	buff.Reset()
	buff.WriteByte(5)
	buff.WriteByte(byte(l >> 24))
	buff.WriteByte(byte(l >> 16))
	buff.WriteByte(byte(l >> 8))
	buff.WriteByte(byte(l))
	for _, e := range entities {
		key, val := e.Key, e.Value
		buff.WriteByte(byte(len(key)))
		buff.WriteString(key)
		buff.WriteByte(byte(len(val)))
		buff.WriteString(val)
	}
	return c.WriteAndFlush(buff.Bytes())
}

func (c *Client) Query(key string) (string, bool, error) {
	c.Lock()
	defer c.Unlock()
	buff := c.buffer
	buff.Reset()
	buff.WriteByte(6)
	buff.WriteByte(byte(len(key)))
	buff.WriteString(key)
	err := c.WriteAndFlush(buff.Bytes())
	if err != nil {
		return "", false, err
	}
	if c.r.Byte() == 0 {
		return "", false, nil
	}
	val := c.r.String(int(c.r.Byte()))
	return val, true, nil
}
