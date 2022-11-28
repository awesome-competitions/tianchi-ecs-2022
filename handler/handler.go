package handler

import (
	"dkv/aof"
	"dkv/cluster"
	"dkv/consts"
	"dkv/model"
	"dkv/server"
	"dkv/storage"
	"dkv/util"
	"github.com/syndtr/goleveldb/leveldb"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"time"
	"unsafe"
)

type Handler struct {
	s                                                storage.Storage
	q, a, li, d, b, za, zr, zd, in, s400, s200, s404 int
	initialized                                      bool
	once                                             sync.Once
	c                                                *cluster.Cluster
	l                                                *aof.Logger
}

func New(s storage.Storage) *Handler {
	c, err := cluster.New()
	if err != nil {
		panic(err)
	}
	log, err := aof.New()
	if err != nil {
		panic(err)
	}
	h := &Handler{
		s:    s,
		c:    c,
		once: sync.Once{},
		l:    log,
	}
	if c.Size > 0 {
		h.once.Do(h.recover)
	}
	return h
}

func (h *Handler) recover() {
	go func() {
		for i := 1; i <= 3; i++ {
			h.recoverWithIndex(i)
		}
		//h.recoverLog()
		h.initialized = true
	}()
}

func (h *Handler) recoverLog() {
	err := h.l.Recover(func(op consts.OP, args ...interface{}) {
		switch op {
		case consts.Add:
			h.s.Add(args[0].(string), args[1].(string))
			h.c.Add(args[0].(string), args[1].(string))
		case consts.Del:
			h.s.Del(args[0].(string))
			h.c.Del(args[0].(string))
		case consts.ZAdd:
			h.s.ZAdd(args[0].(string), args[1].(int), args[2].(string))
			h.c.ZAdd(args[0].(string), args[1].(int), args[2].(string))
		case consts.ZRmv:
			h.s.ZRmv(args[0].(string), args[1].(string))
			h.c.ZRmv(args[0].(string), args[1].(string))
		}
	})
	if err != nil {
		panic(err)
	}
}

func (h *Handler) recoverWithIndex(i int) {
	util.Print("============== start load %d leveldb", i)
	start := time.Now().UnixNano()
	db, err := leveldb.OpenFile(consts.DataDir+"data"+strconv.Itoa(i), nil)
	if err != nil {
		db, err = leveldb.RecoverFile(consts.DataDir+"data"+strconv.Itoa(i), nil)
		if err != nil {
			util.Print("leveldb err: %s", err.Error())
			return
		}
		return
	}
	util.Print("============== load leveldb %d completed", i)
	defer db.Close()
	iter := db.NewIterator(nil, nil)
	size := 0
	for iter.Next() {
		if util.Hash(string(iter.Key()))%3 == h.c.Index {
			h.a++
			h.s.Add(string(iter.Key()), string(iter.Value()))
			size++
		}
	}
	util.Print("============== recover %d success!! load size: %d, cost %d", i, size, (time.Now().UnixNano()-start)/1e6)
}

func (h *Handler) Init(c *server.HttpCodec, body []byte) {
	defer h.once.Do(h.recover)
	h.in++
	if !h.initialized {
		c.BadRequest()
		h.s400++
		return
	}
	h.s200++
	c.Text([]byte("ok"))
}

func (h *Handler) UpdateCluster(c *server.HttpCodec, body []byte) {
	defer h.once.Do(h.recover)
	clusterInfo := model.UpdateCluster{}
	util.Print("UpdateCluster: %s", string(body))
	err := util.Unmarshal(body, &clusterInfo)
	if err != nil {
		h.s400++
		c.BadRequest()
		return
	}
	err = h.c.Init(clusterInfo.Hosts, clusterInfo.Index)
	if err != nil {
		h.s400++
		c.BadRequest()
		return
	}
	c.Text([]byte("ok"))
}

func (h *Handler) Query(c *server.HttpCodec, body []byte) {
	h.q++
	key := util.ParseKey(string(c.Path()))
	if val, ok := h.s.Get(key); ok {
		c.Text([]byte(val))
		return
	}
	if val, ok := h.c.Query(key); ok {
		c.Text([]byte(val))
		return
	}
	h.s404++
	c.NotFound()
}

func (h *Handler) Add(c *server.HttpCodec, body []byte) {
	err := c.Entry.UnmarshalJSON(body)
	if err != nil {
		h.s400++
		c.BadRequest()
		return
	}
	h.a++
	h.s.Add(c.Entry.Key, c.Entry.Value)
	h.c.Add(c.Entry.Key, c.Entry.Value)
	//h.l.Write(consts.Add, c.Entry.Key, c.Entry.Value)
	h.s200++
	c.OK()
}

func (h *Handler) Del(c *server.HttpCodec, body []byte) {
	h.d++
	key := util.ParseKey(string(c.Path()))
	h.s.Del(key)
	h.c.Del(key)
	//h.l.Write(consts.Del, key)
	h.s200++
	c.OK()
}

func (h *Handler) List(c *server.HttpCodec, body []byte) {
	h.li++
	util.ParseStrings(body, &c.Keys)
	c.Entries = c.Entries[:0]
	h.s.List(c.Keys, &c.Entries)
	c.ListBuffer.Reset()
	c.ListBuffer.WriteByte('[')
	for _, entry := range c.Entries {
		c.ListBuffer.Write(consts.KeyBytes)
		c.ListBuffer.WriteString(entry.Key)
		c.ListBuffer.Write(consts.ValueBytes)
		c.ListBuffer.WriteString(entry.Value)
		c.ListBuffer.Write(consts.EndBytes)
	}
	c.ListBuffer.Truncate(c.ListBuffer.Len() - 1)
	c.ListBuffer.WriteByte(']')
	h.s200++
	c.JSON(c.ListBuffer.Bytes())
}

func (h *Handler) Batch(c *server.HttpCodec, body []byte) {
	h.b++
	c.BatchEntries = c.BatchEntries[:0]
	err := util.Unmarshal(body, &c.BatchEntries)
	if err != nil {
		h.s400++
		c.BadRequest()
		return
	}
	h.s200++
	h.s.Batch(c.BatchEntries)
	h.c.Batch(c.BatchEntries)
	//for _, entry := range c.BatchEntries {
	//	h.l.Write(consts.Add, entry.Key, entry.Value)
	//}
	c.OK()
}

func (h *Handler) ZAdd(c *server.HttpCodec, body []byte) {
	h.za++
	key := util.ParseKey(string(c.Path()))
	err := c.ZEntry.UnmarshalJSON(body)
	if err != nil {
		h.s400++
		c.BadRequest()
		return
	}
	h.s200++
	h.s.ZAdd(key, c.ZEntry.Score, c.ZEntry.Value)
	h.c.ZAdd(key, c.ZEntry.Score, c.ZEntry.Value)
	//h.l.Write(consts.ZAdd, key, c.ZEntry.Score, c.ZEntry.Value)
	c.OK()
}

func (h *Handler) ZRange(c *server.HttpCodec, body []byte) {
	h.zr++
	key := util.ParseKey(string(c.Path()))
	val := h.s.ZGet(key)
	if val == nil || len(val) == 0 {
		c.JSON([]byte("[]"))
		h.s404++
		return
	}
	err := c.ZRange.UnmarshalJSON(body)
	if err != nil {
		h.s400++
		c.BadRequest()
		return
	}
	c.ZEntries = c.ZEntries[:0]
	for val, score := range val {
		if score >= c.ZRange.MinScore && score <= c.ZRange.MaxScore {
			c.ZEntries = append(c.ZEntries, model.ZEntry{
				Score: score,
				Value: val,
			})
		}
	}
	if len(c.ZEntries) == 0 {
		c.JSON([]byte("[]"))
		h.s404++
		return
	}
	if len(c.ZEntries) > 1 {
		sort.Slice(c.ZEntries, func(i, j int) bool {
			return c.ZEntries[i].Score < c.ZEntries[j].Score
		})
	}
	c.ZRangeBuffer.Reset()
	c.ZRangeBuffer.WriteByte('[')
	for _, entry := range c.ZEntries {
		c.ZRangeBuffer.Write(consts.ScoreBytes)
		c.ZRangeBuffer.WriteString(strconv.Itoa(entry.Score))
		c.ZRangeBuffer.Write(consts.ScoreValueBytes)
		c.ZRangeBuffer.WriteString(entry.Value)
		c.ZRangeBuffer.Write(consts.EndBytes)
	}
	c.ZRangeBuffer.Truncate(c.ZRangeBuffer.Len() - 1)
	c.ZRangeBuffer.WriteByte(']')
	h.s200++
	c.JSON(c.ZRangeBuffer.Bytes())
}

func (h *Handler) ZRmv(c *server.HttpCodec, body []byte) {
	h.zd++
	key, val := util.ParseKeyValue(string(c.Path()))
	h.s.ZRmv(key, val)
	h.c.ZRmv(key, val)
	//h.l.Write(consts.ZRmv, key, val)
	h.s200++
	c.OK()
}

func b2s(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func s2b(s string) (b []byte) {
	strh := (*reflect.StringHeader)(unsafe.Pointer(&s))
	sh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	sh.Data = strh.Data
	sh.Len = strh.Len
	sh.Cap = strh.Len
	return b
}
