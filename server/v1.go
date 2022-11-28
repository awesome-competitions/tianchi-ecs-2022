package server

import (
	"bytes"
	"dkv/model"
	"dkv/util"
	"github.com/panjf2000/gnet"
	"github.com/valyala/fasthttp"
	"runtime"
	"strconv"
)

var (
	ok                = []byte("HTTP/1.1 200 OK\nContent-Type: text/plain\nContent-Length: 0\n\n")
	notFound          = []byte("HTTP/1.1 404 Not Found\nContent-Type: text/plain\nContent-Length: 0\n\n")
	badRequest        = []byte("HTTP/1.1 400 Bad Request\nContent-Type: text/plain\nContent-Length: 0\n\n")
	requestId  uint32 = 1
)

type httpServer struct {
	*gnet.EventServer

	addr      string
	multicore bool
	fn        func(h *HttpCodec, body []byte)
}

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU() * 2)
}

func New(addr string, multicore bool, fn func(hc *HttpCodec, body []byte)) *httpServer {
	return &httpServer{
		addr:      addr,
		multicore: multicore,
		fn:        fn,
	}
}

type HttpCodec struct {
	parser *HTTPParser
	buf    []byte
	status int

	Entry        *model.Entry
	ZEntry       *model.ZEntry
	ZRange       *model.ZRangeEntry
	ZEntries     []model.ZEntry
	ZRangeBuffer bytes.Buffer
	ListBuffer   bytes.Buffer
	Keys         []string
	Entries      []model.Entry
	BatchEntries []model.Entry

	Fast *fasthttp.Client
}

func NewHttpCodec() *HttpCodec {
	return &HttpCodec{
		parser:   NewHTTPParser(),
		Entry:    &model.Entry{},
		ZEntry:   &model.ZEntry{},
		ZRange:   &model.ZRangeEntry{},
		ZEntries: make([]model.ZEntry, 8),
		Fast:     &fasthttp.Client{},
	}
}

func (hc *HttpCodec) Post(url string, body []byte) (int, []byte, error) {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	req.SetRequestURI(url)
	req.SetBody(body)

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	err := hc.Fast.Do(req, resp)
	if err != nil {
		return 0, nil, err
	}
	return resp.StatusCode(), resp.Body(), nil
}

func (hc *HttpCodec) OK() {
	hc.buf = append(hc.buf, ok...)
}

func (hc *HttpCodec) NotFound() {
	hc.buf = append(hc.buf, notFound...)
}

func (hc *HttpCodec) BadRequest() {
	hc.status = 400
	hc.buf = append(hc.buf, badRequest...)
}

func (hc *HttpCodec) Text(raw []byte) {
	hc.buf = append(hc.buf, "HTTP/1.1 200 OK\nContent-Type: text/plain\nContent-Length: "+strconv.Itoa(len(raw))+"\n\n"...)
	hc.buf = append(hc.buf, raw...)
}

func (hc *HttpCodec) JSON(raw []byte) {
	hc.buf = append(hc.buf, "HTTP/1.1 200 OK\nContent-Type: application/json\nContent-Length: "+strconv.Itoa(len(raw))+"\n\n"...)
	hc.buf = append(hc.buf, raw...)
}

func (hc *HttpCodec) Path() []byte {
	return hc.parser.Path
}

func (hc *HttpCodec) Method() []byte {
	return hc.parser.Method
}

func (hs *httpServer) OnInitComplete(srv gnet.Server) (action gnet.Action) {
	util.Print("HTTP server is listening on %s (multi-cores: %t, event-loops: %d)",
		srv.Addr.String(), srv.Multicore, srv.NumEventLoop)
	return
}

func (hs httpServer) OnOpened(c gnet.Conn) ([]byte, gnet.Action) {
	c.SetContext(&HttpCodec{
		parser:   NewHTTPParser(),
		Entry:    &model.Entry{},
		ZEntry:   &model.ZEntry{},
		ZRange:   &model.ZRangeEntry{},
		ZEntries: make([]model.ZEntry, 8),
		Fast:     &fasthttp.Client{},
	})
	return nil, gnet.None
}

func (hs *httpServer) React(data []byte, c gnet.Conn) (out []byte, action gnet.Action) {
	hc := c.Context().(*HttpCodec)
	hc.parser.Reset()
	offset, err := hc.parser.Parse(data)
	if err != nil {
		return []byte("500 Error"), gnet.Close
	}
	hs.fn(hc, data[offset:])
	out = hc.buf
	hc.buf = hc.buf[:0]
	return
}

func (hs *httpServer) Run() error {
	return gnet.Serve(hs, hs.addr, gnet.WithMulticore(hs.multicore), gnet.WithNumEventLoop(8))
}
