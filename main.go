package main

import (
	"dkv/cluster"
	"dkv/handler"
	"dkv/server"
	"dkv/storage"
	"dkv/util"
)

func main() {
	s := storage.Locked()
	clusterServer := cluster.NewServer(":9090", s)
	go func() {
		if err := clusterServer.Serve(); err != nil {
			util.Print("cluster serve err: %v", err)
		}
	}()

	h := handler.New(s)
	util.Print("server start at: 8080")
	httpServer := server.New("tcp://0.0.0.0:8080", true,
		func(hc *server.HttpCodec, body []byte) {
			p := hc.Path()
			switch p[1] {
			case 'i':
				h.Init(hc, body)
			case 'u':
				h.UpdateCluster(hc, body)
			case 'q':
				h.Query(hc, body)
			case 'a':
				h.Add(hc, body)
			case 'l':
				h.List(hc, body)
			case 'b':
				h.Batch(hc, body)
			case 'd':
				h.Del(hc, body)
			case 'z':
				switch p[3] {
				case 'd':
					h.ZAdd(hc, body)
				case 'a':
					h.ZRange(hc, body)
				case 'm':
					h.ZRmv(hc, body)
				}
			}
		})
	util.Print("server exists: %v", httpServer.Run())
}
