package cluster

import (
	"dkv/consts"
	"dkv/model"
	"dkv/server"
	"dkv/util"
	"encoding/json"
	"io/ioutil"
	"os"
)

type Cluster struct {
	Hosts   []string `json:"hosts"`
	Size    int      `json:"size"`
	Index   int      `json:"index"`
	clients []*Client
}

func New() (*Cluster, error) {
	f, err := os.OpenFile(consts.DataDir+"metadata", os.O_CREATE|os.O_RDWR, os.FileMode(0766))
	if err != nil {
		util.Print("cluster.NewCluster err: %v", err)
		return nil, err
	}
	defer f.Close()
	metadata, err := ioutil.ReadAll(f)
	if err != nil {
		util.Print("ioutil.ReadAll err: %v", err)
		return nil, err
	}
	c := Cluster{}
	if len(metadata) > 0 {
		err = json.Unmarshal(metadata, &c)
		if err != nil {
			util.Print("json.Unmarshal err: %v", err)
			return nil, err
		}
	}
	util.Print("new cluster: %v", c)
	return &c, c.initClients()
}

func (cluster *Cluster) Init(hosts []string, index int) error {
	cluster.Hosts = hosts
	cluster.Size = len(hosts)
	cluster.Index = index - 1
	metadata, err := json.Marshal(cluster)
	if err != nil {
		util.Print("json.Marshal err: %v", err)
		return err
	}
	f, err := os.OpenFile(consts.DataDir+"metadata", os.O_CREATE|os.O_RDWR|os.O_TRUNC, os.FileMode(0766))
	if err != nil {
		util.Print("os.OpenFile err: %v", err)
		return err
	}
	defer f.Close()
	_, err = f.Write(metadata)
	if err != nil {
		util.Print("f.WriteAt err: %v", err)
		return err
	}
	err = f.Sync()
	if err != nil {
		util.Print("f.Sync err: %v", err)
		return err
	}
	util.Print("init cluster: %v", cluster)
	return cluster.initClients()
}

func (cluster *Cluster) initClients() error {
	if cluster.Size > 0 {
		cluster.clients = make([]*Client, 0)
		for i, host := range cluster.Hosts {
			if i != cluster.Index {
				client, err := NewClient(host + ":9090")
				if err != nil {
					util.Print("new client %s err: %v", host, err)
					return err
				}
				cluster.clients = append(cluster.clients, client)
			}
		}
	}
	return nil
}

func (cluster *Cluster) Resp(c *server.HttpCodec, statusCode int, data []byte) {
	if statusCode == 404 {
		c.NotFound()
		return
	}
	if statusCode == 400 {
		c.BadRequest()
		return
	}
	if len(data) > 0 {
		c.JSON(data)
		return
	}
	c.OK()
}

func (cluster *Cluster) Add(key, val string) {
	for _, client := range cluster.clients {
		err := client.Add(key, val)
		if err != nil {
			util.Print("cluster Add err: %v", err)
		}
	}
}

func (cluster *Cluster) Del(key string) {
	for _, client := range cluster.clients {
		err := client.Del(key)
		if err != nil {
			util.Print("cluster Del err: %v", err)
		}
	}
}

func (cluster *Cluster) ZAdd(key string, score int, val string) {
	for _, client := range cluster.clients {
		err := client.ZAdd(key, score, val)
		if err != nil {
			util.Print("cluster ZAdd err: %v", err)
		}
	}
}

func (cluster *Cluster) ZRmv(key, val string) {
	for _, client := range cluster.clients {
		err := client.ZRmv(key, val)
		if err != nil {
			util.Print("cluster ZRmv err: %v", err)
		}
	}
}

func (cluster *Cluster) Batch(entities []model.Entry) {
	for _, client := range cluster.clients {
		err := client.Batch(entities)
		if err != nil {
			util.Print("cluster Batch err: %v", err)
		}
	}
}

func (cluster *Cluster) Query(key string) (string, bool) {
	for _, client := range cluster.clients {
		v, ok, err := client.Query(key)
		if err != nil {
			util.Print("cluster Batch err: %v", err)
		}
		if ok {
			return v, true
		}
	}
	return "", false
}
