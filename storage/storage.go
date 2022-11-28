package storage

import (
	"dkv/model"
)

type Storage interface {
	Get(key string) (string, bool)
	Add(key, val string)
	Del(key string)
	List(keys []string, entries *[]model.Entry)
	Batch(entries []model.Entry)
	ZAdd(key string, score int, val string)
	ZGet(key string) map[string]int
	ZRmv(key, val string)
	Size() int
}
