package storage

import (
	"dkv/model"
	"sync"
)

type LockedStorage struct {
	sync.RWMutex
	m map[string]string
	s map[string]map[string]int
}

func Locked() Storage {
	return &LockedStorage{
		m: make(map[string]string, 16777216),
		s: make(map[string]map[string]int, 16777216),
	}
}

func (h *LockedStorage) Get(key string) (string, bool) {
	h.RLock()
	defer h.RUnlock()
	v, ok := h.m[key]
	return v, ok
}

func (h *LockedStorage) Add(key, val string) {
	h.Lock()
	defer h.Unlock()
	h.m[key] = val
}

func (h *LockedStorage) Del(key string) {
	h.Lock()
	defer h.Unlock()
	delete(h.m, key)
}

func (h *LockedStorage) List(keys []string, entries *[]model.Entry) {
	h.RLock()
	defer h.RUnlock()
	for _, key := range keys {
		v, _ := h.m[key]
		*entries = append(*entries, model.Entry{
			Key:   key,
			Value: v,
		})
	}
}

func (h *LockedStorage) Batch(entries []model.Entry) {
	h.Lock()
	defer h.Unlock()
	for _, entry := range entries {
		h.m[entry.Key] = entry.Value
	}
}

func (h *LockedStorage) ZAdd(key string, score int, val string) {
	h.Lock()
	defer h.Unlock()
	s := h.s[key]
	if s == nil {
		s = map[string]int{}
		h.s[key] = s
	}
	s[val] = score
}

func (h *LockedStorage) ZGet(key string) map[string]int {
	h.RLock()
	defer h.RUnlock()
	s := h.s[key]
	if s == nil {
		return nil
	}
	return s
}

func (h *LockedStorage) ZRmv(key, val string) {
	h.Lock()
	defer h.Unlock()
	s := h.s[key]
	if s == nil {
		return
	}
	delete(s, val)
}

func (h *LockedStorage) Size() int {
	h.Lock()
	defer h.Unlock()
	return len(h.s) + len(h.m)
}
