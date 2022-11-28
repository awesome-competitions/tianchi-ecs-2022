package util

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strings"
)

func WriteText(w http.ResponseWriter, text string) {
	w.Header().Set("content-type", "text/plain")
	_, _ = w.Write([]byte(text))
	w.WriteHeader(200)
}

func WriteJson(w http.ResponseWriter, v interface{}) {
	w.Header().Set("content-type", "application/json")
	bytes, _ := json.Marshal(v)
	_, _ = w.Write(bytes)
	w.WriteHeader(200)
}

func ReadObject(r *http.Request, v interface{}) {
	b, _ := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	_ = json.Unmarshal(b, v)
}

func ParseKey(path string) string {
	return path[strings.LastIndexByte(path, '/')+1:]
}

func ParseKeyValue(path string) (string, string) {
	i := strings.LastIndexByte(path, '/')
	v := path[i+1:]
	return ParseKey(path[:i]), v
}
