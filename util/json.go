package util

import (
	"github.com/json-iterator/go"
)

var fastjson = jsoniter.ConfigFastest

func Marshal(v interface{}) ([]byte, error) {
	return fastjson.Marshal(v)
}

func Unmarshal(data []byte, v interface{}) error {
	return fastjson.Unmarshal(data, v)
}

func ParseStrings(data []byte, arr *[]string) {
	state := 0
	left, right := 0, 0
	*arr = (*arr)[:0]
	for i, b := range data {
		switch state {
		case 0:
			if b == '[' {
				state = 1
			} else if b == ']' {
				*arr = append(*arr, string(data[left+1:right]))
				break
			} else if b == ',' {
				*arr = append(*arr, string(data[left+1:right]))
				state = 1
			}
		case 1:
			if b == '"' {
				state = 2
				left = i
			}
		case 2:
			if b == '"' {
				state = 0
				right = i
			}
		}
	}
}
