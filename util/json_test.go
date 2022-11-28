package util

import (
	"dkv/model"
	"encoding/json"
	"fmt"
	uuid "github.com/satori/go.uuid"
	"testing"
)

var (
	objects = make([]model.Entry, 0)
	byteArr = make([][]byte, 0)
	batch   = 100000
)

func init() {
	for i := 0; i < batch; i++ {
		entry := model.Entry{
			Key:   uuid.NewV4().String(),
			Value: uuid.NewV4().String(),
		}
		objects = append(objects, entry)
		b, _ := json.Marshal(entry)
		byteArr = append(byteArr, b)
	}
}

func BenchmarkMarshal(b *testing.B) {
	for i := 0; i < batch; i++ {
		_, _ = Marshal(objects[i])
	}
}

func BenchmarkUnmarshal(b *testing.B) {
	for i := 0; i < batch; i++ {
		o := model.Entry{}
		_ = Unmarshal(byteArr[i], &o)
	}
}

func BenchmarkStdMarshal(b *testing.B) {
	for i := 0; i < batch; i++ {
		_, _ = json.Marshal(objects[i])
	}
}

func BenchmarkStdUnmarshal(b *testing.B) {
	for i := 0; i < batch; i++ {
		o := model.Entry{}
		_ = json.Unmarshal(byteArr[i], &o)
	}
}

func TestParseStrings(t *testing.T) {
	str := "[\n    \"a\",\n    \"a1\",\n    \"b\",\n    \"a12\"\n]"
	arr := make([]string, 0)
	ParseStrings([]byte(str), &arr)
	fmt.Println(arr)
	str = "[\n    \"a\",\n    \"a1\",\n    \"b\",\n    \"a12\",\n    \"a15\"]"
	ParseStrings([]byte(str), &arr)
	fmt.Println(arr)
	str = "[\n    \"a\",\n    \"a1\",\n    \"b\"]"
	ParseStrings([]byte(str), &arr)
	fmt.Println(arr)

}
