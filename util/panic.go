package util

import (
	"fmt"
	"runtime/debug"
)

func Recover(tag string) {
	if err := recover(); err != nil {
		Print(fmt.Sprintf("%s err: %v, panic: %s", tag, err, string(debug.Stack())))
	}
}
