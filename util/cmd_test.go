package util

import (
	"fmt"
	"testing"
)

func TestExecute(t *testing.T) {
	fmt.Println(Execute("go", "env"))
}
