package util

import (
	"fmt"
	"testing"
)

func TestHash(t *testing.T) {
	fmt.Println(Hash("qps-66df22fb-d333-4c26-8764-0ce29f1c60e2") % 3)
}
