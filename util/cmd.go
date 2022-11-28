package util

import (
	"os/exec"
)

func Execute(name string, arg ...string) string {
	cmd := exec.Command(name, arg...)
	msg, err := cmd.Output()
	if err != nil {
		return err.Error()
	}
	return string(msg)
}
