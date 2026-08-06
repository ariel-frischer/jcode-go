package jcode

import (
	"fmt"
	"os/exec"
)

func startProcess(cmd *exec.Cmd) error { return cmd.Start() }

func terminateProcess(cmd *exec.Cmd, waitDone <-chan error) {
	if cmd.Process == nil {
		return
	}
	_ = cmd.Process.Kill()
	<-waitDone
}

func stopProcess(pid int) {
	_ = exec.Command("taskkill", "/PID", fmt.Sprint(pid), "/T", "/F").Run()
}
