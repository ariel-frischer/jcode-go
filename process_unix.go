//go:build !windows

package jcode

import (
	"os"
	"os/exec"
	"syscall"
)

func startProcess(cmd *exec.Cmd) error { return cmd.Start() }
func terminateProcess(cmd *exec.Cmd, waitDone <-chan error) {
	if cmd.Process == nil {
		return
	}
	_ = cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-waitDone:
	default:
		_ = cmd.Process.Kill()
		<-waitDone
	}
}
func stopProcess(pid int) {
	if process, err := os.FindProcess(pid); err == nil {
		_ = process.Signal(syscall.SIGTERM)
	}
}
