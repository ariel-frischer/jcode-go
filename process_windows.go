package jcode

import (
	"errors"
	"fmt"
	"os/exec"
	"time"
)

func startProcess(cmd *exec.Cmd) (int, error) {
	if err := cmd.Start(); err != nil {
		return 0, err
	}
	return 0, nil
}

// Windows lifecycle parity is outside the Linux supervision program. These
// parameters only preserve the shared internal launcher signature.
func terminateProcess(
	cmd *exec.Cmd,
	_ int,
	waitDone <-chan error,
	_ time.Duration,
	_ time.Duration,
	_ <-chan struct{},
) (bool, error) {
	if cmd == nil || cmd.Process == nil {
		return false, nil
	}
	_ = cmd.Process.Kill()
	<-waitDone
	return false, nil
}

func stopProcess(pid int) error {
	if pid <= 1 {
		return errors.New("refusing to signal an invalid or unrecorded process")
	}
	return exec.Command("taskkill", "/PID", fmt.Sprint(pid), "/T", "/F").Run()
}
