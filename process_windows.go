package jcode

import (
	"context"
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
	reapTimeout time.Duration,
	_ <-chan struct{},
) (bool, error) {
	return terminateProcessObserved(cmd, 0, waitDone, 0, reapTimeout, nil, nil)
}

func terminateProcessObserved(
	cmd *exec.Cmd,
	_ int,
	waitDone <-chan error,
	_ time.Duration,
	reapTimeout time.Duration,
	_ <-chan struct{},
	_ func(string),
) (bool, error) {
	if cmd == nil || cmd.Process == nil {
		return false, nil
	}
	var errs []error
	if err := cmd.Process.Kill(); err != nil {
		errs = append(errs, fmt.Errorf("kill process: %w", err))
	}
	reapTimeout = finiteDuration(reapTimeout, defaultShutdownReapTimeout)
	reapTimer := time.NewTimer(reapTimeout)
	select {
	case err := <-waitDone:
		if err != nil {
			errs = append(errs, fmt.Errorf("wait for killed process: %w", err))
		}
	case <-reapTimer.C:
		errs = append(errs, fmt.Errorf("wait for killed process: timeout after %s", reapTimeout))
	}
	if !reapTimer.Stop() {
		select {
		case <-reapTimer.C:
		default:
		}
	}
	return false, errors.Join(errs...)
}

func stopProcess(pid int) error {
	return stopProcessWithTaskkill(pid, defaultShutdownReapTimeout, func(ctx context.Context, pid int) error {
		return exec.CommandContext(ctx, "taskkill", "/PID", fmt.Sprint(pid), "/T", "/F").Run()
	})
}

func stopProcessWithTaskkill(pid int, timeout time.Duration, taskkill func(context.Context, int) error) error {
	if pid <= 1 {
		return errors.New("refusing to signal an invalid or unrecorded process")
	}
	ctx, cancel := context.WithTimeout(context.Background(), finiteDuration(timeout, defaultShutdownReapTimeout))
	defer cancel()
	err := taskkill(ctx, pid)
	if err == nil {
		return nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) && exitErr.ExitCode() == 128 {
		return nil
	}
	if cause := context.Cause(ctx); cause != nil {
		return fmt.Errorf("taskkill process %d: %w", pid, errors.Join(err, cause))
	}
	return fmt.Errorf("taskkill process %d: %w", pid, err)
}
