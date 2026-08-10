//go:build !windows

package jcode

import (
	"errors"
	"fmt"
	"os/exec"
	"syscall"
	"time"
)

func startProcess(cmd *exec.Cmd) (int, error) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return 0, err
	}
	processGroupID := cmd.Process.Pid
	if processGroupID <= 1 {
		_ = cmd.Process.Kill()
		return 0, fmt.Errorf("invalid owned process group %d", processGroupID)
	}
	return processGroupID, nil
}

func terminateProcess(
	_ *exec.Cmd,
	processGroupID int,
	waitDone <-chan error,
	grace time.Duration,
	reapTimeout time.Duration,
	forceKill <-chan struct{},
) (bool, error) {
	return terminateProcessGroup(processGroupID, waitDone, grace, reapTimeout, forceKill, processGroupOperations{
		signal: signalProcessGroup,
		alive:  processGroupAlive,
	})
}

type processGroupOperations struct {
	signal func(int, syscall.Signal) error
	alive  func(int) (bool, error)
}

func terminateProcessGroup(
	processGroupID int,
	waitDone <-chan error,
	grace time.Duration,
	reapTimeout time.Duration,
	forceKill <-chan struct{},
	operations processGroupOperations,
) (bool, error) {
	if processGroupID <= 1 {
		return false, fmt.Errorf("invalid owned process group %d", processGroupID)
	}
	grace = finiteDuration(grace, defaultShutdownGracePeriod)
	reapTimeout = finiteDuration(reapTimeout, defaultShutdownReapTimeout)
	select {
	case <-waitDone:
		return false, nil
	default:
	}

	var errs []error
	if err := operations.signal(processGroupID, syscall.SIGTERM); err != nil {
		errs = append(errs, fmt.Errorf("signal owned process group %d with SIGTERM: %w", processGroupID, err))
	}

	waitReceived := false
	contextTriggered := false
	graceTimer := time.NewTimer(grace)
	select {
	case <-waitDone:
		waitReceived = true
	case <-graceTimer.C:
	case <-forceKill:
		contextTriggered = true
	}
	if !graceTimer.Stop() {
		select {
		case <-graceTimer.C:
		default:
		}
	}
	if waitReceived {
		return contextTriggered, errors.Join(errs...)
	}
	select {
	case <-waitDone:
		return contextTriggered, errors.Join(errs...)
	default:
	}

	alive, aliveErr := operations.alive(processGroupID)
	if aliveErr != nil {
		errs = append(errs, fmt.Errorf("check owned process group %d after SIGTERM: %w", processGroupID, aliveErr))
	}
	if !alive {
		if err := waitForProcessReap(processGroupID, waitDone, reapTimeout); err != nil {
			errs = append(errs, err)
		}
		return contextTriggered, errors.Join(errs...)
	}

	select {
	case <-waitDone:
		return contextTriggered, errors.Join(errs...)
	default:
	}
	if err := operations.signal(processGroupID, syscall.SIGKILL); err != nil {
		errs = append(errs, fmt.Errorf("signal owned process group %d with SIGKILL: %w", processGroupID, err))
	}

	if err := waitForProcessReap(processGroupID, waitDone, reapTimeout); err != nil {
		errs = append(errs, err)
	}
	return contextTriggered, errors.Join(errs...)
}

func waitForProcessReap(processGroupID int, waitDone <-chan error, timeout time.Duration) error {
	reapTimer := time.NewTimer(timeout)
	defer reapTimer.Stop()
	select {
	case <-waitDone:
		return nil
	case <-reapTimer.C:
		return fmt.Errorf("reap owned process group %d: timeout after %s", processGroupID, timeout)
	}
}

func signalProcessGroup(processGroupID int, signal syscall.Signal) error {
	if processGroupID <= 1 {
		return fmt.Errorf("invalid owned process group %d", processGroupID)
	}
	if err := syscall.Kill(-processGroupID, signal); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	return nil
}

func processGroupAlive(processGroupID int) (bool, error) {
	if processGroupID <= 1 {
		return false, fmt.Errorf("invalid owned process group %d", processGroupID)
	}
	err := syscall.Kill(-processGroupID, 0)
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, syscall.ESRCH):
		return false, nil
	default:
		return true, err
	}
}

func stopProcess(pid int) error {
	if pid <= 1 {
		return fmt.Errorf("invalid owned daemon PID %d", pid)
	}
	if err := syscall.Kill(pid, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	return nil
}
