//go:build !windows

package jcode

import (
	"errors"
	"fmt"
	"os/exec"
	"syscall"
	"time"
)

const processExitPollInterval = 5 * time.Millisecond

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
	if processGroupID <= 1 {
		return false, fmt.Errorf("invalid owned process group %d", processGroupID)
	}
	grace = finiteDuration(grace, defaultShutdownGracePeriod)
	reapTimeout = finiteDuration(reapTimeout, defaultShutdownReapTimeout)

	var errs []error
	if err := signalProcessGroup(processGroupID, syscall.SIGTERM); err != nil {
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

	alive, aliveErr := processGroupAlive(processGroupID)
	if aliveErr != nil {
		errs = append(errs, fmt.Errorf("check owned process group %d after SIGTERM: %w", processGroupID, aliveErr))
	}
	if !alive && waitReceived {
		return contextTriggered, errors.Join(errs...)
	}

	if err := signalProcessGroup(processGroupID, syscall.SIGKILL); err != nil {
		errs = append(errs, fmt.Errorf("signal owned process group %d with SIGKILL: %w", processGroupID, err))
	}

	reapDeadline := time.Now().Add(reapTimeout)
	if !waitReceived {
		remaining := time.Until(reapDeadline)
		if remaining <= 0 {
			errs = append(errs, fmt.Errorf("reap owned process group %d: timeout after %s", processGroupID, reapTimeout))
			return contextTriggered, errors.Join(errs...)
		}
		reapTimer := time.NewTimer(remaining)
		select {
		case <-waitDone:
			waitReceived = true
		case <-reapTimer.C:
			errs = append(errs, fmt.Errorf("reap owned process group %d: timeout after %s", processGroupID, reapTimeout))
		}
		if !reapTimer.Stop() {
			select {
			case <-reapTimer.C:
			default:
			}
		}
	}
	if waitReceived {
		if err := waitForProcessGroupExit(processGroupID, reapDeadline); err != nil {
			errs = append(errs, err)
		}
	}
	return contextTriggered, errors.Join(errs...)
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

func waitForProcessGroupExit(processGroupID int, deadline time.Time) error {
	for {
		alive, err := processGroupAlive(processGroupID)
		if err != nil {
			return fmt.Errorf("check owned process group %d after SIGKILL: %w", processGroupID, err)
		}
		if !alive {
			return nil
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return fmt.Errorf("owned process group %d still exists after bounded reap", processGroupID)
		}
		delay := min(processExitPollInterval, remaining)
		timer := time.NewTimer(delay)
		<-timer.C
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
