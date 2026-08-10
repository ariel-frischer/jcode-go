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
	leaderReaped := false
	select {
	case <-waitDone:
		leaderReaped = true
	default:
	}
	if leaderReaped {
		alive, err := operations.alive(processGroupID)
		if err != nil {
			return false, fmt.Errorf("check owned process group %d after bridge exit: %w", processGroupID, err)
		}
		if !alive {
			return false, nil
		}
	}

	var errs []error
	if err := operations.signal(processGroupID, syscall.SIGTERM); err != nil {
		errs = append(errs, fmt.Errorf("signal owned process group %d with SIGTERM: %w", processGroupID, err))
	}

	contextTriggered := false
	graceTimer := time.NewTimer(grace)
	checkTimer := time.NewTicker(10 * time.Millisecond)
	graceExpired := false
	groupExited := false
	for !graceExpired && !groupExited {
		var wait <-chan error
		if !leaderReaped {
			wait = waitDone
		}
		select {
		case <-wait:
			leaderReaped = true
		case <-checkTimer.C:
			alive, err := operations.alive(processGroupID)
			if err != nil {
				errs = append(errs, fmt.Errorf("check owned process group %d during SIGTERM grace: %w", processGroupID, err))
				graceExpired = true
				continue
			}
			groupExited = !alive
		case <-graceTimer.C:
			graceExpired = true
		case <-forceKill:
			contextTriggered = true
			graceExpired = true
		}
	}
	checkTimer.Stop()
	if !graceTimer.Stop() {
		select {
		case <-graceTimer.C:
		default:
		}
	}
	if groupExited {
		if !leaderReaped {
			if err := waitForProcessReap(processGroupID, waitDone, reapTimeout); err != nil {
				errs = append(errs, err)
			}
		}
		return contextTriggered, errors.Join(errs...)
	}

	alive, aliveErr := operations.alive(processGroupID)
	if aliveErr != nil {
		errs = append(errs, fmt.Errorf("check owned process group %d after SIGTERM: %w", processGroupID, aliveErr))
	}
	if !alive {
		if !leaderReaped {
			if err := waitForProcessReap(processGroupID, waitDone, reapTimeout); err != nil {
				errs = append(errs, err)
			}
		}
		return contextTriggered, errors.Join(errs...)
	}

	if err := operations.signal(processGroupID, syscall.SIGKILL); err != nil {
		errs = append(errs, fmt.Errorf("signal owned process group %d with SIGKILL: %w", processGroupID, err))
	}

	if !leaderReaped {
		if err := waitForProcessReap(processGroupID, waitDone, reapTimeout); err != nil {
			errs = append(errs, err)
		}
	}
	if err := waitForProcessGroupExit(processGroupID, reapTimeout, operations.alive); err != nil {
		errs = append(errs, err)
	}
	return contextTriggered, errors.Join(errs...)
}

func waitForProcessGroupExit(processGroupID int, timeout time.Duration, alive func(int) (bool, error)) error {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		running, err := alive(processGroupID)
		if err != nil {
			return fmt.Errorf("check owned process group %d after SIGKILL: %w", processGroupID, err)
		}
		if !running {
			return nil
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			return fmt.Errorf("reap owned process group %d: timeout after %s", processGroupID, timeout)
		}
	}
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
