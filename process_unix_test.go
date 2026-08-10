//go:build linux

package jcode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

const linuxProcessHelperEnv = "JCODE_GO_LINUX_PROCESS_HELPER"

func TestLaunchOptionsShutdownBoundsUseFiniteDefaults(t *testing.T) {
	options := (LaunchOptions{
		ShutdownGracePeriod: -time.Second,
		ShutdownReapTimeout: -time.Second,
	}).withDefaults()
	if options.ShutdownGracePeriod != 5*time.Second {
		t.Fatalf("ShutdownGracePeriod = %s, want 5s", options.ShutdownGracePeriod)
	}
	if options.ShutdownReapTimeout != 5*time.Second {
		t.Fatalf("ShutdownReapTimeout = %s, want 5s", options.ShutdownReapTimeout)
	}
}

func TestShutdownProcessGroupCooperativeTERM(t *testing.T) {
	process := startLinuxHelper(t, "cooperative")
	instance := testProcessInstance(t, process, 3*time.Second, 3*time.Second)

	started := time.Now()
	if err := instance.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed >= 2500*time.Millisecond {
		t.Fatalf("cooperative shutdown took %s, want less than TERM grace", elapsed)
	}
	if _, err := os.Stat(process.termPath); err != nil {
		t.Fatalf("TERM marker: %v", err)
	}
	assertProcessTerminated(t, process.cmd.Process.Pid)
}

func TestShutdownProcessGroupEscalatesAfterGrace(t *testing.T) {
	process := startLinuxHelper(t, "ignore")
	const grace = 120 * time.Millisecond
	instance := testProcessInstance(t, process, grace, time.Second)
	recorder := &safeObservationRecorder{}
	instance.observer = recorder

	started := time.Now()
	if err := instance.Shutdown(); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(started)
	if elapsed < grace {
		t.Fatalf("shutdown took %s, want at least TERM grace %s", elapsed, grace)
	}
	if elapsed >= time.Second {
		t.Fatalf("shutdown took %s, want bounded KILL escalation", elapsed)
	}
	if _, err := os.Stat(process.termPath); err != nil {
		t.Fatalf("TERM marker: %v", err)
	}
	assertProcessTerminated(t, process.cmd.Process.Pid)
	observations := recorder.waitForKind(t, "shutdown_complete")
	assertObservationSubsequence(t, observations, []Observation{
		{Kind: "shutdown_start"},
		{Kind: "shutdown_grace_start"},
		{Kind: "shutdown_force_kill"},
		{Kind: "shutdown_reap_complete"},
		{Kind: "shutdown_cleanup_complete"},
		{Kind: "shutdown_complete"},
	})
	assertObservationsExclude(t, observations, process.termPath, process.childPIDPath)
}

func TestShutdownKillsTERMInheritingGroupBeforeLeaderReap(t *testing.T) {
	process := startLinuxHelper(t, "descendant")
	data, err := os.ReadFile(process.childPIDPath)
	if err != nil {
		t.Fatal(err)
	}
	childPID, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		t.Fatal(err)
	}
	instance := testProcessInstance(t, process, 100*time.Millisecond, time.Second)

	if err := instance.Shutdown(); err != nil {
		t.Fatal(err)
	}
	assertProcessTerminated(t, process.cmd.Process.Pid)
	assertProcessTerminated(t, childPID)
}

func TestShutdownCancellationForcesEscalationButStillReaps(t *testing.T) {
	process := startLinuxHelper(t, "ignore")
	instance := testProcessInstance(t, process, 5*time.Second, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	started := time.Now()
	err := ShutdownInstance(ctx, instance)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ShutdownInstance() error = %v, want context.Canceled", err)
	}
	if elapsed := time.Since(started); elapsed >= time.Second {
		t.Fatalf("canceled shutdown took %s, want immediate escalation", elapsed)
	}
	assertProcessTerminated(t, process.cmd.Process.Pid)
}

func TestShutdownDeadlineDuringGraceForcesEscalation(t *testing.T) {
	process := startLinuxHelper(t, "ignore")
	instance := testProcessInstance(t, process, 5*time.Second, 3*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	started := time.Now()
	err := ShutdownInstance(ctx, instance)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ShutdownInstance() error = %v, want context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(started); elapsed >= 2*time.Second {
		t.Fatalf("deadline-triggered shutdown took %s, want grace shortened", elapsed)
	}
	assertProcessTerminated(t, process.cmd.Process.Pid)
}

func TestShutdownCancellationIsPreservedAlongsidePhaseFailure(t *testing.T) {
	instance := &launchedInstance{processGroupID: 1, cleanupTimeout: 20 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := ShutdownInstance(ctx, instance)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ShutdownInstance() error = %v, want context canceled joined to phase failure", err)
	}
	if err == nil || !strings.Contains(err.Error(), "invalid owned process group") {
		t.Fatalf("ShutdownInstance() error = %v, want process-group phase failure", err)
	}
}

func TestTerminateProcessDoesNotInspectOrSignalAfterWaitReceived(t *testing.T) {
	var signals []syscall.Signal
	aliveCalls := 0
	operations := processGroupOperations{
		signal: func(_ int, signal syscall.Signal) error {
			signals = append(signals, signal)
			return nil
		},
		alive: func(int) (bool, error) {
			aliveCalls++
			return true, nil
		},
	}

	_, err := terminateProcessGroup(42, closedWaitResult(nil), time.Second, time.Second, nil, operations)
	if err != nil {
		t.Fatal(err)
	}
	if len(signals) != 0 {
		t.Fatalf("signals = %v, want none after observing reap", signals)
	}
	if aliveCalls != 0 {
		t.Fatalf("post-reap liveness checks = %d, want 0", aliveCalls)
	}
}

func TestTerminateProcessDoesNotInspectOrKillWhenWaitArrivesAfterTERM(t *testing.T) {
	waitDone := make(chan error, 1)
	var signals []syscall.Signal
	aliveCalls := 0
	operations := processGroupOperations{
		signal: func(_ int, signal syscall.Signal) error {
			signals = append(signals, signal)
			if signal == syscall.SIGTERM {
				waitDone <- nil
			}
			return nil
		},
		alive: func(int) (bool, error) {
			aliveCalls++
			return true, nil
		},
	}

	_, err := terminateProcessGroup(42, waitDone, time.Second, time.Second, nil, operations)
	if err != nil {
		t.Fatal(err)
	}
	if len(signals) != 1 || signals[0] != syscall.SIGTERM {
		t.Fatalf("signals = %v, want only SIGTERM before observing reap", signals)
	}
	if aliveCalls != 0 {
		t.Fatalf("post-reap liveness checks = %d, want 0", aliveCalls)
	}
}

func TestTerminateProcessAggregatesUnreapedEscalationErrors(t *testing.T) {
	termErr := errors.New("term failed")
	killErr := errors.New("kill failed")
	operations := processGroupOperations{
		signal: func(_ int, signal syscall.Signal) error {
			if signal == syscall.SIGTERM {
				return termErr
			}
			return killErr
		},
		alive: func(int) (bool, error) { return true, nil },
	}

	started := time.Now()
	_, err := terminateProcessGroup(42, make(chan error), time.Millisecond, 10*time.Millisecond, nil, operations)
	if !errors.Is(err, termErr) || !errors.Is(err, killErr) || !strings.Contains(err.Error(), "reap") {
		t.Fatalf("terminateProcessGroup() error = %v, want TERM, KILL, and reap errors", err)
	}
	if elapsed := time.Since(started); elapsed >= time.Second {
		t.Fatalf("terminateProcessGroup() took %s, want finite escalation", elapsed)
	}
}

func TestTerminateProcessBoundsReapWhenUnreapedGroupIsAbsent(t *testing.T) {
	var signals []syscall.Signal
	operations := processGroupOperations{
		signal: func(_ int, signal syscall.Signal) error {
			signals = append(signals, signal)
			return nil
		},
		alive: func(int) (bool, error) { return false, nil },
	}

	started := time.Now()
	_, err := terminateProcessGroup(42, make(chan error), time.Millisecond, 10*time.Millisecond, nil, operations)
	if err == nil || !strings.Contains(err.Error(), "reap") {
		t.Fatalf("terminateProcessGroup() error = %v, want bounded reap timeout", err)
	}
	if len(signals) != 1 || signals[0] != syscall.SIGTERM {
		t.Fatalf("signals = %v, want TERM without KILL for absent group", signals)
	}
	if elapsed := time.Since(started); elapsed >= time.Second {
		t.Fatalf("terminateProcessGroup() took %s, want finite reap", elapsed)
	}
}

func TestShutdownBoundedWhenWaitResultIsWithheld(t *testing.T) {
	process := startLinuxHelper(t, "ignore")
	hiddenWait := process.waitDone
	process.waitDone = make(chan error)
	instance := testProcessInstance(t, process, 25*time.Millisecond, 80*time.Millisecond)

	started := time.Now()
	err := instance.Shutdown()
	if err == nil || !strings.Contains(err.Error(), "reap") {
		t.Fatalf("Shutdown() error = %v, want bounded reap error", err)
	}
	if elapsed := time.Since(started); elapsed >= 500*time.Millisecond {
		t.Fatalf("shutdown took %s, want bounded reap", elapsed)
	}
	select {
	case <-hiddenWait:
	case <-time.After(time.Second):
		t.Fatal("actual process wait did not complete after KILL")
	}
}

func TestShutdownAlreadyExitedProcessTreatsESRCHAsSuccess(t *testing.T) {
	process := startLinuxHelper(t, "exit")
	select {
	case <-process.waitDone:
	case <-time.After(3 * time.Second):
		t.Fatal("helper did not exit")
	}
	process.waitDone = closedWaitResult(nil)
	instance := testProcessInstance(t, process, 20*time.Millisecond, 100*time.Millisecond)

	if err := instance.Shutdown(); err != nil {
		t.Fatal(err)
	}
}

func TestShutdownNeverSignalsInvalidOrUnrecordedGroups(t *testing.T) {
	for _, processGroupID := range []int{0, 1, -1} {
		t.Run(strconv.Itoa(processGroupID), func(t *testing.T) {
			process := startLinuxHelper(t, "ignore")
			instance := testProcessInstance(t, process, 10*time.Millisecond, 40*time.Millisecond)
			instance.processGroupID = processGroupID

			err := instance.Shutdown()
			if err == nil || !strings.Contains(err.Error(), "process group") {
				t.Fatalf("Shutdown() error = %v, want invalid process group error", err)
			}
			if !processIsRunning(process.cmd.Process.Pid) {
				t.Fatal("invalid ownership identity signaled the helper")
			}
			_ = syscall.Kill(-process.processGroupID, syscall.SIGKILL)
			select {
			case <-process.waitDone:
			case <-time.After(time.Second):
				t.Fatal("test cleanup did not reap helper")
			}
		})
	}
}

func TestConcurrentShutdownCallersShareOneResult(t *testing.T) {
	process := startLinuxHelper(t, "ignore")
	instance := testProcessInstance(t, process, 80*time.Millisecond, time.Second)

	const callers = 24
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for n := 0; n < callers; n++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			switch n % 3 {
			case 0:
				errs <- instance.Shutdown()
			case 1:
				errs <- instance.Close()
			default:
				errs <- ShutdownInstance(context.Background(), instance)
			}
		}(n)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("shared shutdown error = %v, want nil", err)
		}
	}
	assertProcessTerminated(t, process.cmd.Process.Pid)
}

func TestConcurrentShutdownCallersShareOneErrorResult(t *testing.T) {
	instance := &launchedInstance{processGroupID: 1, cleanupTimeout: 20 * time.Millisecond}
	const callers = 12
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for n := 0; n < callers; n++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			if n%2 == 0 {
				errs <- instance.Shutdown()
				return
			}
			errs <- ShutdownInstance(context.Background(), instance)
		}(n)
	}
	wg.Wait()
	close(errs)
	var first error
	for err := range errs {
		if err == nil {
			t.Fatal("shared shutdown error = nil, want invalid process group error")
		}
		if first == nil {
			first = err
			continue
		}
		if err != first {
			t.Fatalf("shutdown callers received different stored errors: %p != %p", err, first)
		}
	}
}

func TestShutdownCleansPersistentRuntimeButRetainsPersistentHome(t *testing.T) {
	home, err := os.MkdirTemp("", "jc-persistent-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	runtimeDir := filepath.Join(home, "run")
	if err := os.Mkdir(runtimeDir, 0o700); err != nil {
		t.Fatal(err)
	}
	owned := createOwnedRuntimePaths(t, runtimeDir)
	unrelated := filepath.Join(runtimeDir, "caller-owned")
	if err := os.WriteFile(unrelated, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	instance := &launchedInstance{
		socketPath: filepath.Join(runtimeDir, "jcode-api.sock"), jcodeHome: home,
		runtimeDir: runtimeDir, ownedPaths: owned, cleanupTimeout: 100 * time.Millisecond,
	}

	if err := instance.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(home); err != nil {
		t.Fatalf("persistent home removed: %v", err)
	}
	if _, err := os.Stat(unrelated); err != nil {
		t.Fatalf("unrelated runtime path removed: %v", err)
	}
	for _, path := range owned {
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			t.Fatalf("owned path %s still exists: %v", path, err)
		}
	}
}

func TestShutdownRemovesEmptyPersistentRuntimeButRetainsDurableHome(t *testing.T) {
	home, err := os.MkdirTemp("", "jc-durable-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	runtimeDir := filepath.Join(home, "run")
	if err := os.Mkdir(runtimeDir, 0o700); err != nil {
		t.Fatal(err)
	}
	durable := filepath.Join(home, "config.toml")
	if err := os.WriteFile(durable, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	owned := createOwnedRuntimePaths(t, runtimeDir)
	instance := &launchedInstance{
		socketPath: filepath.Join(runtimeDir, "jcode-api.sock"), jcodeHome: home,
		runtimeDir: runtimeDir, ownedPaths: owned, cleanupTimeout: 100 * time.Millisecond,
	}

	if err := instance.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(runtimeDir); !os.IsNotExist(err) {
		t.Fatalf("owned runtime directory still exists: %v", err)
	}
	if data, err := os.ReadFile(durable); err != nil || string(data) != "keep" {
		t.Fatalf("durable caller state = %q, %v", data, err)
	}
}

func TestShutdownRemovesSDKCreatedEphemeralHome(t *testing.T) {
	home, err := os.MkdirTemp("", instanceHomePrefix)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	runtimeDir := filepath.Join(home, "run")
	if err := os.Mkdir(runtimeDir, 0o700); err != nil {
		t.Fatal(err)
	}
	owned := createOwnedRuntimePaths(t, runtimeDir)
	instance := &launchedInstance{
		socketPath: filepath.Join(runtimeDir, "jcode-api.sock"), jcodeHome: home,
		runtimeDir: runtimeDir, ownedPaths: owned, ephemeral: true,
		cleanupTimeout: 100 * time.Millisecond,
	}

	if err := instance.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(home); !os.IsNotExist(err) {
		t.Fatalf("ephemeral home still exists: %v", err)
	}
}

func TestShutdownRejectsUnownedAndSymlinkRuntimePaths(t *testing.T) {
	home := t.TempDir()
	outside := t.TempDir()
	outsideFile := filepath.Join(outside, "jcode-api.sock")
	if err := os.WriteFile(outsideFile, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	runtimeLink := filepath.Join(home, "run")
	if err := os.Symlink(outside, runtimeLink); err != nil {
		t.Fatal(err)
	}
	instance := &launchedInstance{
		socketPath: outsideFile, jcodeHome: home, runtimeDir: runtimeLink,
		ownedPaths: []string{outsideFile}, cleanupTimeout: 20 * time.Millisecond,
	}

	err := instance.Shutdown()
	if err == nil || !strings.Contains(err.Error(), "owned runtime") {
		t.Fatalf("Shutdown() error = %v, want owned runtime validation error", err)
	}
	if _, err := os.Stat(outsideFile); err != nil {
		t.Fatalf("unowned path removed: %v", err)
	}
}

func TestShutdownCleanupContinuesAfterRuntimePathError(t *testing.T) {
	home, err := os.MkdirTemp("", instanceHomePrefix)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	outside := t.TempDir()
	outsideFile := filepath.Join(outside, "jcode-api.sock")
	if err := os.WriteFile(outsideFile, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	runtimeLink := filepath.Join(home, "run")
	if err := os.Symlink(outside, runtimeLink); err != nil {
		t.Fatal(err)
	}
	instance := &launchedInstance{
		jcodeHome: home, runtimeDir: runtimeLink, ownedPaths: []string{outsideFile},
		ephemeral: true, cleanupTimeout: 40 * time.Millisecond,
	}

	started := time.Now()
	err = instance.Shutdown()
	if err == nil || !strings.Contains(err.Error(), "owned runtime") {
		t.Fatalf("Shutdown() error = %v, want owned runtime validation error", err)
	}
	if elapsed := time.Since(started); elapsed >= 200*time.Millisecond {
		t.Fatalf("cleanup took %s, want one shared cleanup bound", elapsed)
	}
	if _, err := os.Lstat(home); !os.IsNotExist(err) {
		t.Fatalf("ephemeral home still exists after runtime cleanup error: %v", err)
	}
	if _, err := os.Stat(outsideFile); err != nil {
		t.Fatalf("unowned path removed after cleanup error: %v", err)
	}
}

func TestRemoveOwnedRuntimePathsReturnsUnsafeTypeWithoutRetrying(t *testing.T) {
	home := t.TempDir()
	runtimeDir := filepath.Join(home, "run")
	if err := os.Mkdir(runtimeDir, 0o700); err != nil {
		t.Fatal(err)
	}
	unsafePath := filepath.Join(runtimeDir, "jcode-api.sock")
	if err := os.Symlink(filepath.Join(t.TempDir(), "outside"), unsafePath); err != nil {
		t.Fatal(err)
	}

	started := time.Now()
	err := removeOwnedRuntimePaths(home, runtimeDir, []string{unsafePath}, 300*time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "unsafe type") {
		t.Fatalf("removeOwnedRuntimePaths() error = %v, want unsafe type", err)
	}
	if elapsed := time.Since(started); elapsed >= 100*time.Millisecond {
		t.Fatalf("permanent cleanup error returned after %s, want no retry", elapsed)
	}
}

func TestRemoveOwnedRuntimePathsReturnsLstatErrorWithoutRetrying(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory search permission checks")
	}
	home := t.TempDir()
	runtimeDir := filepath.Join(home, "run")
	if err := os.Mkdir(runtimeDir, 0o700); err != nil {
		t.Fatal(err)
	}
	ownedPath := filepath.Join(runtimeDir, "jcode-api.sock")
	if err := os.WriteFile(ownedPath, []byte("owned"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(runtimeDir, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(runtimeDir, 0o700) })

	started := time.Now()
	err := removeOwnedRuntimePaths(home, runtimeDir, []string{ownedPath}, 300*time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "inspect owned runtime file") {
		t.Fatalf("removeOwnedRuntimePaths() error = %v, want Lstat error", err)
	}
	if elapsed := time.Since(started); elapsed >= 100*time.Millisecond {
		t.Fatalf("permanent Lstat error returned after %s, want no retry", elapsed)
	}
}

func TestLaunchInstanceRejectsSymlinkRuntimeBeforeRemovingPaths(t *testing.T) {
	home := t.TempDir()
	outside := t.TempDir()
	outsideSocket := filepath.Join(outside, "jcode-api.sock")
	if err := os.WriteFile(outsideSocket, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(home, "run")); err != nil {
		t.Fatal(err)
	}
	inheritLogins := false
	instance, err := LaunchInstance(LaunchOptions{
		JcodeHome: home, InheritLogins: &inheritLogins, Binary: filepath.Join(t.TempDir(), "missing-jcode"),
	})
	if err == nil || instance != nil {
		t.Fatalf("LaunchInstance() = (%v, %v), want startup failure", instance, err)
	}
	if data, readErr := os.ReadFile(outsideSocket); readErr != nil || string(data) != "keep" {
		t.Fatalf("outside socket = %q, %v; launch followed runtime symlink", data, readErr)
	}
}

func TestShutdownStopsOnlyRecordedDaemonPID(t *testing.T) {
	daemon := startLinuxHelper(t, "cooperative")
	unrelated := startLinuxHelper(t, "ignore")
	home := t.TempDir()
	runtimeDir := filepath.Join(home, "run")
	if err := os.Mkdir(runtimeDir, 0o700); err != nil {
		t.Fatal(err)
	}
	registry := map[string]any{
		"private": map[string]any{
			"socket": filepath.Join(runtimeDir, "jcode.sock"),
			"pid":    daemon.cmd.Process.Pid,
		},
		"unrelated": map[string]any{
			"socket": filepath.Join(t.TempDir(), "jcode.sock"),
			"pid":    unrelated.cmd.Process.Pid,
		},
	}
	data, err := json.Marshal(registry)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, "servers.json"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	instance := &launchedInstance{
		jcodeHome: home, runtimeDir: runtimeDir,
		cleanupTimeout: 20 * time.Millisecond,
	}

	if err := instance.Shutdown(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-daemon.waitDone:
	case <-time.After(3 * time.Second):
		t.Fatal("recorded daemon did not stop")
	}
	if !processIsRunning(unrelated.cmd.Process.Pid) {
		t.Fatal("shutdown affected a daemon registered for another runtime socket")
	}
}

func TestReadDaemonPIDRequiresMatchingSocketAndSafePID(t *testing.T) {
	home := t.TempDir()
	runtimeDir := filepath.Join(home, "run")
	if err := os.Mkdir(runtimeDir, 0o700); err != nil {
		t.Fatal(err)
	}
	registryPath := filepath.Join(home, "servers.json")
	writeRegistry := func(t *testing.T, socket string, pid int) {
		t.Helper()
		data, err := json.Marshal(map[string]any{
			"entry": map[string]any{"socket": socket, "pid": pid},
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(registryPath, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}

	writeRegistry(t, filepath.Join(runtimeDir, "jcode.sock"), 4242)
	if pid := readDaemonPID(home, runtimeDir); pid != 4242 {
		t.Fatalf("readDaemonPID() = %d, want matching PID", pid)
	}
	writeRegistry(t, filepath.Join(t.TempDir(), "jcode.sock"), 4242)
	if pid := readDaemonPID(home, runtimeDir); pid != 0 {
		t.Fatalf("readDaemonPID() = %d, want 0 for another socket", pid)
	}
	writeRegistry(t, filepath.Join(runtimeDir, "jcode.sock"), 1)
	if pid := readDaemonPID(home, runtimeDir); pid != 0 {
		t.Fatalf("readDaemonPID() = %d, want 0 for unsafe PID", pid)
	}
}

func TestReadDaemonPIDRejectsSymlinkedRegistry(t *testing.T) {
	home := t.TempDir()
	runtimeDir := filepath.Join(home, "run")
	if err := os.Mkdir(runtimeDir, 0o700); err != nil {
		t.Fatal(err)
	}
	registry := map[string]any{
		"private": map[string]any{"socket": filepath.Join(runtimeDir, "jcode.sock"), "pid": os.Getpid()},
	}
	data, err := json.Marshal(registry)
	if err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "servers.json")
	if err := os.WriteFile(outside, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(home, "servers.json")); err != nil {
		t.Fatal(err)
	}

	if pid := readDaemonPID(home, runtimeDir); pid != 0 {
		t.Fatalf("readDaemonPID() = %d, want 0 for symlinked registry", pid)
	}
}

func TestShutdownInstancePreservesExternalInstanceCompatibility(t *testing.T) {
	instance := &trackingInstance{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := ShutdownInstance(ctx, instance)
	if err != nil {
		t.Fatalf("ShutdownInstance() error = %v, want nil after successful external shutdown", err)
	}
	if instance.shutdowns.Load() != 1 {
		t.Fatalf("external instance shutdowns = %d, want 1", instance.shutdowns.Load())
	}
}

type linuxHelperProcess struct {
	cmd            *exec.Cmd
	processGroupID int
	waitDone       <-chan error
	readyPath      string
	termPath       string
	childPIDPath   string
}

func startLinuxHelper(t *testing.T, mode string) *linuxHelperProcess {
	t.Helper()
	dir := t.TempDir()
	process := &linuxHelperProcess{
		readyPath: filepath.Join(dir, "ready"), termPath: filepath.Join(dir, "term"),
		childPIDPath: filepath.Join(dir, "child-pid"),
	}
	process.cmd = exec.Command(os.Args[0], "-test.run=^TestLinuxPrivateProcessHelper$", "--",
		mode, process.readyPath, process.termPath, process.childPIDPath)
	process.cmd.Env = append(os.Environ(), linuxProcessHelperEnv+"=1")
	process.cmd.Stdout = nil
	process.cmd.Stderr = nil
	processGroupID, err := startProcess(process.cmd)
	if err != nil {
		t.Fatal(err)
	}
	process.processGroupID = processGroupID
	actualGroupID, err := syscall.Getpgid(process.cmd.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	if actualGroupID != processGroupID || processGroupID != process.cmd.Process.Pid {
		t.Fatalf("process group = %d, recorded = %d, pid = %d", actualGroupID, processGroupID, process.cmd.Process.Pid)
	}
	waitDone := make(chan error, 1)
	go func() { waitDone <- process.cmd.Wait() }()
	process.waitDone = waitDone
	t.Cleanup(func() {
		_ = syscall.Kill(-process.processGroupID, syscall.SIGKILL)
		select {
		case <-waitDone:
		default:
		}
	})
	waitForPath(t, process.readyPath)
	return process
}

func testProcessInstance(t *testing.T, process *linuxHelperProcess, grace, reap time.Duration) *launchedInstance {
	t.Helper()
	home := t.TempDir()
	runtimeDir := filepath.Join(home, "run")
	if err := os.Mkdir(runtimeDir, 0o700); err != nil {
		t.Fatal(err)
	}
	return &launchedInstance{
		jcodeHome: home, runtimeDir: runtimeDir, cmd: process.cmd,
		processGroupID: process.processGroupID, waitDone: process.waitDone,
		shutdownGracePeriod: grace, shutdownReapTimeout: reap,
		cleanupTimeout: 20 * time.Millisecond,
	}
}

func createOwnedRuntimePaths(t *testing.T, runtimeDir string) []string {
	t.Helper()
	paths := instanceOwnedRuntimePaths(runtimeDir)
	listener, err := net.Listen("unix", paths[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	for _, path := range paths[1:] {
		if err := os.WriteFile(path, []byte("owned"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return paths
}

func closedWaitResult(err error) <-chan error {
	result := make(chan error, 1)
	result <- err
	return result
}

func waitForPath(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for helper path %s", filepath.Base(path))
}

func assertProcessTerminated(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if !processIsRunning(pid) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("process %d is still running", pid)
}

func processIsRunning(pid int) bool {
	if pid <= 1 {
		return false
	}
	err := syscall.Kill(pid, 0)
	if errors.Is(err, syscall.ESRCH) {
		return false
	}
	if err != nil {
		return true
	}
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err == nil && procStatState(data) == "Z" {
		return false
	}
	return true
}

func procStatState(data []byte) string {
	text := string(data)
	commandEnd := strings.LastIndexByte(text, ')')
	if commandEnd < 0 {
		return ""
	}
	fields := strings.Fields(text[commandEnd+1:])
	if len(fields) == 0 {
		return ""
	}
	return fields[0]
}

func TestProcStatStateParsesAfterFinalCommandParenthesis(t *testing.T) {
	stat := []byte("123 (worker name) with ) characters) Z 1 2 3")
	if state := procStatState(stat); state != "Z" {
		t.Fatalf("procStatState() = %q, want Z", state)
	}
	if state := procStatState([]byte("malformed")); state != "" {
		t.Fatalf("procStatState(malformed) = %q, want empty", state)
	}
}

func TestLinuxPrivateProcessHelper(t *testing.T) {
	if os.Getenv(linuxProcessHelperEnv) != "1" {
		return
	}
	separator := 0
	for n, arg := range os.Args {
		if arg == "--" {
			separator = n
			break
		}
	}
	if separator == 0 || len(os.Args) < separator+5 {
		os.Exit(2)
	}
	mode := os.Args[separator+1]
	readyPath := os.Args[separator+2]
	termPath := os.Args[separator+3]
	childPIDPath := os.Args[separator+4]
	if mode == "exit" {
		_ = os.WriteFile(readyPath, []byte("ready"), 0o600)
		return
	}

	signals := make(chan os.Signal, 2)
	signal.Notify(signals, syscall.SIGTERM)
	if mode == "descendant" {
		childReady := readyPath + "-child"
		child := exec.Command(os.Args[0], "-test.run=^TestLinuxPrivateProcessHelper$", "--",
			"ignore", childReady, termPath+"-child", childPIDPath+"-unused")
		child.Env = append(os.Environ(), linuxProcessHelperEnv+"=1")
		if err := child.Start(); err != nil {
			os.Exit(3)
		}
		if err := os.WriteFile(childPIDPath, []byte(strconv.Itoa(child.Process.Pid)), 0o600); err != nil {
			os.Exit(4)
		}
		deadline := time.Now().Add(time.Second)
		for time.Now().Before(deadline) {
			if _, err := os.Stat(childReady); err == nil {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	if err := os.WriteFile(readyPath, []byte("ready"), 0o600); err != nil {
		os.Exit(5)
	}
	for sig := range signals {
		if sig != syscall.SIGTERM {
			continue
		}
		_ = os.WriteFile(termPath, []byte("term"), 0o600)
		if mode == "cooperative" {
			return
		}
	}
}
