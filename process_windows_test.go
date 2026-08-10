//go:build windows

package jcode

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestStopProcessTreatsTaskkillExit128AsAlreadyGone(t *testing.T) {
	taskkill := func(ctx context.Context, _ int) error {
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestWindowsProcessHelper$", "--", "exit128")
		cmd.Env = append(os.Environ(), "JCODE_GO_WINDOWS_PROCESS_HELPER=1")
		return cmd.Run()
	}

	if err := stopProcessWithTaskkill(42, time.Second, taskkill); err != nil {
		t.Fatalf("stopProcessWithTaskkill() error = %v, want nil for taskkill exit 128", err)
	}
}

func TestStopProcessBoundsTaskkill(t *testing.T) {
	taskkill := func(ctx context.Context, _ int) error {
		<-ctx.Done()
		return errors.New("taskkill did not exit")
	}

	started := time.Now()
	err := stopProcessWithTaskkill(42, 30*time.Millisecond, taskkill)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stopProcessWithTaskkill() error = %v, want deadline exceeded", err)
	}
	if elapsed := time.Since(started); elapsed >= time.Second {
		t.Fatalf("stopProcessWithTaskkill() took %s, want bounded taskkill", elapsed)
	}
}

func TestTerminateProcessBoundsMissingWaitResult(t *testing.T) {
	for name, waitDone := range map[string]<-chan error{
		"nil":            nil,
		"never-signaled": make(chan error),
	} {
		t.Run(name, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestWindowsProcessHelper$")
			cmd.Env = append(os.Environ(), "JCODE_GO_WINDOWS_PROCESS_HELPER=1")
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			actualWait := make(chan error, 1)
			go func() { actualWait <- cmd.Wait() }()

			started := time.Now()
			_, err := terminateProcess(cmd, 0, waitDone, 0, 30*time.Millisecond, nil)
			if err == nil || !strings.Contains(err.Error(), "timeout") {
				t.Fatalf("terminateProcess() error = %v, want bounded wait timeout", err)
			}
			if elapsed := time.Since(started); elapsed >= time.Second {
				t.Fatalf("terminateProcess() took %s, want finite reap bound", elapsed)
			}
			select {
			case <-actualWait:
			case <-time.After(time.Second):
				t.Fatal("killed helper was not reaped")
			}
		})
	}
}

func TestTerminateProcessReturnsKillAndWaitErrors(t *testing.T) {
	cmd := exec.Command(os.Args[0], "-test.run=^TestWindowsProcessHelper$")
	cmd.Env = append(os.Environ(), "JCODE_GO_WINDOWS_PROCESS_HELPER=1")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err == nil {
		t.Fatal("Wait() error = nil, want killed process error")
	}
	waitErr := errors.New("wait failed")
	waitDone := make(chan error, 1)
	waitDone <- waitErr

	_, err := terminateProcess(cmd, 0, waitDone, 0, time.Second, nil)
	if !errors.Is(err, os.ErrProcessDone) || !errors.Is(err, waitErr) {
		t.Fatalf("terminateProcess() error = %v, want joined kill and wait errors", err)
	}
}

func TestTerminateProcessPreservesSuccessfulTermination(t *testing.T) {
	cmd := exec.Command(os.Args[0], "-test.run=^TestWindowsProcessHelper$")
	cmd.Env = append(os.Environ(), "JCODE_GO_WINDOWS_PROCESS_HELPER=1")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	waitDone := make(chan error, 1)
	waitDone <- nil

	if _, err := terminateProcess(cmd, 0, waitDone, 0, time.Second, nil); err != nil {
		t.Fatalf("terminateProcess() error = %v, want nil", err)
	}
	_ = cmd.Wait()
}

func TestWindowsProcessHelper(t *testing.T) {
	if os.Getenv("JCODE_GO_WINDOWS_PROCESS_HELPER") != "1" {
		return
	}
	if len(os.Args) > 1 && os.Args[len(os.Args)-1] == "exit128" {
		os.Exit(128)
	}
	time.Sleep(24 * time.Hour)
}
