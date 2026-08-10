//go:build linux

package jcode_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	jcode "github.com/ariel-frischer/jcode-go"
	"github.com/ariel-frischer/jcode-go/protocol"
)

const (
	acceptanceHelperEnv  = "JCODE_GO_ACCEPTANCE_HELPER"
	acceptanceModeEnv    = "JCODE_GO_ACCEPTANCE_MODE"
	acceptanceMarksEnv   = "JCODE_GO_ACCEPTANCE_MARKS"
	acceptanceTriggerEnv = "JCODE_GO_ACCEPTANCE_TRIGGER"
	realOAuthSmokeEnv    = "JCODE_GO_REAL_OAUTH_SMOKE"
	realOAuthWorkdirEnv  = "JCODE_GO_REAL_OAUTH_WORKDIR"
)

func TestPrivateRuntimeLifecycleAcceptance(t *testing.T) {
	worktree, err := filepath.Abs(".")
	if err != nil {
		t.Fatalf("resolve acceptance worktree: %T", err)
	}

	t.Run("accepted_stall_cancel_and_cleanup", func(t *testing.T) {
		harness := launchAcceptanceHarness(t, worktree, "term_ignore")
		session := createAcceptanceSession(t, harness, worktree)
		turn, err := session.StartTurn(context.Background(), "synthetic acceptance input", jcode.SendOptions{})
		if err != nil {
			t.Fatalf("StartTurn failed: %T", err)
		}
		acceptedCtx, cancelAccepted := context.WithTimeout(context.Background(), time.Second)
		defer cancelAccepted()
		if err := turn.Accepted(acceptedCtx); err != nil {
			t.Fatalf("Accepted failed: %T", err)
		}

		stalledCtx, cancelStalled := context.WithTimeout(context.Background(), 40*time.Millisecond)
		_, waitErr := turn.Wait(stalledCtx)
		cancelStalled()
		if !errors.Is(waitErr, context.DeadlineExceeded) {
			t.Fatalf("stalled Wait classification = %T, want deadline", waitErr)
		}

		cancelCtx, cancelTurn := context.WithTimeout(context.Background(), time.Second)
		started := time.Now()
		if err := turn.Cancel(cancelCtx); err != nil {
			cancelTurn()
			t.Fatalf("Cancel failed: %T", err)
		}
		cancelTurn()
		if elapsed := time.Since(started); elapsed >= time.Second {
			t.Fatalf("Cancel took %s, want bounded completion", elapsed)
		}

		waitCtx, cancelWait := context.WithTimeout(context.Background(), time.Second)
		result, err := turn.Wait(waitCtx)
		cancelWait()
		if err != nil {
			t.Fatalf("fresh Wait failed: %T", err)
		}
		if result.Kind != jcode.TurnResultCanceled || !errors.Is(result.Err, jcode.ErrTurnCanceled) {
			t.Fatalf("terminal kind = %q, canceled=%t", result.Kind, errors.Is(result.Err, jcode.ErrTurnCanceled))
		}
		shutdownElapsed := harness.closeAndAssertClean(t)
		if shutdownElapsed < 80*time.Millisecond {
			t.Fatalf("private shutdown elapsed_ms=%d, want TERM grace before KILL", shutdownElapsed.Milliseconds())
		}
		if pathMissing(filepath.Join(harness.marksDir, "term-observed")) {
			t.Fatal("TERM-ignoring descendant did not observe group TERM")
		}
	})

	t.Run("owned_bridge_exit_is_typed_and_cleanup_is_bounded", func(t *testing.T) {
		harness := launchAcceptanceHarness(t, worktree, "bridge_exit")
		session := createAcceptanceSession(t, harness, worktree)
		turn, err := session.StartTurn(context.Background(), "synthetic bridge-exit input", jcode.SendOptions{})
		if err != nil {
			t.Fatalf("StartTurn failed: %T", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := turn.Accepted(ctx); err != nil {
			t.Fatalf("Accepted failed: %T", err)
		}
		if err := os.WriteFile(harness.triggerPath, []byte("exit"), 0o600); err != nil {
			t.Fatalf("trigger bridge exit: %T", err)
		}
		result, err := turn.Wait(ctx)
		if err != nil {
			t.Fatalf("Wait failed: %T", err)
		}
		if result.Kind != jcode.TurnResultBridgeExited || !errors.Is(result.Err, jcode.ErrBridgeExited) {
			t.Fatalf("terminal kind = %q, bridge_exited=%t", result.Kind, errors.Is(result.Err, jcode.ErrBridgeExited))
		}
		harness.closeAndAssertClean(t)
		if !pathMissing(filepath.Join(harness.marksDir, "term-observed")) {
			t.Fatal("post-reap shutdown signaled the ambiguous process group")
		}
	})
}

func TestPrivateRuntimeRealOAuthSmoke(t *testing.T) {
	if os.Getenv(realOAuthSmokeEnv) != "1" {
		t.Skip("set JCODE_GO_REAL_OAUTH_SMOKE=1 to run the approved real OAuth smoke")
	}
	worktree := os.Getenv(realOAuthWorkdirEnv)
	if !filepath.IsAbs(worktree) {
		t.Fatal("phase=validate_workdir class=not_absolute")
	}
	worktree = filepath.Clean(worktree)
	started := time.Now()
	t.Log("phase=launch provider=openai model=gpt-5.6-luna")
	launchCtx, cancelLaunch := context.WithTimeout(context.Background(), 45*time.Second)
	client, err := jcode.Launch(launchCtx, jcode.LaunchOptions{
		WorkingDir:          worktree,
		Provider:            "openai",
		Model:               "gpt-5.6-luna",
		StartupTimeout:      30 * time.Second,
		ShutdownGracePeriod: 3 * time.Second,
		ShutdownReapTimeout: 3 * time.Second,
	})
	cancelLaunch()
	if err != nil {
		t.Fatalf("phase=launch class=%s", smokeErrorClass(err))
	}

	instance, detached := client.DetachInstance()
	if !detached {
		_ = client.Close()
		t.Fatal("phase=detach class=ownership_unavailable")
	}
	home := instance.JcodeHome()
	socket := instance.SocketPath()
	shutdown := func() {
		_ = client.Close()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_ = jcode.ShutdownInstance(shutdownCtx, instance)
		cancel()
	}
	defer shutdown()

	t.Log("phase=create_session")
	sessionCtx, cancelSession := context.WithTimeout(context.Background(), 15*time.Second)
	session, err := client.CreateSession(sessionCtx, jcode.CreateSessionOptions{WorkingDir: worktree})
	cancelSession()
	if err != nil {
		t.Fatalf("phase=create_session class=%s", smokeErrorClass(err))
	}
	t.Log("phase=session_created")

	t.Log("phase=turn_start")
	turnCtx, cancelTurn := context.WithTimeout(context.Background(), 2*time.Minute)
	turn, err := session.StartTurn(turnCtx, "Reply with one short acknowledgement.", jcode.SendOptions{})
	if err != nil {
		cancelTurn()
		t.Fatalf("phase=turn_start class=%s", smokeErrorClass(err))
	}
	acceptedCtx, cancelAccepted := context.WithTimeout(context.Background(), 30*time.Second)
	if err := turn.Accepted(acceptedCtx); err != nil {
		cancelAccepted()
		cancelTurn()
		t.Fatalf("phase=turn_accept class=%s", smokeErrorClass(err))
	}
	cancelAccepted()
	t.Log("phase=turn_accepted")

	result, err := drainAndWaitTurn(turnCtx, turn)
	cancelTurn()
	if err != nil {
		t.Fatalf("phase=turn_wait class=%s", smokeErrorClass(err))
	}
	if result.Kind != jcode.TurnResultCompleted || result.Err != nil {
		t.Fatalf("phase=turn_terminal kind=%s class=%s", result.Kind, smokeErrorClass(result.Err))
	}
	t.Logf("phase=turn_terminal kind=%s elapsed_ms=%d", result.Kind, time.Since(started).Milliseconds())

	if err := client.Close(); err != nil && !errors.Is(err, jcode.ErrClosed) {
		t.Fatalf("phase=client_close class=%s", smokeErrorClass(err))
	}
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	err = jcode.ShutdownInstance(shutdownCtx, instance)
	cancelShutdown()
	if err != nil {
		t.Fatalf("phase=instance_shutdown class=%s", smokeErrorClass(err))
	}
	homeGone := pathMissing(home)
	socketGone := pathMissing(socket)
	t.Logf("phase=cleanup home_removed=%t socket_removed=%t elapsed_ms=%d", homeGone, socketGone, time.Since(started).Milliseconds())
	if !homeGone || !socketGone {
		t.Fatal("phase=cleanup class=owned_path_remains")
	}
}

func drainAndWaitTurn(ctx context.Context, turn *jcode.Turn) (jcode.TurnResult, error) {
	resultCh := make(chan struct {
		result jcode.TurnResult
		err    error
	}, 1)
	go func() {
		result, err := turn.Wait(ctx)
		resultCh <- struct {
			result jcode.TurnResult
			err    error
		}{result, err}
	}()
	for {
		select {
		case outcome := <-resultCh:
			return outcome.result, outcome.err
		default:
		}
		_, err := turn.Next(ctx)
		if err != nil {
			select {
			case outcome := <-resultCh:
				return outcome.result, outcome.err
			case <-ctx.Done():
				return jcode.TurnResult{}, ctx.Err()
			}
		}
	}
}

func smokeErrorClass(err error) string {
	if err == nil {
		return "none"
	}
	var launchErr *jcode.LaunchError
	switch {
	case errors.As(err, &launchErr):
		return "launch_" + string(launchErr.Code)
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, jcode.ErrBridgeExited):
		return "bridge_exited"
	case errors.Is(err, jcode.ErrDisconnected):
		return "transport_disconnected"
	case errors.Is(err, jcode.ErrClosed):
		return "client_closed"
	default:
		return "typed_unclassified"
	}
}

type acceptanceHarness struct {
	client      *jcode.Client
	marksDir    string
	triggerPath string
	home        string
	runtimeDir  string
	socketPath  string
	bridge      acceptanceProcessIdentity
	server      acceptanceProcessIdentity
	worktree    string
}

type acceptanceProcessIdentity struct {
	pid       int
	startTime string
}

func launchAcceptanceHarness(t *testing.T, worktree, mode string) *acceptanceHarness {
	t.Helper()
	marksDir := t.TempDir()
	triggerPath := filepath.Join(marksDir, "bridge-exit")
	wrapperPath := filepath.Join(marksDir, "jcode-fixture")
	testBinary, err := os.Executable()
	if err != nil {
		t.Fatalf("resolve acceptance executable: %T", err)
	}
	script := fmt.Sprintf("#!/bin/sh\nif [ \"$JCODE_GO_ACCEPTANCE_MODE\" = \"term_ignore\" ]; then trap '' TERM; fi\n\"%s\" -test.run=^TestPrivateRuntimeAcceptanceBridgeHelper$ &\nchild=$!\nprintf '%%s\\n' \"$$\" > \"$JCODE_GO_ACCEPTANCE_MARKS/bridge.pid\"\nprintf '%%s\\n' \"$child\" > \"$JCODE_GO_ACCEPTANCE_MARKS/server.pid\"\nwhile kill -0 \"$child\" 2>/dev/null; do\n  if [ -f \"$JCODE_GO_ACCEPTANCE_TRIGGER\" ]; then exit 23; fi\n  sleep 0.01\ndone\nwait \"$child\"\n", strings.ReplaceAll(testBinary, "\"", "\\\""))
	if err := os.WriteFile(wrapperPath, []byte(script), 0o700); err != nil {
		t.Fatalf("write acceptance bridge: %T", err)
	}
	unrelated := filepath.Join(marksDir, "caller-owned")
	if err := os.WriteFile(unrelated, []byte("keep"), 0o600); err != nil {
		t.Fatalf("write caller-owned marker: %T", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	client, err := jcode.Launch(ctx, jcode.LaunchOptions{
		WorkingDir: worktree,
		Binary:     wrapperPath,
		Env: map[string]string{
			acceptanceHelperEnv:  "1",
			acceptanceModeEnv:    mode,
			acceptanceMarksEnv:   marksDir,
			acceptanceTriggerEnv: triggerPath,
		},
		StartupTimeout:      3 * time.Second,
		ShutdownGracePeriod: 80 * time.Millisecond,
		ShutdownReapTimeout: time.Second,
		CleanupTimeout:      time.Second,
	})
	cancel()
	if err != nil {
		t.Fatalf("Launch failed: %T", err)
	}
	h := &acceptanceHarness{client: client, marksDir: marksDir, triggerPath: triggerPath, worktree: worktree}
	h.home = readMark(t, marksDir, "home")
	h.runtimeDir = filepath.Join(h.home, "run")
	h.socketPath = filepath.Join(h.runtimeDir, "jcode-api.sock")
	h.bridge = readProcessIdentity(t, readPIDMark(t, marksDir, "bridge.pid"))
	h.server = readProcessIdentity(t, readPIDMark(t, marksDir, "server.pid"))
	if got := filepath.Clean(readMark(t, marksDir, "cwd")); got != worktree {
		t.Fatal("private bridge did not use selected worktree")
	}
	if !filepath.IsAbs(h.home) || !strings.Contains(filepath.Base(h.home), "jcode-sdk-instance-") {
		t.Fatal("private launch did not use an absolute ephemeral home")
	}
	return h
}

func createAcceptanceSession(t *testing.T, harness *acceptanceHarness, worktree string) jcode.Session {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	session, err := harness.client.CreateSession(ctx, jcode.CreateSessionOptions{WorkingDir: worktree})
	if err != nil {
		t.Fatalf("CreateSession failed: %T", err)
	}
	if filepath.Clean(session.Info.WorkingDir) != worktree {
		t.Fatal("session did not retain selected worktree")
	}
	if got := filepath.Clean(readMark(t, harness.marksDir, "session-cwd")); got != worktree {
		t.Fatal("create_session did not send selected worktree")
	}
	return session
}

func (h *acceptanceHarness) closeAndAssertClean(t *testing.T) time.Duration {
	t.Helper()
	started := time.Now()
	if err := h.client.Close(); err != nil && !errors.Is(err, jcode.ErrClosed) {
		t.Fatalf("Client.Close failed: %T", err)
	}
	if elapsed := time.Since(started); elapsed >= 2*time.Second {
		t.Fatalf("private shutdown took %s, want bounded cleanup", elapsed)
	}
	assertProcessGone(t, "bridge", h.bridge)
	assertProcessGone(t, "descendant", h.server)
	for _, resource := range []struct {
		name string
		path string
	}{
		{name: "socket", path: h.socketPath},
		{name: "runtime directory", path: h.runtimeDir},
		{name: "ephemeral home", path: h.home},
	} {
		if !pathMissing(resource.path) {
			t.Fatalf("SDK-owned %s remains", resource.name)
		}
	}
	if _, err := os.Stat(filepath.Join(h.marksDir, "caller-owned")); err != nil {
		t.Fatal("caller-owned path was removed")
	}
	return time.Since(started)
}

func readMark(t *testing.T, dir, name string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(path)
		if err == nil {
			return strings.TrimSpace(string(data))
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for fixture mark %s", name)
	return ""
}

func readPIDMark(t *testing.T, dir, name string) int {
	t.Helper()
	pid, err := strconv.Atoi(readMark(t, dir, name))
	if err != nil || pid <= 1 {
		t.Fatalf("invalid fixture PID mark %s", name)
	}
	return pid
}

func readProcessIdentity(t *testing.T, pid int) acceptanceProcessIdentity {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		_, startTime, err := readProcessStat(pid)
		if err == nil {
			return acceptanceProcessIdentity{pid: pid, startTime: startTime}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out recording fixture process identity")
	return acceptanceProcessIdentity{}
}

func assertProcessGone(t *testing.T, name string, identity acceptanceProcessIdentity) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if !processIdentityAlive(identity) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("SDK-owned %s process remains", name)
}

func processIdentityAlive(identity acceptanceProcessIdentity) bool {
	state, startTime, err := readProcessStat(identity.pid)
	return err == nil && startTime == identity.startTime && state != "Z"
}

func readProcessStat(pid int) (state, startTime string, err error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return "", "", err
	}
	end := strings.LastIndexByte(string(data), ')')
	if end < 0 {
		return "", "", errors.New("malformed process stat")
	}
	fields := strings.Fields(string(data)[end+1:])
	if len(fields) <= 19 {
		return "", "", errors.New("short process stat")
	}
	return fields[0], fields[19], nil
}

func pathMissing(path string) bool {
	_, err := os.Lstat(path)
	return os.IsNotExist(err)
}

func TestPrivateRuntimeAcceptanceBridgeHelper(t *testing.T) {
	if os.Getenv(acceptanceHelperEnv) != "1" {
		return
	}
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("resolve fixture working directory: %T", err)
	}
	marksDir := os.Getenv(acceptanceMarksEnv)
	writeHelperMark(t, marksDir, "cwd", cwd)
	writeHelperMark(t, marksDir, "home", os.Getenv("JCODE_HOME"))

	recordAndIgnoreTERM(marksDir)
	if os.Getenv(acceptanceModeEnv) == "bridge_exit" {
		go exitAfterTrigger(os.Getenv(acceptanceTriggerEnv))
	}
	socketPath := os.Getenv("JCODE_API_SOCKET")
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("listen on fixture socket: %T", err)
	}
	defer listener.Close()
	for {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		err = serveAcceptanceConnection(conn, os.Getenv(acceptanceModeEnv))
		_ = conn.Close()
		if err == nil {
			return
		}
		if !errors.Is(err, io.EOF) {
			t.Fatalf("serve fixture connection: %T", err)
		}
	}
}

func recordAndIgnoreTERM(marksDir string) {
	term := make(chan os.Signal, 1)
	signal.Notify(term, syscall.SIGTERM)
	go func() {
		for range term {
			_ = os.WriteFile(filepath.Join(marksDir, "term-observed"), []byte("observed\n"), 0o600)
		}
	}()
}

func exitAfterTrigger(triggerPath string) {
	for {
		if _, err := os.Stat(triggerPath); err == nil {
			time.Sleep(250 * time.Millisecond)
			os.Exit(0)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func writeHelperMark(t *testing.T, dir, name, value string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(value+"\n"), 0o600); err != nil {
		t.Fatalf("write fixture marker: %T", err)
	}
}

func serveAcceptanceConnection(conn net.Conn, mode string) error {
	decoder := protocol.NewDecoder(conn)
	encoder := protocol.NewEncoder(conn)
	const sessionID = "acceptance_session"
	for {
		data, err := decoder.ReadFrame()
		if err != nil {
			return err
		}
		frame, err := decodeClientFrame(data)
		if err != nil {
			return err
		}
		switch frame.Request.Req {
		case "hello":
			if err := sendAcceptanceFrame(encoder, frame.ID, "hello_ok", map[string]any{"version": 1, "server": "acceptance"}); err != nil {
				return err
			}
		case "create_session":
			var fields struct {
				WorkingDir string `json:"working_dir"`
			}
			if err := json.Unmarshal(frame.Request.Fields, &fields); err != nil {
				return err
			}
			if err := os.WriteFile(filepath.Join(os.Getenv(acceptanceMarksEnv), "session-cwd"), []byte(fields.WorkingDir+"\n"), 0o600); err != nil {
				return err
			}
			if err := sendAcceptanceFrame(encoder, frame.ID, "sessions", map[string]any{"session": map[string]any{
				"session_id": sessionID, "working_dir": fields.WorkingDir, "status": "idle",
			}}); err != nil {
				return err
			}
		case "send_message":
			if err := sendAcceptanceEvent(encoder, "message_accepted", map[string]any{"session_id": sessionID}); err != nil {
				return err
			}
			if mode == "complete" {
				return sendAcceptanceEvent(encoder, "turn_done", map[string]any{"session_id": sessionID})
			}
		case "cancel":
			if err := sendAcceptanceFrame(encoder, frame.ID, "ok", nil); err != nil {
				return err
			}
			if err := sendAcceptanceEvent(encoder, "turn_done", map[string]any{"session_id": sessionID}); err != nil {
				return err
			}
			if mode != "term_ignore" {
				return nil
			}
		}
	}
}

func decodeClientFrame(data []byte) (protocol.ClientFrame, error) {
	var wire map[string]json.RawMessage
	if err := json.Unmarshal(data, &wire); err != nil {
		return protocol.ClientFrame{}, err
	}
	var frame protocol.ClientFrame
	if err := json.Unmarshal(wire["v"], &frame.V); err != nil {
		return frame, err
	}
	if err := json.Unmarshal(wire["id"], &frame.ID); err != nil {
		return frame, err
	}
	if err := json.Unmarshal(wire["req"], &frame.Request.Req); err != nil {
		return frame, err
	}
	delete(wire, "v")
	delete(wire, "id")
	delete(wire, "req")
	fields, err := json.Marshal(wire)
	frame.Request.Fields = fields
	return frame, err
}

func sendAcceptanceFrame(encoder *protocol.Encoder, replyTo uint64, kind string, fields map[string]any) error {
	data, err := protocol.EncodeServerFrame(1, &replyTo, kind, fields)
	if err != nil {
		return err
	}
	var object map[string]any
	if err := json.Unmarshal(data, &object); err != nil {
		return err
	}
	return encoder.Write(object)
}

func sendAcceptanceEvent(encoder *protocol.Encoder, kind string, fields map[string]any) error {
	data, err := protocol.EncodeServerFrame(1, nil, kind, fields)
	if err != nil {
		return err
	}
	var object map[string]any
	if err := json.Unmarshal(data, &object); err != nil {
		return err
	}
	return encoder.Write(object)
}
