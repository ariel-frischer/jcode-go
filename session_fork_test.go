package jcode

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/ariel-frischer/jcode-go/protocol"
	"github.com/ariel-frischer/jcode-go/transport"
)

func TestForkSessionSuccessSendsExactPayload(t *testing.T) {
	client, server := newForkSessionFixture(t, Options{})
	serverDone := make(chan error, 1)
	go func() {
		data, err := server.Receive()
		if err != nil {
			serverDone <- err
			return
		}
		var wire map[string]json.RawMessage
		if err := json.Unmarshal(data, &wire); err != nil {
			serverDone <- err
			return
		}
		if len(wire) != 4 {
			serverDone <- errors.New("fork_session request contains unexpected fields")
			return
		}
		var request struct {
			ID        uint64 `json:"id"`
			Req       string `json:"req"`
			SessionID string `json:"session_id"`
		}
		if err := json.Unmarshal(data, &request); err != nil {
			serverDone <- err
			return
		}
		if request.Req != "fork_session" || request.SessionID != "session_source" {
			serverDone <- errors.New("fork_session request has incorrect wire fields")
			return
		}
		serverDone <- server.Send(mustServerFrame(t, request.ID, "session_forked", map[string]any{
			"session": map[string]any{
				"session_id":       "session_forked",
				"working_dir":      "/worktree",
				"title":            "Forked session",
				"status":           "idle",
				"transcript_bytes": 42,
			},
		}))
	}()

	session, err := client.ForkSession(context.Background(), "session_source")
	if err != nil {
		t.Fatal(err)
	}
	if session.ID != "session_forked" || session.Info.WorkingDir != "/worktree" || session.Info.Title != "Forked session" || session.Info.Status != "idle" || session.Info.TranscriptBytes != 42 {
		t.Fatalf("session=%+v, want complete forked session metadata", session)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
}

func TestForkSessionRejectsMalformedReply(t *testing.T) {
	client, server := newForkSessionFixture(t, Options{})
	go replyToForkRequest(t, server, "session_forked", map[string]any{"session": "not-an-object"})

	if _, err := client.ForkSession(context.Background(), "session_source"); err == nil {
		t.Fatal("ForkSession error=nil, want malformed reply error")
	}
}

func TestForkSessionReturnsServerError(t *testing.T) {
	client, server := newForkSessionFixture(t, Options{})
	go replyToForkRequest(t, server, "error", map[string]any{
		"code":          "unknown_session",
		"message":       "source session does not exist",
		"provider_code": "not_found",
	})

	_, err := client.ForkSession(context.Background(), "session_source")
	var eventErr EventError
	if !errors.As(err, &eventErr) {
		t.Fatalf("ForkSession error=%v, want EventError", err)
	}
	if eventErr.Code != "unknown_session" || eventErr.Message != "source session does not exist" || eventErr.ProviderCode != "not_found" {
		t.Fatalf("ForkSession error=%+v, want preserved server fields", eventErr)
	}
}

func TestForkSessionHonorsCanceledContext(t *testing.T) {
	client, server := newForkSessionFixture(t, Options{})
	serverDone := make(chan error, 1)
	go func() {
		_, err := server.Receive()
		serverDone <- err
	}()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := client.ForkSession(ctx, "session_source"); !errors.Is(err, context.Canceled) {
		t.Fatalf("ForkSession error=%v, want context canceled", err)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
}

func TestForkSessionTimesOutWhenBridgeNeverReplies(t *testing.T) {
	client, server := newForkSessionFixture(t, Options{RequestTimeout: 20 * time.Millisecond})
	serverDone := make(chan error, 1)
	go func() {
		_, err := server.Receive()
		serverDone <- err
	}()

	started := time.Now()
	_, err := client.ForkSession(context.Background(), "session_source")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ForkSession error=%v, want context deadline exceeded", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("ForkSession took %s, want bounded timeout", elapsed)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
}

func TestDecodeTypedSessionForkedAndWakeRequested(t *testing.T) {
	forked, err := decodeTypedEvent(Event{
		Kind:   "session_forked",
		Fields: json.RawMessage(`{"session":{"session_id":"session_forked","working_dir":"/worktree","title":"Fork","status":"idle","transcript_bytes":42,"archived":true,"archived_at_ms":99}}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	forkedValue, ok := forked.(*SessionForked)
	if !ok || forkedValue.Session.ID != "session_forked" || forkedValue.Session.WorkingDir != "/worktree" || forkedValue.Session.Title != "Fork" || forkedValue.Session.Status != "idle" || forkedValue.Session.TranscriptBytes != 42 || !forkedValue.Session.Archived || forkedValue.Session.ArchivedAtMS != 99 {
		t.Fatalf("event=%#v, want SessionForked with all fields preserved", forked)
	}

	wake, err := decodeTypedEvent(Event{
		Kind:   "wake_requested",
		Fields: json.RawMessage(`{"session_id":"session_wake","reason":"background task completed","notification":"Tests finished"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	wakeValue, ok := wake.(*WakeRequested)
	if !ok || wakeValue.SessionID != "session_wake" || wakeValue.Reason != "background task completed" || wakeValue.Notification != "Tests finished" {
		t.Fatalf("event=%#v, want WakeRequested with all fields preserved", wake)
	}
}

func newForkSessionFixture(t *testing.T, options Options) (*Client, *transport.FakeServer) {
	t.Helper()
	clientSide, serverSide := transport.NewPipePair()
	server := transport.NewFakeServer(serverSide)
	handshakeDone := make(chan error, 1)
	go func() {
		data, err := server.Receive()
		if err != nil {
			handshakeDone <- err
			return
		}
		var request protocol.ClientFrame
		if err := json.Unmarshal(data, &request); err != nil {
			handshakeDone <- err
			return
		}
		handshakeDone <- server.Send(mustServerFrame(t, request.ID, "hello_ok", map[string]any{"version": 1}))
	}()
	client, err := NewClient(context.Background(), clientSide, options)
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	if err := <-handshakeDone; err != nil {
		client.Close()
		server.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		client.Close()
		server.Close()
	})
	return client, server
}

func replyToForkRequest(t *testing.T, server *transport.FakeServer, kind string, fields map[string]any) {
	t.Helper()
	data, err := server.Receive()
	if err != nil {
		return
	}
	var request protocol.ClientFrame
	if err := json.Unmarshal(data, &request); err != nil {
		return
	}
	_ = server.Send(mustServerFrame(t, request.ID, kind, fields))
}
