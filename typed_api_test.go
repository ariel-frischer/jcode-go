package jcode

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/ariel-frischer/jcode-go/protocol"
	"github.com/ariel-frischer/jcode-go/transport"
)

func TestTypedSessionAndEventSurface(t *testing.T) {
	clientSide, serverSide := transport.NewPipePair()
	server := transport.NewFakeServer(serverSide)
	defer server.Close()
	serverDone := make(chan error, 1)
	go func() {
		receive := func() (protocol.ClientFrame, error) {
			data, err := server.Receive()
			if err != nil {
				return protocol.ClientFrame{}, err
			}
			var frame protocol.ClientFrame
			if err := json.Unmarshal(data, &frame); err != nil {
				return protocol.ClientFrame{}, err
			}
			return frame, nil
		}
		hello, err := receive()
		if err != nil {
			serverDone <- err
			return
		}
		if err := server.Send(mustServerFrame(t, hello.ID, "hello_ok", map[string]any{"version": 1})); err != nil {
			serverDone <- err
			return
		}
		create, err := receive()
		if err != nil {
			serverDone <- err
			return
		}
		if err := server.Send(mustServerFrame(t, create.ID, "attached", map[string]any{
			"session": map[string]any{"session_id": "session_test", "working_dir": "/tmp", "status": "active"},
		})); err != nil {
			serverDone <- err
			return
		}
		send, err := receive()
		if err != nil {
			serverDone <- err
			return
		}
		if err := server.Send(mustServerFrame(t, send.ID, "ok", nil)); err != nil {
			serverDone <- err
			return
		}
		if err := server.Send(mustEventFrame(t, "text_delta", map[string]any{"session_id": "session_test", "text": "hello"})); err != nil {
			serverDone <- err
			return
		}
		serverDone <- server.Send(mustEventFrame(t, "turn_done", map[string]any{"session_id": "session_test"}))
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	client, err := NewClient(ctx, clientSide, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	session, err := client.CreateSession(ctx, CreateSessionOptions{WorkingDir: "/tmp"})
	if err != nil {
		t.Fatal(err)
	}
	if session.ID != "session_test" || session.Info.WorkingDir != "/tmp" {
		t.Fatalf("session=%+v", session)
	}
	stream := session.Events(ctx)
	if err := session.Send(ctx, "hello", SendOptions{}); err != nil {
		t.Fatal(err)
	}
	event, err := stream.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	text, ok := event.(*TextDelta)
	if !ok || text.Text != "hello" || text.SessionID != session.ID {
		t.Fatalf("event=%T %+v", event, event)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
}

func mustEventFrame(t *testing.T, kind string, fields map[string]any) []byte {
	t.Helper()
	data, err := protocol.EncodeServerFrame(1, nil, kind, fields)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
