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

type observationRecorder struct {
	observations []Observation
}

func (r *observationRecorder) Observe(observation Observation) {
	r.observations = append(r.observations, observation)
}

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
		_, err = receive()
		if err != nil {
			serverDone <- err
			return
		}
		if err := server.Send(mustEventFrame(t, "message_accepted", map[string]any{"session_id": "session_test"})); err != nil {
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
	recorder := &observationRecorder{}
	client, err := NewClient(ctx, clientSide, Options{Observer: recorder})
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
	if len(recorder.observations) == 0 || recorder.observations[len(recorder.observations)-1].Kind != "create_session_ok" {
		t.Fatalf("observations=%+v, want create_session_ok", recorder.observations)
	}
	stream := session.Events(ctx)
	if err := session.Send(ctx, "hello", SendOptions{}); err != nil {
		t.Fatal(err)
	}
	var text *TextDelta
	var turnDone *TurnDone
	var sawUnknown bool
	for turnDone == nil {
		var event TypedEvent
		event, err = stream.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		switch value := event.(type) {
		case UnknownEvent:
			sawUnknown = true
		case *TextDelta:
			text = value
		case *TurnDone:
			turnDone = value
		}
	}
	if text == nil || text.Text != "hello" || text.SessionID != session.ID {
		t.Fatalf("text=%+v, want hello for session %q", text, session.ID)
	}
	if !sawUnknown {
		t.Fatal("stream did not preserve and skip the unknown message_accepted event")
	}
	if turnDone.SessionID != session.ID {
		t.Fatalf("turn_done=%+v, want session %q", turnDone, session.ID)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
}

func TestTypedEventStreamSurfacesHarnessError(t *testing.T) {
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
			"session": map[string]any{"session_id": "session_error", "working_dir": "/tmp", "status": "active"},
		})); err != nil {
			serverDone <- err
			return
		}
		if _, err := receive(); err != nil {
			serverDone <- err
			return
		}
		if err := server.Send(mustEventFrame(t, "message_accepted", map[string]any{"session_id": "session_error"})); err != nil {
			serverDone <- err
			return
		}
		serverDone <- server.Send(mustEventFrame(t, "error", map[string]any{
			"code":    "provider_error",
			"message": "provider exploded",
		}))
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
	stream := session.Events(ctx)
	if err := session.Send(ctx, "hello", SendOptions{}); err != nil {
		t.Fatal(err)
	}

	var streamErr error
	for streamErr == nil {
		_, streamErr = stream.Next(ctx)
	}
	var eventErr EventError
	if !errors.As(streamErr, &eventErr) {
		t.Fatalf("stream error=%v, want EventError", streamErr)
	}
	if eventErr.Code != "provider_error" || eventErr.Message != "provider exploded" {
		t.Fatalf("event error=%+v, want provider_error/provider exploded", eventErr)
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
