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
		if err := server.Send(mustEventFrame(t, "reasoning_done", map[string]any{
			"session_id": "session_test", "duration_secs": 13.5,
		})); err != nil {
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
	var reasoningDone *ReasoningDone
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
		case *ReasoningDone:
			reasoningDone = value
		case *TextDelta:
			text = value
		case *TurnDone:
			turnDone = value
		}
	}
	if text == nil || text.Text != "hello" || text.SessionID != session.ID {
		t.Fatalf("text=%+v, want hello for session %q", text, session.ID)
	}
	if reasoningDone == nil || reasoningDone.SessionID != session.ID || reasoningDone.DurationSecs != 13.5 {
		t.Fatalf("reasoning_done=%+v, want duration 13.5 for session %q", reasoningDone, session.ID)
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

func TestDecodeTypedToolEvents(t *testing.T) {
	tests := []struct {
		name   string
		kind   string
		fields string
		check  func(*testing.T, TypedEvent)
	}{
		{
			name:   "tool start",
			kind:   "tool_start",
			fields: `{"session_id":"session_start","call_id":"call_start","name":"read"}`,
			check: func(t *testing.T, event TypedEvent) {
				value, ok := event.(*ToolStart)
				if !ok || value.SessionID != "session_start" || value.CallID != "call_start" || value.Name != "read" {
					t.Fatalf("event=%#v, want *ToolStart with exact fields", event)
				}
			},
		},
		{
			name:   "tool input delta",
			kind:   "tool_input_delta",
			fields: `{"session_id":"session_input","call_id":"call_input","delta":"{\"path\":"}`,
			check: func(t *testing.T, event TypedEvent) {
				value, ok := event.(*ToolInputDelta)
				if !ok || value.SessionID != "session_input" || value.CallID != "call_input" || value.Delta != `{"path":` {
					t.Fatalf("event=%#v, want *ToolInputDelta with exact fields", event)
				}
			},
		},
		{
			name:   "tool exec",
			kind:   "tool_exec",
			fields: `{"session_id":"session_exec","call_id":"call_exec","name":"bash"}`,
			check: func(t *testing.T, event TypedEvent) {
				value, ok := event.(*ToolExec)
				if !ok || value.SessionID != "session_exec" || value.CallID != "call_exec" || value.Name != "bash" {
					t.Fatalf("event=%#v, want *ToolExec with exact fields", event)
				}
			},
		},
		{
			name:   "tool exec empty values",
			kind:   "tool_exec",
			fields: `{"session_id":"","call_id":"","name":""}`,
			check: func(t *testing.T, event TypedEvent) {
				value, ok := event.(*ToolExec)
				if !ok || value.SessionID != "" || value.CallID != "" || value.Name != "" {
					t.Fatalf("event=%#v, want *ToolExec preserving empty fields", event)
				}
			},
		},
		{
			name:   "tool exec omitted fields",
			kind:   "tool_exec",
			fields: `{}`,
			check: func(t *testing.T, event TypedEvent) {
				value, ok := event.(*ToolExec)
				if !ok || value.SessionID != "" || value.CallID != "" || value.Name != "" {
					t.Fatalf("event=%#v, want *ToolExec preserving omitted-field zero values", event)
				}
			},
		},
		{
			name:   "tool exec extra fields",
			kind:   "tool_exec",
			fields: `{"session_id":"session_exec","call_id":"call_exec","name":"bash","future":true}`,
			check: func(t *testing.T, event TypedEvent) {
				value, ok := event.(*ToolExec)
				if !ok || value.SessionID != "session_exec" || value.CallID != "call_exec" || value.Name != "bash" {
					t.Fatalf("event=%#v, want *ToolExec ignoring extra fields", event)
				}
			},
		},
		{
			name:   "tool done",
			kind:   "tool_done",
			fields: `{"session_id":"session_done","call_id":"call_done","name":"write","output":"ok"}`,
			check: func(t *testing.T, event TypedEvent) {
				value, ok := event.(*ToolDone)
				if !ok || value.SessionID != "session_done" || value.CallID != "call_done" || value.Name != "write" || value.Output != "ok" {
					t.Fatalf("event=%#v, want *ToolDone with exact fields", event)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			event, err := decodeTypedEvent(Event{Kind: test.kind, Fields: json.RawMessage(test.fields)})
			if err != nil {
				t.Fatal(err)
			}
			test.check(t, event)
		})
	}
}

func TestDecodeTypedEventPreservesUnknownKinds(t *testing.T) {
	for _, kind := range []string{"future_event", "tool-exec", "ToolExec"} {
		t.Run(kind, func(t *testing.T) {
			fields := json.RawMessage(`{"session_id":"session_unknown","call_id":"call_unknown","name":"bash","future":true}`)
			event, err := decodeTypedEvent(Event{Kind: kind, Fields: fields})
			if err != nil {
				t.Fatal(err)
			}
			value, ok := event.(UnknownEvent)
			if !ok || value.Kind != kind || string(value.Fields) != string(fields) {
				t.Fatalf("event=%#v, want UnknownEvent preserving kind and fields", event)
			}
		})
	}
}

func TestDecodeTypedToolExecRejectsMalformedFields(t *testing.T) {
	event, err := decodeTypedEvent(Event{
		Kind:   "tool_exec",
		Fields: json.RawMessage(`{"session_id":7,"call_id":"call_exec","name":"bash"}`),
	})
	if err == nil || event != nil {
		t.Fatalf("event=%#v err=%v, want recognized tool_exec decode error", event, err)
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
