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

func TestClientCorrelatesReplyAndPublishesEvents(t *testing.T) {
	clientSide, serverSide := transport.NewPipePair()
	server := transport.NewFakeServer(serverSide)
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	serverDone := make(chan error, 1)
	go func() {
		frame, err := server.Receive()
		if err != nil {
			serverDone <- err
			return
		}
		var request protocol.ClientFrame
		if err := json.Unmarshal(frame, &request); err != nil {
			serverDone <- err
			return
		}
		hello, err := protocol.EncodeServerFrame(1, &request.ID, "hello_ok", map[string]any{"version": 1, "server": "test"})
		if err != nil {
			serverDone <- err
			return
		}
		serverDone <- server.Send(hello)
		return
	}()
	client, err := NewClient(ctx, clientSide, Options{EventBuffer: 2})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}

	sub := client.Subscribe("")
	var id uint64 = 7
	text, err := protocol.EncodeServerFrame(1, nil, "text_delta", map[string]any{"session_id": "s", "text": "hi"})
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Send(text); err != nil {
		t.Fatal(err)
	}
	event, err := sub.Next(ctx)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if event.Kind != "text_delta" {
		t.Fatalf("kind=%q", event.Kind)
	}
	var fields struct {
		SessionID string `json:"session_id"`
		Text      string `json:"text"`
	}
	if err := event.Decode(&fields); err != nil {
		t.Fatal(err)
	}
	if fields.SessionID != "s" || fields.Text != "hi" {
		t.Fatalf("fields=%+v", fields)
	}
	_ = id
}

func TestSubscriptionOverflowTerminatesOnlySubscriber(t *testing.T) {
	clientSide, serverSide := transport.NewPipePair()
	server := transport.NewFakeServer(serverSide)
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() {
		frame, err := server.Receive()
		if err != nil {
			return
		}
		var request protocol.ClientFrame
		if json.Unmarshal(frame, &request) != nil {
			return
		}
		hello, err := protocol.EncodeServerFrame(1, &request.ID, "hello_ok", map[string]any{"version": 1, "server": "test"})
		if err == nil {
			_ = server.Send(hello)
		}
	}()
	client, err := NewClient(ctx, clientSide, Options{EventBuffer: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	sub := client.Subscribe("")
	for i := 0; i < 2; i++ {
		text, err := protocol.EncodeServerFrame(1, nil, "text_delta", map[string]any{})
		if err != nil {
			t.Fatal(err)
		}
		if err := server.Send(text); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.After(time.Second)
	for {
		nextCtx, cancelNext := context.WithTimeout(ctx, 10*time.Millisecond)
		_, err := sub.Next(nextCtx)
		cancelNext()
		if errors.Is(err, ErrSubscriberOverflow) {
			break
		}
		select {
		case <-deadline:
			t.Fatal("subscription did not overflow")
		default:
		}
	}
}

func mustJSON(value any) []byte {
	data, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return data
}
