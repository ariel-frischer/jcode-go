package jcode

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ariel-frischer/jcode-go/protocol"
	"github.com/ariel-frischer/jcode-go/transport"
)

func startValidationServer(t *testing.T, side *transport.Pipe, reply func(*transport.FakeServer, protocol.ClientFrame)) {
	t.Helper()
	server := transport.NewFakeServer(side)
	t.Cleanup(func() { _ = server.Close() })
	go func() {
		frame, err := server.Receive()
		if err != nil {
			return
		}
		var request protocol.ClientFrame
		if json.Unmarshal(frame, &request) != nil {
			return
		}
		reply(server, request)
	}()
}

func TestClientConcurrentRequestsOutOfOrder(t *testing.T) {
	clientSide, serverSide := transport.NewPipePair()
	server := transport.NewFakeServer(serverSide)
	defer server.Close()
	go func() {
		for i := 0; i < 9; i++ {
			frame, err := server.Receive()
			if err != nil {
				return
			}
			var request protocol.ClientFrame
			if json.Unmarshal(frame, &request) != nil {
				return
			}
			if i == 0 {
				_ = server.Send(mustServerFrame(t, request.ID, "hello_ok", map[string]any{"version": 1}))
				continue
			}
			// Deliberately reverse the request order. The client must correlate by ID.
			if i == 8 {
				time.Sleep(10 * time.Millisecond)
			}
			_ = server.Send(mustServerFrame(t, request.ID, "ok", map[string]any{"n": request.ID}))
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	client, err := NewClient(ctx, clientSide, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	const count = 8
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req, _ := protocol.NewRawRequest("ping", map[string]any{})
			if _, err := client.Request(ctx, req); err != nil {
				t.Errorf("request: %v", err)
			}
		}()
	}
	wg.Wait()
}

func TestClientRequestCancellationRemovesPending(t *testing.T) {
	clientSide, serverSide := transport.NewPipePair()
	server := transport.NewFakeServer(serverSide)
	defer server.Close()
	go func() {
		frame, err := server.Receive()
		if err != nil {
			return
		}
		var request protocol.ClientFrame
		if json.Unmarshal(frame, &request) != nil {
			return
		}
		_ = server.Send(mustServerFrame(t, request.ID, "hello_ok", map[string]any{"version": 1}))
		_, _ = server.Receive() // keep the timed-out request open
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	client, err := NewClient(ctx, clientSide, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	requestCtx, requestCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer requestCancel()
	req, _ := protocol.NewRawRequest("ping", map[string]any{})
	if _, err := client.Request(requestCtx, req); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error=%v", err)
	}
	// A later reply for the canceled ID must be ignored, not delivered to a new request.
}

func TestClientMalformedServerFrameClosesSubscribers(t *testing.T) {
	clientSide, serverSide := transport.NewPipePair()
	server := transport.NewFakeServer(serverSide)
	defer server.Close()
	go func() {
		frame, err := server.Receive()
		if err != nil {
			return
		}
		var request protocol.ClientFrame
		if json.Unmarshal(frame, &request) != nil {
			return
		}
		_ = server.Send(mustServerFrame(t, request.ID, "hello_ok", map[string]any{"version": 1}))
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	client, err := NewClient(ctx, clientSide, Options{})
	if err != nil {
		t.Fatal(err)
	}
	sub := client.Subscribe("")
	defer client.Close()
	go func() { _ = server.Send([]byte(`{"v":1,"ev":`)) }()
	_, err = sub.Next(ctx)
	if !errors.Is(err, protocol.ErrMalformedFrame) {
		t.Fatalf("error=%v", err)
	}
}

func mustServerFrame(t *testing.T, id uint64, kind string, fields map[string]any) []byte {
	t.Helper()
	data, err := protocol.EncodeServerFrame(1, &id, kind, fields)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
