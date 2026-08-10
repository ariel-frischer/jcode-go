package jcode

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ariel-frischer/jcode-go/protocol"
)

func TestTurnResultKindsAreStable(t *testing.T) {
	want := map[TurnResultKind]string{
		TurnResultCompleted:                 "completed",
		TurnResultCanceled:                  "canceled",
		TurnResultLifecycleCanceled:         "lifecycle_canceled",
		TurnResultLifecycleDeadlineExceeded: "lifecycle_deadline_exceeded",
		TurnResultProviderError:             "provider_error",
		TurnResultProtocolError:             "protocol_error",
		TurnResultSubscriberOverflow:        "subscriber_overflow",
		TurnResultBridgeExited:              "bridge_exited",
		TurnResultTransportDisconnected:     "transport_disconnected",
		TurnResultClientClosed:              "client_closed",
	}
	if len(want) != 10 {
		t.Fatalf("terminal taxonomy has %d entries, want 10", len(want))
	}
	for kind, value := range want {
		if string(kind) != value {
			t.Errorf("kind %q = %q, want %q", kind, kind, value)
		}
	}
}

func TestTurnFirstTerminalResultWinsImmutably(t *testing.T) {
	client, server := newTurnTestClient(t, Options{})
	defer client.Close()
	defer server.Close()

	turn := newTurn(client, "session_turn", client.subscribe("session_turn", 1))
	start := make(chan struct{})
	results := []TurnResult{
		{Kind: TurnResultCompleted},
		{Kind: TurnResultCanceled, Err: ErrTurnCanceled},
		{Kind: TurnResultLifecycleCanceled, Err: context.Canceled},
		{Kind: TurnResultLifecycleDeadlineExceeded, Err: context.DeadlineExceeded},
		{Kind: TurnResultProviderError, Err: EventError{Code: "provider_failed"}},
		{Kind: TurnResultProtocolError, Err: ErrProtocolFailure},
		{Kind: TurnResultSubscriberOverflow, Err: ErrSubscriberOverflow},
		{Kind: TurnResultTransportDisconnected, Err: ErrDisconnected},
		{Kind: TurnResultBridgeExited, Err: ErrBridgeExited},
		{Kind: TurnResultClientClosed, Err: ErrClosed},
	}
	var wg sync.WaitGroup
	for _, result := range results {
		result := result
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			turn.finishTerminal(result)
		}()
	}
	close(start)
	wg.Wait()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	first, err := turn.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	matched := false
	for _, result := range results {
		if reflect.DeepEqual(first, result) {
			matched = true
			break
		}
	}
	if !matched {
		t.Fatalf("first result %+v does not match a competing terminal result", first)
	}
	for _, result := range results {
		turn.finishTerminal(result)
	}
	again, err := turn.Wait(ctx)
	if err != nil || !reflect.DeepEqual(again, first) {
		t.Fatalf("later Wait = %+v, %v, want immutable %+v", again, err, first)
	}
}

func TestTurnTransportDisconnectTerminatesTurnButNotRawSubscription(t *testing.T) {
	client, server := newTurnTestClient(t, Options{})
	defer client.Close()

	raw := client.Subscribe("session_turn")
	defer raw.Close()
	requestSeen := make(chan struct{})
	go func() {
		_, _ = receiveTurnRequest(server)
		close(requestSeen)
		_ = server.Close()
	}()
	turn, err := (Session{client: client, ID: "session_turn"}).StartTurn(context.Background(), "prompt", SendOptions{})
	if err != nil {
		t.Fatal(err)
	}
	<-requestSeen

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := turn.Accepted(ctx); !errors.Is(err, ErrDisconnected) {
		t.Fatalf("Accepted = %v, want transport disconnect", err)
	}
	result, err := turn.Wait(ctx)
	if err != nil || result.Kind != TurnResultTransportDisconnected || !errors.Is(result.Err, ErrDisconnected) {
		t.Fatalf("Wait = %+v, %v, want transport disconnect", result, err)
	}
	shortCtx, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer stop()
	if _, err := raw.Next(shortCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("raw subscription error = %v, want caller deadline while reconnect remains explicit", err)
	}
	if _, err := turn.Next(ctx); !errors.Is(err, ErrDisconnected) {
		t.Fatalf("Next = %v, want transport disconnect", err)
	}
}

func TestTurnTransportFailureRedactsDiagnosticText(t *testing.T) {
	client, server := newTurnTestClient(t, Options{})
	defer client.Close()
	defer server.Close()

	go func() { _, _ = receiveTurnRequest(server) }()
	turn, err := (Session{client: client, ID: "session_turn"}).StartTurn(context.Background(), "prompt", SendOptions{})
	if err != nil {
		t.Fatal(err)
	}
	const unsafe = "credential-value /private/runtime.sock prompt-content"
	client.disconnect(errors.New(unsafe), false)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := turn.Wait(ctx)
	if err != nil || result.Kind != TurnResultTransportDisconnected || !errors.Is(result.Err, ErrDisconnected) {
		t.Fatalf("Wait = %+v, %v, want transport disconnect", result, err)
	}
	if strings.Contains(result.Err.Error(), unsafe) {
		t.Fatal("owned Turn retained unsafe transport diagnostic")
	}
}

func TestTurnProtocolFailureIsTypedAndSanitized(t *testing.T) {
	client, server := newTurnTestClient(t, Options{})
	defer client.Close()
	defer server.Close()

	go func() {
		_, _ = receiveTurnRequest(server)
		_ = server.Send([]byte(`{"v":1,"ev":`))
	}()
	turn, err := (Session{client: client, ID: "session_turn"}).StartTurn(context.Background(), "prompt", SendOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := turn.Accepted(ctx); !errors.Is(err, ErrProtocolFailure) {
		t.Fatalf("Accepted = %v, want protocol failure", err)
	}
	result, err := turn.Wait(ctx)
	if err != nil || result.Kind != TurnResultProtocolError {
		t.Fatalf("Wait = %+v, %v, want protocol failure", result, err)
	}
	if !errors.Is(result.Err, ErrProtocolFailure) || !errors.Is(result.Err, protocol.ErrMalformedFrame) {
		t.Fatalf("protocol cause = %v, want stable and framing sentinels", result.Err)
	}
}

func TestTurnProtocolFailureClasses(t *testing.T) {
	tests := []struct {
		name     string
		options  Options
		frame    []byte
		sentinel error
	}{
		{name: "malformed", frame: []byte(`{"v":1,"ev":`), sentinel: protocol.ErrMalformedFrame},
		{name: "invalid", frame: []byte(`{"v":1}`), sentinel: protocol.ErrInvalidFrame},
		{name: "oversized", options: Options{MaxFrameSize: 256}, frame: bytes.Repeat([]byte("x"), 257), sentinel: protocol.ErrFrameTooLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, server := newTurnTestClient(t, tt.options)
			defer client.Close()
			defer server.Close()
			go func() {
				_, _ = receiveTurnRequest(server)
				_ = server.Send(tt.frame)
			}()
			turn, err := (Session{client: client, ID: "session_turn"}).StartTurn(context.Background(), "prompt", SendOptions{})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			result, err := turn.Wait(ctx)
			if err != nil || result.Kind != TurnResultProtocolError {
				t.Fatalf("Wait = %+v, %v, want protocol failure", result, err)
			}
			if !errors.Is(result.Err, ErrProtocolFailure) || !errors.Is(result.Err, tt.sentinel) {
				t.Fatalf("protocol cause = %v, want %v", result.Err, tt.sentinel)
			}
		})
	}
}

func TestTurnTypedEventDecodeFailureIsProtocolFailure(t *testing.T) {
	client, server := newTurnTestClient(t, Options{})
	defer client.Close()
	defer server.Close()

	go func() {
		_, _ = receiveTurnRequest(server)
		_ = server.Send(mustEventFrame(t, "text_delta", map[string]any{
			"session_id": "session_turn",
			"text":       map[string]any{"unsafe": "provider response"},
		}))
	}()
	turn, err := (Session{client: client, ID: "session_turn"}).StartTurn(context.Background(), "prompt", SendOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := turn.Accepted(ctx); !errors.Is(err, ErrProtocolFailure) {
		t.Fatalf("Accepted = %v, want protocol failure", err)
	}
	result, err := turn.Wait(ctx)
	if err != nil || result.Kind != TurnResultProtocolError || !errors.Is(result.Err, ErrProtocolFailure) {
		t.Fatalf("Wait = %+v, %v, want typed decode protocol failure", result, err)
	}
}

func TestTurnClientCloseIsTyped(t *testing.T) {
	client, server := newTurnTestClient(t, Options{})
	defer server.Close()

	requestSeen := make(chan struct{})
	go func() {
		_, _ = receiveTurnRequest(server)
		close(requestSeen)
	}()
	turn, err := (Session{client: client, ID: "session_turn"}).StartTurn(context.Background(), "prompt", SendOptions{})
	if err != nil {
		t.Fatal(err)
	}
	<-requestSeen
	if err := client.Close(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Close = %v, want ErrClosed", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := turn.Wait(ctx)
	if err != nil || result.Kind != TurnResultClientClosed || !errors.Is(result.Err, ErrClosed) {
		t.Fatalf("Wait = %+v, %v, want client close", result, err)
	}
}

func TestTurnConcurrentClientCloseIsStable(t *testing.T) {
	client, server := newTurnTestClient(t, Options{})
	defer server.Close()

	go func() { _, _ = receiveTurnRequest(server) }()
	turn, err := (Session{client: client, ID: "session_turn"}).StartTurn(context.Background(), "prompt", SendOptions{})
	if err != nil {
		t.Fatal(err)
	}
	const callers = 8
	start := make(chan struct{})
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs <- client.Close()
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("Close = %v, want ErrClosed", err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := turn.Wait(ctx)
	if err != nil || result.Kind != TurnResultClientClosed || !errors.Is(result.Err, ErrClosed) {
		t.Fatalf("Wait = %+v, %v, want client close", result, err)
	}
}

type bridgeExitInstance struct {
	exited    chan struct{}
	shutdowns int
	mu        sync.Mutex
}

func (i *bridgeExitInstance) SocketPath() string { return "" }
func (i *bridgeExitInstance) JcodeHome() string  { return "" }
func (i *bridgeExitInstance) bridgeExited() <-chan struct{} {
	return i.exited
}
func (i *bridgeExitInstance) Shutdown() error {
	i.mu.Lock()
	i.shutdowns++
	i.mu.Unlock()
	return nil
}
func (i *bridgeExitInstance) Close() error { return i.Shutdown() }

func TestTurnAttachedBridgeExitIsTyped(t *testing.T) {
	client, server := newTurnTestClient(t, Options{})
	defer client.Close()
	defer server.Close()

	instance := &bridgeExitInstance{exited: make(chan struct{})}
	client.setInstance(instance)
	go func() { _, _ = receiveTurnRequest(server) }()
	turn, err := (Session{client: client, ID: "session_turn"}).StartTurn(context.Background(), "prompt", SendOptions{})
	if err != nil {
		t.Fatal(err)
	}
	close(instance.exited)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := turn.Accepted(ctx); !errors.Is(err, ErrBridgeExited) {
		t.Fatalf("Accepted = %v, want bridge exit", err)
	}
	result, err := turn.Wait(ctx)
	if err != nil || result.Kind != TurnResultBridgeExited || !errors.Is(result.Err, ErrBridgeExited) {
		t.Fatalf("Wait = %+v, %v, want bridge exit", result, err)
	}
	if _, err := turn.Next(ctx); !errors.Is(err, ErrBridgeExited) {
		t.Fatalf("Next = %v, want bridge exit", err)
	}
}

func TestDetachedBridgeExitDoesNotTerminateTurn(t *testing.T) {
	client, server := newTurnTestClient(t, Options{})
	defer client.Close()
	defer server.Close()

	instance := &bridgeExitInstance{exited: make(chan struct{})}
	client.setInstance(instance)
	monitorDone := client.bridgeMonitor.done
	finish := make(chan struct{})
	go func() {
		_, _ = receiveTurnRequest(server)
		<-finish
		_ = server.Send(mustEventFrame(t, "turn_done", map[string]any{"session_id": "session_turn"}))
	}()
	turn, err := (Session{client: client, ID: "session_turn"}).StartTurn(context.Background(), "prompt", SendOptions{})
	if err != nil {
		t.Fatal(err)
	}
	detached, ok := client.DetachInstance()
	if !ok || detached != instance {
		t.Fatalf("DetachInstance = (%v, %v), want attached instance", detached, ok)
	}
	select {
	case <-monitorDone:
	case <-time.After(time.Second):
		t.Fatal("detached bridge monitor did not exit promptly")
	}
	close(instance.exited)
	shortCtx, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer stop()
	if result, err := turn.Wait(shortCtx); !errors.Is(err, context.DeadlineExceeded) || result != (TurnResult{}) {
		t.Fatalf("detached bridge exit terminated turn: %+v, %v", result, err)
	}
	close(finish)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := turn.Wait(ctx)
	if err != nil || result.Kind != TurnResultCompleted || result.Err != nil {
		t.Fatalf("Wait = %+v, %v, want completion", result, err)
	}
}

func TestReplacedBridgeExitMonitorDoesNotTerminateTurn(t *testing.T) {
	client, server := newTurnTestClient(t, Options{})
	defer client.Close()
	defer server.Close()

	oldInstance := &bridgeExitInstance{exited: make(chan struct{})}
	replacement := &bridgeExitInstance{exited: make(chan struct{})}
	client.setInstance(oldInstance)
	oldMonitorDone := client.bridgeMonitor.done
	finish := make(chan struct{})
	go func() {
		_, _ = receiveTurnRequest(server)
		<-finish
		_ = server.Send(mustEventFrame(t, "turn_done", map[string]any{"session_id": "session_turn"}))
	}()
	turn, err := (Session{client: client, ID: "session_turn"}).StartTurn(context.Background(), "prompt", SendOptions{})
	if err != nil {
		t.Fatal(err)
	}

	client.setInstance(replacement)
	select {
	case <-oldMonitorDone:
	case <-time.After(time.Second):
		t.Fatal("replaced bridge monitor did not exit promptly")
	}
	close(oldInstance.exited)
	shortCtx, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
	if result, waitErr := turn.Wait(shortCtx); !errors.Is(waitErr, context.DeadlineExceeded) || result != (TurnResult{}) {
		stop()
		t.Fatalf("replaced bridge exit terminated turn: %+v, %v", result, waitErr)
	}
	stop()

	close(finish)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := turn.Wait(ctx)
	if err != nil || result.Kind != TurnResultCompleted || result.Err != nil {
		t.Fatalf("Wait = %+v, %v, want completion", result, err)
	}
}

func TestBridgeExitFailureSerializesWithDetach(t *testing.T) {
	client, server := newTurnTestClient(t, Options{})
	defer client.Close()
	defer server.Close()

	instance := &bridgeExitInstance{exited: make(chan struct{})}
	client.setInstance(instance)
	go func() { _, _ = receiveTurnRequest(server) }()
	turn, err := (Session{client: client, ID: "session_turn"}).StartTurn(context.Background(), "prompt", SendOptions{})
	if err != nil {
		t.Fatal(err)
	}

	client.subsMu.Lock()
	subsLocked := true
	defer func() {
		if subsLocked {
			client.subsMu.Unlock()
		}
	}()
	close(instance.exited)
	deadline := time.Now().Add(time.Second)
	for {
		if !client.stateMu.TryLock() {
			break
		}
		client.stateMu.Unlock()
		if time.Now().After(deadline) {
			t.Fatal("bridge monitor did not retain the client state lock while failing turn subscribers")
		}
		runtime.Gosched()
	}

	detached := make(chan struct{})
	go func() {
		_, _ = client.DetachInstance()
		close(detached)
	}()
	select {
	case <-detached:
		t.Fatal("DetachInstance completed while bridge failure was validating the attachment")
	case <-time.After(20 * time.Millisecond):
	}

	client.subsMu.Unlock()
	subsLocked = false
	select {
	case <-detached:
	case <-time.After(time.Second):
		t.Fatal("DetachInstance did not finish after bridge failure completed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := turn.Wait(ctx)
	if err != nil || result.Kind != TurnResultBridgeExited || !errors.Is(result.Err, ErrBridgeExited) {
		t.Fatalf("Wait = %+v, %v, want serialized bridge exit", result, err)
	}
}
