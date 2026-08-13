package jcode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ariel-frischer/jcode-go/protocol"
	"github.com/ariel-frischer/jcode-go/transport"
)

func TestTurnFastAcceptanceAndOrderedEvents(t *testing.T) {
	client, server := newTurnTestClient(t, Options{})
	defer client.Close()
	defer server.Close()

	serverDone := make(chan error, 1)
	go func() {
		request, err := receiveTurnRequest(server)
		if err != nil {
			serverDone <- err
			return
		}
		if request.Request.Req != "send_message" {
			serverDone <- fmt.Errorf("request=%q, want send_message", request.Request.Req)
			return
		}
		for _, frame := range [][]byte{
			mustEventFrame(t, "message_accepted", map[string]any{"session_id": "session_turn"}),
			mustEventFrame(t, "text_delta", map[string]any{"session_id": "session_turn", "text": "first"}),
			mustEventFrame(t, "reasoning_delta", map[string]any{"session_id": "session_turn", "text": "second"}),
			mustEventFrame(t, "turn_done", map[string]any{"session_id": "session_turn"}),
		} {
			if err := server.Send(frame); err != nil {
				serverDone <- err
				return
			}
		}
		serverDone <- nil
	}()

	beforeSubscriptions := client.nextSub.Load()
	turn, err := (Session{client: client, ID: "session_turn"}).StartTurn(context.Background(), "prompt", SendOptions{})
	if err != nil {
		t.Fatalf("StartTurn: %v", err)
	}
	if after := client.nextSub.Load(); after != beforeSubscriptions+1 {
		t.Fatalf("subscriptions advanced from %d to %d, want exactly one", beforeSubscriptions, after)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := turn.Accepted(ctx); err != nil {
		t.Fatalf("Accepted: %v", err)
	}

	var got []string
	for len(got) < 3 {
		event, err := turn.Next(ctx)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		switch value := event.(type) {
		case UnknownEvent:
			got = append(got, value.Kind)
		case *TextDelta:
			got = append(got, value.Text)
		case *ReasoningDelta:
			got = append(got, value.Text)
		case *TurnDone:
			got = append(got, "turn_done")
		default:
			t.Fatalf("unexpected event %T", event)
		}
	}
	if want := []string{"first", "second", "turn_done"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("events=%v, want %v", got, want)
	}
	result, err := turn.Wait(ctx)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if string(result.Kind) != "completed" || result.Err != nil {
		t.Fatalf("result=%+v, want completed", result)
	}
	waitResults := make(chan TurnResult, 8)
	var waiters sync.WaitGroup
	for range 8 {
		waiters.Add(1)
		go func() {
			defer waiters.Done()
			got, waitErr := turn.Wait(ctx)
			if waitErr != nil {
				t.Errorf("concurrent Wait: %v", waitErr)
				return
			}
			waitResults <- got
		}()
	}
	waiters.Wait()
	close(waitResults)
	for got := range waitResults {
		if !reflect.DeepEqual(got, result) {
			t.Fatalf("concurrent Wait result=%+v, want %+v", got, result)
		}
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
}

func TestTurnMethodContextsDoNotTerminateLifecycle(t *testing.T) {
	client, server := newTurnTestClient(t, Options{})
	defer client.Close()
	defer server.Close()

	releaseAcceptance := make(chan struct{})
	finish := make(chan struct{})
	go func() {
		_, _ = receiveTurnRequest(server)
		<-releaseAcceptance
		_ = server.Send(mustEventFrame(t, "message_accepted", map[string]any{"session_id": "session_turn"}))
		<-finish
		_ = server.Send(mustEventFrame(t, "turn_done", map[string]any{"session_id": "session_turn"}))
	}()
	turn, err := (Session{client: client, ID: "session_turn"}).StartTurn(context.Background(), "prompt", SendOptions{})
	if err != nil {
		t.Fatal(err)
	}
	shortCtx, cancelShort := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelShort()
	if err := turn.Accepted(shortCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Accepted error=%v, want deadline", err)
	}
	if _, terminal := turn.terminalResult(); terminal {
		t.Fatal("Accepted wait context terminated the Turn")
	}
	close(releaseAcceptance)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := turn.Accepted(ctx); err != nil {
		t.Fatalf("Accepted after release: %v", err)
	}
	nextCtx, cancelNext := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelNext()
	if _, err := turn.Next(nextCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Next error=%v, want deadline", err)
	}
	if _, terminal := turn.terminalResult(); terminal {
		t.Fatal("Next wait context terminated the Turn")
	}
	close(finish)
	result, err := turn.Wait(ctx)
	if err != nil || string(result.Kind) != "completed" {
		t.Fatalf("Wait result=%+v err=%v", result, err)
	}
}

func TestTurnReservesBufferSlotForTerminalEvent(t *testing.T) {
	client, server := newTurnTestClient(t, Options{EventBuffer: 1})
	defer client.Close()
	defer server.Close()

	sendTerminal := make(chan struct{})
	go func() {
		_, _ = receiveTurnRequest(server)
		_ = server.Send(mustEventFrame(t, "message_accepted", map[string]any{"session_id": "session_turn"}))
		_ = server.Send(mustEventFrame(t, "text_delta", map[string]any{"session_id": "session_turn", "text": "queued"}))
		<-sendTerminal
		_ = server.Send(mustEventFrame(t, "turn_done", map[string]any{"session_id": "session_turn"}))
	}()
	turn, err := (Session{client: client, ID: "session_turn"}).StartTurn(context.Background(), "prompt", SendOptions{})
	if err != nil {
		t.Fatal(err)
	}
	waitForTurnQueue(t, turn, 1)
	close(sendTerminal)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := turn.Wait(ctx)
	if err != nil || string(result.Kind) != "completed" {
		t.Fatalf("Wait result=%+v err=%v", result, err)
	}
	first, err := turn.Next(ctx)
	if err != nil || first.(*TextDelta).Text != "queued" {
		t.Fatalf("first event=%+v err=%v", first, err)
	}
	if event, err := turn.Next(ctx); err != nil {
		t.Fatalf("terminal Next: %v", err)
	} else if _, ok := event.(*TurnDone); !ok {
		t.Fatalf("terminal event=%T, want *TurnDone", event)
	}
}

func TestTurnEventBufferOverflowTerminatesOnlyTurn(t *testing.T) {
	client, server := newTurnTestClient(t, Options{EventBuffer: 1})
	defer client.Close()
	defer server.Close()

	sendOverflow := make(chan struct{})
	go func() {
		_, _ = receiveTurnRequest(server)
		_ = server.Send(mustEventFrame(t, "message_accepted", map[string]any{"session_id": "session_turn"}))
		_ = server.Send(mustEventFrame(t, "text_delta", map[string]any{"session_id": "session_turn", "text": "queued"}))
		<-sendOverflow
		_ = server.Send(mustEventFrame(t, "text_delta", map[string]any{"session_id": "session_turn", "text": "overflow"}))
	}()
	turn, err := (Session{client: client, ID: "session_turn"}).StartTurn(context.Background(), "prompt", SendOptions{})
	if err != nil {
		t.Fatal(err)
	}
	waitForTurnQueue(t, turn, 1)
	close(sendOverflow)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := turn.Wait(ctx)
	if err != nil || result.Kind != TurnResultSubscriberOverflow || !errors.Is(result.Err, ErrSubscriberOverflow) {
		t.Fatalf("Wait result=%+v err=%v, want subscriber overflow", result, err)
	}
	if client.State() != StateConnected {
		t.Fatalf("client state=%s, want connected", client.State())
	}
}

func TestTurnSuccessfulTerminalImpliesAcceptance(t *testing.T) {
	client, server := newTurnTestClient(t, Options{})
	defer client.Close()
	defer server.Close()

	go func() {
		_, _ = receiveTurnRequest(server)
		_ = server.Send(mustEventFrame(t, "turn_done", map[string]any{"session_id": "session_turn"}))
	}()
	turn, err := (Session{client: client, ID: "session_turn"}).StartTurn(context.Background(), "prompt", SendOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := turn.Accepted(ctx); err != nil {
		t.Fatalf("Accepted: %v", err)
	}
	result, err := turn.Wait(ctx)
	if err != nil || string(result.Kind) != "completed" {
		t.Fatalf("Wait result=%+v err=%v", result, err)
	}
}

func TestTurnCancelIsSentOnceAndShared(t *testing.T) {
	client, server := newTurnTestClient(t, Options{})
	defer client.Close()
	defer server.Close()

	allowTerminal := make(chan struct{})
	serverDone := make(chan error, 1)
	go func() {
		send, err := receiveTurnRequest(server)
		if err != nil {
			serverDone <- err
			return
		}
		if send.Request.Req != "send_message" {
			serverDone <- fmt.Errorf("request=%q, want send_message", send.Request.Req)
			return
		}
		if err := server.Send(mustEventFrame(t, "message_accepted", map[string]any{"session_id": "session_turn"})); err != nil {
			serverDone <- err
			return
		}
		cancel, err := receiveTurnRequest(server)
		if err != nil {
			serverDone <- err
			return
		}
		if cancel.Request.Req != "cancel" {
			serverDone <- fmt.Errorf("request=%q, want cancel", cancel.Request.Req)
			return
		}
		var fields struct {
			SessionID string `json:"session_id"`
		}
		if err := json.Unmarshal(cancel.Request.Fields, &fields); err != nil {
			serverDone <- err
			return
		}
		if fields.SessionID != "session_turn" {
			serverDone <- fmt.Errorf("cancel session_id=%q", fields.SessionID)
			return
		}
		if err := server.Send(mustServerFrame(t, cancel.ID, "ok", nil)); err != nil {
			serverDone <- err
			return
		}
		<-allowTerminal
		serverDone <- server.Send(mustEventFrame(t, "turn_done", map[string]any{"session_id": "session_turn"}))
	}()

	turn, err := (Session{client: client, ID: "session_turn"}).StartTurn(context.Background(), "prompt", SendOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := turn.Accepted(ctx); err != nil {
		t.Fatal(err)
	}

	const callers = 8
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- turn.Cancel(ctx)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("Cancel: %v", err)
		}
	}
	waitCtx, cancelWait := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelWait()
	if result, err := turn.Wait(waitCtx); !errors.Is(err, context.DeadlineExceeded) || result != (TurnResult{}) {
		t.Fatalf("Wait before server terminal result=%+v err=%v", result, err)
	}
	close(allowTerminal)
	result, err := turn.Wait(ctx)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if result.Kind != TurnResultCanceled || !errors.Is(result.Err, ErrTurnCanceled) {
		t.Fatalf("result=%+v, want canceled", result)
	}
	if err := turn.Cancel(ctx); err != nil {
		t.Fatalf("repeated Cancel: %v", err)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
}

func TestTurnCancelReplyImmediatelyFollowedByTerminalIsCanceled(t *testing.T) {
	client, server := newTurnTestClient(t, Options{})
	defer client.Close()
	defer server.Close()

	serverDone := make(chan error, 1)
	go func() {
		if _, err := receiveTurnRequest(server); err != nil {
			serverDone <- err
			return
		}
		if err := server.Send(mustEventFrame(t, "message_accepted", map[string]any{"session_id": "session_turn"})); err != nil {
			serverDone <- err
			return
		}
		cancel, err := receiveTurnRequest(server)
		if err != nil {
			serverDone <- err
			return
		}
		if err := server.Send(mustServerFrame(t, cancel.ID, "ok", nil)); err != nil {
			serverDone <- err
			return
		}
		serverDone <- server.Send(mustEventFrame(t, "turn_done", map[string]any{"session_id": "session_turn"}))
	}()

	turn, err := (Session{client: client, ID: "session_turn"}).StartTurn(context.Background(), "prompt", SendOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := turn.Accepted(ctx); err != nil {
		t.Fatal(err)
	}
	if err := turn.Cancel(ctx); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	result, err := turn.Wait(ctx)
	if err != nil || result.Kind != TurnResultCanceled || !errors.Is(result.Err, ErrTurnCanceled) {
		t.Fatalf("Wait result=%+v err=%v, want canceled", result, err)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
}

func TestTurnCancelContextOnlyInterruptsCallerWait(t *testing.T) {
	client, server := newTurnTestClient(t, Options{})
	defer client.Close()
	defer server.Close()

	cancelReceived := make(chan struct{})
	allowTerminal := make(chan struct{})
	serverDone := make(chan error, 1)
	go func() {
		if _, err := receiveTurnRequest(server); err != nil {
			serverDone <- err
			return
		}
		if err := server.Send(mustEventFrame(t, "message_accepted", map[string]any{"session_id": "session_turn"})); err != nil {
			serverDone <- err
			return
		}
		cancel, err := receiveTurnRequest(server)
		if err != nil {
			serverDone <- err
			return
		}
		close(cancelReceived)
		if err := server.Send(mustServerFrame(t, cancel.ID, "ok", nil)); err != nil {
			serverDone <- err
			return
		}
		<-allowTerminal
		serverDone <- server.Send(mustEventFrame(t, "turn_done", map[string]any{"session_id": "session_turn"}))
	}()

	turn, err := (Session{client: client, ID: "session_turn"}).StartTurn(context.Background(), "prompt", SendOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := turn.Accepted(ctx); err != nil {
		t.Fatal(err)
	}

	doneCtx, stop := context.WithCancel(context.Background())
	stop()
	if err := turn.Cancel(doneCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Cancel with done context error=%v, want context canceled", err)
	}
	select {
	case <-cancelReceived:
	case <-time.After(time.Second):
		t.Fatal("shared cancel attempt did not continue after caller context ended")
	}
	if err := turn.Cancel(ctx); err != nil {
		t.Fatalf("valid Cancel: %v", err)
	}
	close(allowTerminal)
	result, err := turn.Wait(ctx)
	if err != nil || result.Kind != TurnResultCanceled || !errors.Is(result.Err, ErrTurnCanceled) {
		t.Fatalf("Wait result=%+v err=%v", result, err)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
}

func TestTurnNaturalCompletionRacingCancelWinsImmutably(t *testing.T) {
	client, server := newTurnTestClient(t, Options{})
	defer client.Close()
	defer server.Close()

	replyCancel := make(chan struct{})
	cancelReceived := make(chan struct{})
	serverDone := make(chan error, 1)
	go func() {
		if _, err := receiveTurnRequest(server); err != nil {
			serverDone <- err
			return
		}
		if err := server.Send(mustEventFrame(t, "message_accepted", map[string]any{"session_id": "session_turn"})); err != nil {
			serverDone <- err
			return
		}
		cancel, err := receiveTurnRequest(server)
		if err != nil {
			serverDone <- err
			return
		}
		close(cancelReceived)
		if err := server.Send(mustEventFrame(t, "turn_done", map[string]any{"session_id": "session_turn"})); err != nil {
			serverDone <- err
			return
		}
		<-replyCancel
		serverDone <- server.Send(mustServerFrame(t, cancel.ID, "ok", nil))
	}()

	turn, err := (Session{client: client, ID: "session_turn"}).StartTurn(context.Background(), "prompt", SendOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := turn.Accepted(ctx); err != nil {
		t.Fatal(err)
	}
	cancelDone := make(chan error, 1)
	go func() { cancelDone <- turn.Cancel(ctx) }()
	select {
	case <-cancelReceived:
	case <-ctx.Done():
		t.Fatalf("server did not receive cancel: %v", ctx.Err())
	}
	result, err := turn.Wait(ctx)
	if err != nil || string(result.Kind) != "completed" {
		t.Fatalf("first Wait result=%+v err=%v", result, err)
	}
	close(replyCancel)
	if err := <-cancelDone; err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	again, err := turn.Wait(ctx)
	if err != nil || !reflect.DeepEqual(again, result) {
		t.Fatalf("second Wait result=%+v err=%v, want %+v", again, err, result)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
}

func TestTurnCancelAfterTerminalDoesNotWrite(t *testing.T) {
	client, server := newTurnTestClient(t, Options{})
	defer client.Close()
	defer server.Close()

	go func() {
		_, _ = receiveTurnRequest(server)
		_ = server.Send(mustEventFrame(t, "turn_done", map[string]any{"session_id": "session_turn"}))
	}()
	turn, err := (Session{client: client, ID: "session_turn"}).StartTurn(context.Background(), "prompt", SendOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := turn.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	before := client.nextID.Load()
	if err := turn.Cancel(ctx); err != nil {
		t.Fatal(err)
	}
	if after := client.nextID.Load(); after != before {
		t.Fatalf("request ID advanced from %d to %d after terminal Cancel", before, after)
	}
}

func TestTurnLifecycleContextTerminatesLocallyWithoutServerCancel(t *testing.T) {
	tests := []struct {
		name     string
		newCtx   func() (context.Context, context.CancelFunc)
		wantKind string
		wantErr  error
	}{
		{
			name: "canceled",
			newCtx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			wantKind: "lifecycle_canceled",
			wantErr:  context.Canceled,
		},
		{
			name: "deadline",
			newCtx: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), 20*time.Millisecond)
			},
			wantKind: "lifecycle_deadline_exceeded",
			wantErr:  context.DeadlineExceeded,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, server := newTurnTestClient(t, Options{})
			defer client.Close()
			defer server.Close()

			go func() {
				_, _ = receiveTurnRequest(server)
				_ = server.Send(mustEventFrame(t, "message_accepted", map[string]any{"session_id": "session_turn"}))
			}()
			lifecycleCtx, stop := tt.newCtx()
			turn, err := (Session{client: client, ID: "session_turn"}).StartTurn(lifecycleCtx, "prompt", SendOptions{})
			if err != nil {
				t.Fatal(err)
			}
			waitCtx, cancelWait := context.WithTimeout(context.Background(), time.Second)
			defer cancelWait()
			if err := turn.Accepted(waitCtx); err != nil {
				t.Fatal(err)
			}
			before := client.nextID.Load()
			if tt.wantErr == context.Canceled {
				stop()
			} else {
				defer stop()
			}
			result, err := turn.Wait(waitCtx)
			if err != nil {
				t.Fatalf("Wait: %v", err)
			}
			if string(result.Kind) != tt.wantKind || !errors.Is(result.Err, tt.wantErr) {
				t.Fatalf("result=%+v, want kind=%q err=%v", result, tt.wantKind, tt.wantErr)
			}
			if after := client.nextID.Load(); after != before {
				t.Fatalf("lifecycle context sent a request: ID %d -> %d", before, after)
			}
		})
	}
}

func TestTurnLifecycleContextPreservesCancellationCause(t *testing.T) {
	client, server := newTurnTestClient(t, Options{})
	defer client.Close()
	defer server.Close()

	go func() {
		_, _ = receiveTurnRequest(server)
		_ = server.Send(mustEventFrame(t, "message_accepted", map[string]any{"session_id": "session_turn"}))
	}()
	lifecycleCtx, cancelLifecycle := context.WithCancelCause(context.Background())
	turn, err := (Session{client: client, ID: "session_turn"}).StartTurn(lifecycleCtx, "prompt", SendOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := turn.Accepted(ctx); err != nil {
		t.Fatal(err)
	}
	cause := errors.New("caller stopped lifecycle")
	cancelLifecycle(cause)
	result, err := turn.Wait(ctx)
	if err != nil || !errors.Is(result.Err, cause) {
		t.Fatalf("Wait result=%+v err=%v, want cause %v", result, err, cause)
	}
}

func TestTurnWaitContextDoesNotChangeStoredResult(t *testing.T) {
	client, server := newTurnTestClient(t, Options{})
	defer client.Close()
	defer server.Close()

	finish := make(chan struct{})
	go func() {
		_, _ = receiveTurnRequest(server)
		_ = server.Send(mustEventFrame(t, "message_accepted", map[string]any{"session_id": "session_turn"}))
		<-finish
		_ = server.Send(mustEventFrame(t, "turn_done", map[string]any{"session_id": "session_turn"}))
	}()
	turn, err := (Session{client: client, ID: "session_turn"}).StartTurn(context.Background(), "prompt", SendOptions{})
	if err != nil {
		t.Fatal(err)
	}
	waitCtx, cancelWait := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelWait()
	if result, err := turn.Wait(waitCtx); !errors.Is(err, context.DeadlineExceeded) || result.Kind != "" || result.Err != nil {
		t.Fatalf("interrupted Wait result=%+v err=%v", result, err)
	}
	close(finish)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := turn.Wait(ctx)
	if err != nil || string(result.Kind) != "completed" || result.Err != nil {
		t.Fatalf("terminal Wait result=%+v err=%v", result, err)
	}
}

func TestStartTurnRejectsEndedLifecycleBeforeWriting(t *testing.T) {
	client, server := newTurnTestClient(t, Options{})
	defer client.Close()
	defer server.Close()

	lifecycleCtx, cancel := context.WithCancel(context.Background())
	cancel()
	beforeRequests := client.nextID.Load()
	beforeSubscriptions := client.nextSub.Load()
	turn, err := (Session{client: client, ID: "session_turn"}).StartTurn(lifecycleCtx, "prompt", SendOptions{})
	if turn != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("StartTurn turn=%v err=%v, want context canceled", turn, err)
	}
	if after := client.nextID.Load(); after != beforeRequests {
		t.Fatalf("request ID advanced from %d to %d", beforeRequests, after)
	}
	if after := client.nextSub.Load(); after != beforeSubscriptions {
		t.Fatalf("subscription ID advanced from %d to %d", beforeSubscriptions, after)
	}
}

func TestStartTurnRejectsNoReplyBeforeWriting(t *testing.T) {
	client, server := newTurnTestClient(t, Options{})
	defer client.Close()
	defer server.Close()

	before := client.nextID.Load()
	beforeSubscriptions := client.nextSub.Load()
	turn, err := (Session{client: client, ID: "session_turn"}).StartTurn(context.Background(), "prompt", SendOptions{NoReply: true})
	if turn != nil || !errors.Is(err, ErrTurnNoReply) {
		t.Fatalf("StartTurn turn=%v err=%v, want ErrTurnNoReply", turn, err)
	}
	if after := client.nextID.Load(); after != before {
		t.Fatalf("request ID advanced from %d to %d", before, after)
	}
	if after := client.nextSub.Load(); after != beforeSubscriptions {
		t.Fatalf("subscription ID advanced from %d to %d", beforeSubscriptions, after)
	}
}

func TestTurnProviderErrorTerminatesAcceptanceAndWait(t *testing.T) {
	client, server := newTurnTestClient(t, Options{})
	defer client.Close()
	defer server.Close()

	go func() {
		_, _ = receiveTurnRequest(server)
		_ = server.Send(mustEventFrame(t, "error", map[string]any{
			"code":          "provider_error",
			"message":       "sensitive provider detail",
			"provider_code": "temporarily_unavailable",
		}))
	}()
	turn, err := (Session{client: client, ID: "session_turn"}).StartTurn(context.Background(), "prompt", SendOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	acceptErr := turn.Accepted(ctx)
	var eventErr EventError
	if !errors.As(acceptErr, &eventErr) || eventErr.Code != "provider_error" || eventErr.Message != "" || eventErr.ProviderCode != "temporarily_unavailable" {
		t.Fatalf("Accepted error=%v, want provider EventError", acceptErr)
	}
	if strings.Contains(acceptErr.Error(), "sensitive provider detail") {
		t.Fatalf("Accepted error exposed provider text: %v", acceptErr)
	}
	result, err := turn.Wait(ctx)
	if err != nil || string(result.Kind) != "provider_error" || !errors.As(result.Err, &eventErr) {
		t.Fatalf("Wait result=%+v err=%v", result, err)
	}
	if _, err := turn.Next(ctx); !errors.As(err, &eventErr) {
		t.Fatalf("Next error=%v, want provider EventError", err)
	}
}

func TestTurnProviderErrorAfterAcceptancePreservesAcceptedResult(t *testing.T) {
	client, server := newTurnTestClient(t, Options{})
	defer client.Close()
	defer server.Close()

	releaseError := make(chan struct{})
	go func() {
		_, _ = receiveTurnRequest(server)
		_ = server.Send(mustEventFrame(t, "message_accepted", map[string]any{"session_id": "session_turn"}))
		<-releaseError
		_ = server.Send(mustEventFrame(t, "error", map[string]any{
			"code":    "provider_error",
			"message": "sensitive provider detail",
		}))
	}()
	turn, err := (Session{client: client, ID: "session_turn"}).StartTurn(context.Background(), "prompt", SendOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := turn.Accepted(ctx); err != nil {
		t.Fatalf("Accepted: %v", err)
	}
	close(releaseError)
	result, err := turn.Wait(ctx)
	var eventErr EventError
	if err != nil || !errors.As(result.Err, &eventErr) || eventErr.Code != "provider_error" || eventErr.Message != "" {
		t.Fatalf("Wait result=%+v err=%v", result, err)
	}
	if err := turn.Accepted(ctx); err != nil {
		t.Fatalf("Accepted changed after provider failure: %v", err)
	}
}

func TestTurnCancelProviderErrorIsSharedAndTurnCanStillComplete(t *testing.T) {
	client, server := newTurnTestClient(t, Options{})
	defer client.Close()
	defer server.Close()

	finish := make(chan struct{})
	go func() {
		_, _ = receiveTurnRequest(server)
		_ = server.Send(mustEventFrame(t, "message_accepted", map[string]any{"session_id": "session_turn"}))
		cancel, err := receiveTurnRequest(server)
		if err != nil {
			return
		}
		_ = server.Send(mustServerFrame(t, cancel.ID, "error", map[string]any{
			"code":    "cancel_rejected",
			"message": "redacted diagnostic",
		}))
		<-finish
		_ = server.Send(mustEventFrame(t, "turn_done", map[string]any{"session_id": "session_turn"}))
	}()
	turn, err := (Session{client: client, ID: "session_turn"}).StartTurn(context.Background(), "prompt", SendOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	firstErr := turn.Cancel(ctx)
	var eventErr EventError
	if !errors.As(firstErr, &eventErr) || eventErr.Code != "cancel_rejected" {
		t.Fatalf("Cancel error=%v, want cancel_rejected", firstErr)
	}
	if secondErr := turn.Cancel(ctx); !reflect.DeepEqual(secondErr, firstErr) {
		t.Fatalf("second Cancel error=%v, want shared %v", secondErr, firstErr)
	}
	close(finish)
	result, err := turn.Wait(ctx)
	if err != nil || string(result.Kind) != "completed" {
		t.Fatalf("Wait result=%+v err=%v", result, err)
	}
}

func TestSessionSendCompatibilityStillReturnsOnAcceptance(t *testing.T) {
	client, server := newTurnTestClient(t, Options{})
	defer client.Close()
	defer server.Close()

	go func() {
		_, _ = receiveTurnRequest(server)
		_ = server.Send(mustEventFrame(t, "message_accepted", map[string]any{"session_id": "session_turn"}))
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := (Session{client: client, ID: "session_turn"}).Send(ctx, "prompt", SendOptions{}); err != nil {
		t.Fatalf("Send: %v", err)
	}
}

func TestSessionSendNoReplyCompatibilityRemainsNotificationOnly(t *testing.T) {
	client, server := newTurnTestClient(t, Options{})
	defer client.Close()
	defer server.Close()

	serverDone := make(chan error, 1)
	go func() {
		request, err := receiveTurnRequest(server)
		if err != nil {
			serverDone <- err
			return
		}
		var fields struct {
			NoReply bool `json:"no_reply"`
		}
		if err := json.Unmarshal(request.Request.Fields, &fields); err != nil {
			serverDone <- err
			return
		}
		if request.Request.Req != "send_message" || !fields.NoReply {
			serverDone <- fmt.Errorf("request=%q no_reply=%v", request.Request.Req, fields.NoReply)
			return
		}
		serverDone <- nil
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := (Session{client: client, ID: "session_turn"}).Send(ctx, "prompt", SendOptions{NoReply: true}); err != nil {
		t.Fatalf("Send NoReply: %v", err)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
}

func TestSessionEventsCompatibilityRemainsIndependentAndCallerOwned(t *testing.T) {
	client, server := newTurnTestClient(t, Options{})
	defer client.Close()
	defer server.Close()

	stream := (Session{client: client, ID: "session_turn"}).Events(context.Background())
	if err := server.Send(mustEventFrame(t, "text_delta", map[string]any{
		"session_id": "session_turn",
		"text":       "independent",
	})); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	event, err := stream.Next(ctx)
	if err != nil {
		t.Fatalf("Events.Next: %v", err)
	}
	text, ok := event.(*TextDelta)
	if !ok || text.Text != "independent" {
		t.Fatalf("event=%+v, want independent TextDelta", event)
	}
	stream.Close()
	client.subsMu.Lock()
	subscriptions := len(client.subs)
	client.subsMu.Unlock()
	if subscriptions != 0 {
		t.Fatalf("subscriptions=%d after caller Close, want 0", subscriptions)
	}
}

func newTurnTestClient(t *testing.T, options Options) (*Client, *transport.FakeServer) {
	t.Helper()
	clientSide, serverSide := transport.NewPipePair()
	server := transport.NewFakeServer(serverSide)
	handshakeDone := make(chan error, 1)
	go func() {
		hello, err := receiveTurnRequest(server)
		if err != nil {
			handshakeDone <- err
			return
		}
		if hello.Request.Req != "hello" {
			handshakeDone <- fmt.Errorf("request=%q, want hello", hello.Request.Req)
			return
		}
		handshakeDone <- server.Send(mustServerFrame(t, hello.ID, "hello_ok", map[string]any{"version": 1}))
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	client, err := NewClient(ctx, clientSide, options)
	if err != nil {
		server.Close()
		t.Fatalf("NewClient: %v", err)
	}
	if err := <-handshakeDone; err != nil {
		client.Close()
		server.Close()
		t.Fatalf("handshake: %v", err)
	}
	return client, server
}

func receiveTurnRequest(server *transport.FakeServer) (protocol.ClientFrame, error) {
	data, err := server.Receive()
	if err != nil {
		return protocol.ClientFrame{}, err
	}
	var wire map[string]json.RawMessage
	if err := json.Unmarshal(data, &wire); err != nil {
		return protocol.ClientFrame{}, err
	}
	var frame protocol.ClientFrame
	if err := json.Unmarshal(wire["v"], &frame.V); err != nil {
		return protocol.ClientFrame{}, err
	}
	if err := json.Unmarshal(wire["id"], &frame.ID); err != nil {
		return protocol.ClientFrame{}, err
	}
	if err := json.Unmarshal(wire["req"], &frame.Request.Req); err != nil {
		return protocol.ClientFrame{}, err
	}
	delete(wire, "v")
	delete(wire, "id")
	delete(wire, "req")
	frame.Request.Fields, err = json.Marshal(wire)
	if err != nil {
		return protocol.ClientFrame{}, err
	}
	return frame, nil
}

func waitForTurnQueue(t *testing.T, turn *Turn, size int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for len(turn.events) != size {
		if time.Now().After(deadline) {
			t.Fatalf("turn event queue size=%d, want %d", len(turn.events), size)
		}
		time.Sleep(time.Millisecond)
	}
}
