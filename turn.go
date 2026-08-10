package jcode

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/ariel-frischer/jcode-go/protocol"
)

// ErrTurnNoReply reports that StartTurn was called for a notification-only
// message, which cannot have an owned turn lifecycle.
var ErrTurnNoReply = errors.New("jcode turn does not support no-reply messages")

// TurnResultKind identifies the semantic terminal class of a turn. Exported
// constants and the complete terminal taxonomy are intentionally left to the
// terminal-outcome API that extends this initial surface.
type TurnResultKind string

// TurnResult is the immutable outcome stored by a Turn. Err preserves the
// underlying cause for errors.Is and errors.As inspection.
type TurnResult struct {
	Kind TurnResultKind
	Err  error
}

const (
	turnResultCompleted         TurnResultKind = "completed"
	turnResultCanceled          TurnResultKind = "canceled"
	turnResultLifecycleCanceled TurnResultKind = "lifecycle_canceled"
	turnResultLifecycleDeadline TurnResultKind = "lifecycle_deadline_exceeded"
	turnResultProviderError     TurnResultKind = "provider_error"
	turnResultFailed            TurnResultKind = "failed"
)

type turnState uint8

const (
	turnStarting turnState = iota
	turnAwaitingAcceptance
	turnAccepted
	turnCancelRequested
	turnTerminal
)

// Turn owns one prompt lifecycle, including its single underlying event
// subscription, acceptance, ordered typed events, explicit cancellation, and
// immutable terminal result.
type Turn struct {
	client       *Client
	sessionID    string
	subscription *Subscription
	events       chan TypedEvent

	nextMu sync.Mutex
	mu     sync.Mutex
	state  turnState

	acceptanceSet bool
	acceptanceErr error
	acceptedDone  chan struct{}

	terminal     bool
	result       TurnResult
	terminalDone chan struct{}

	cancelOnce      sync.Once
	cancelRequested bool
	cancelErr       error
	cancelDone      chan struct{}
}

func newTurn(client *Client, sessionID string, subscription *Subscription) *Turn {
	return &Turn{
		client:       client,
		sessionID:    sessionID,
		subscription: subscription,
		// One extra slot is reserved for turn_done after the normal event
		// buffer fills exactly to its configured bound.
		events:       make(chan TypedEvent, client.options.EventBuffer+1),
		state:        turnStarting,
		acceptedDone: make(chan struct{}),
		terminalDone: make(chan struct{}),
		cancelDone:   make(chan struct{}),
	}
}

func (t *Turn) start(lifecycleCtx context.Context) {
	go t.dispatch()
	go t.watchLifecycle(lifecycleCtx)
}

func (t *Turn) markWriteComplete() {
	t.mu.Lock()
	if t.state == turnStarting {
		t.state = turnAwaitingAcceptance
	}
	t.mu.Unlock()
}

// Accepted waits only for server acceptance. Canceling ctx interrupts this wait
// without changing the turn lifecycle or sending protocol cancellation.
func (t *Turn) Accepted(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err, ok := t.acceptanceResult(); ok {
		return err
	}
	select {
	case <-t.acceptedDone:
		err, _ := t.acceptanceResult()
		return err
	case <-ctx.Done():
		if err, ok := t.acceptanceResult(); ok {
			return err
		}
		return fmt.Errorf("wait for turn acceptance: %w", ctx.Err())
	}
}

// Next returns the turn's typed events in server order. message_accepted is
// exposed through Accepted rather than duplicated in this stream. Only one
// goroutine may call Next at a time. Canceling ctx interrupts only this read.
func (t *Turn) Next(ctx context.Context) (TypedEvent, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	t.nextMu.Lock()
	defer t.nextMu.Unlock()

	select {
	case event, ok := <-t.events:
		return t.nextResult(event, ok)
	case <-ctx.Done():
		select {
		case event, ok := <-t.events:
			return t.nextResult(event, ok)
		default:
			return nil, fmt.Errorf("wait for turn event: %w", ctx.Err())
		}
	}
}

// Cancel starts the protocol cancel request at most once. The request runs under
// the Client request timeout, while each caller's ctx bounds only that caller's
// wait for the shared attempt result. A successful request is not terminal.
func (t *Turn) Cancel(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	t.cancelOnce.Do(func() {
		t.mu.Lock()
		if t.terminal {
			close(t.cancelDone)
			t.mu.Unlock()
			return
		}
		t.mu.Unlock()
		go t.runCancel()
	})
	return t.waitCancel(ctx)
}

// Wait waits for the immutable first terminal result. Its error return only
// reports interruption of this particular wait before the turn is terminal.
func (t *Turn) Wait(ctx context.Context) (TurnResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if result, ok := t.terminalResult(); ok {
		return result, nil
	}
	select {
	case <-t.terminalDone:
		result, _ := t.terminalResult()
		return result, nil
	case <-ctx.Done():
		if result, ok := t.terminalResult(); ok {
			return result, nil
		}
		return TurnResult{}, fmt.Errorf("wait for turn completion: %w", ctx.Err())
	}
}

func (t *Turn) dispatch() {
	defer close(t.events)
	for {
		event, err := t.subscription.Next(context.Background())
		if err != nil {
			wrapped := fmt.Errorf("receive turn event: %w", err)
			t.finishTerminal(TurnResult{Kind: turnResultFailed, Err: wrapped})
			return
		}

		if event.Kind == "message_accepted" {
			t.finishAcceptance(nil)
			t.mu.Lock()
			if !t.terminal && t.state != turnCancelRequested {
				t.state = turnAccepted
			}
			t.mu.Unlock()
			continue
		}
		if value, ok := event.Frame.Event.(protocol.Error); ok {
			cause := EventError{Code: value.Code}
			wrapped := fmt.Errorf("turn failed: %w", cause)
			t.finishTerminal(TurnResult{Kind: turnResultProviderError, Err: wrapped})
			return
		}

		typed, err := decodeTypedEvent(event)
		if err != nil {
			wrapped := fmt.Errorf("decode turn event: %w", err)
			t.finishTerminal(TurnResult{Kind: turnResultFailed, Err: wrapped})
			return
		}
		terminalEvent := event.Kind == "turn_done"
		if terminalEvent {
			t.finishTurnDone(typed)
			return
		}
		if !t.publishEvent(typed) {
			wrapped := fmt.Errorf("buffer turn event: %w", ErrSubscriberOverflow)
			t.finishTerminal(TurnResult{Kind: turnResultFailed, Err: wrapped})
			return
		}
	}
}

func (t *Turn) publishEvent(event TypedEvent) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.terminal {
		return false
	}
	limit := cap(t.events) - 1
	if len(t.events) >= limit {
		return false
	}
	select {
	case t.events <- event:
		return true
	default:
		return false
	}
}

func (t *Turn) finishTurnDone(event TypedEvent) bool {
	t.mu.Lock()
	if t.terminal {
		t.mu.Unlock()
		return false
	}
	select {
	case t.events <- event:
	default:
		wrapped := fmt.Errorf("buffer terminal turn event: %w", ErrSubscriberOverflow)
		if !t.acceptanceSet {
			t.acceptanceSet = true
			t.acceptanceErr = wrapped
			close(t.acceptedDone)
		}
		t.terminal = true
		t.state = turnTerminal
		t.result = TurnResult{Kind: turnResultFailed, Err: wrapped}
		close(t.terminalDone)
		t.mu.Unlock()
		t.subscription.Close()
		return false
	}
	if !t.acceptanceSet {
		t.acceptanceSet = true
		close(t.acceptedDone)
	}
	kind := turnResultCompleted
	if t.cancelRequested {
		kind = turnResultCanceled
	}
	t.terminal = true
	t.state = turnTerminal
	t.result = TurnResult{Kind: kind}
	close(t.terminalDone)
	t.mu.Unlock()
	t.subscription.Close()
	return true
}

func (t *Turn) watchLifecycle(ctx context.Context) {
	select {
	case <-ctx.Done():
		kind := turnResultLifecycleCanceled
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			kind = turnResultLifecycleDeadline
		}
		cause := context.Cause(ctx)
		if cause == nil {
			cause = ctx.Err()
		}
		wrapped := fmt.Errorf("turn lifecycle ended: %w", cause)
		t.finishTerminal(TurnResult{Kind: kind, Err: wrapped})
	case <-t.terminalDone:
	}
}

func (t *Turn) runCancel() {
	err := t.requestCancel(context.Background())
	t.mu.Lock()
	t.cancelErr = err
	close(t.cancelDone)
	t.mu.Unlock()
}

func (t *Turn) requestCancel(ctx context.Context) error {
	req, err := protocol.NewRawRequest("cancel", struct {
		SessionID string `json:"session_id"`
	}{t.sessionID})
	if err != nil {
		return fmt.Errorf("encode turn cancellation: %w", err)
	}
	frame, err := t.client.requestWithReplyObserver(ctx, req, func(frame protocol.ServerFrame) {
		if _, ok := frame.Event.(protocol.OK); !ok {
			return
		}
		t.mu.Lock()
		if !t.terminal {
			t.cancelRequested = true
			t.state = turnCancelRequested
		}
		t.mu.Unlock()
	})
	if err != nil {
		return fmt.Errorf("cancel turn: %w", err)
	}
	switch value := frame.Event.(type) {
	case protocol.OK:
		return nil
	case protocol.Error:
		return fmt.Errorf("cancel turn: %w", EventError{Code: value.Code})
	default:
		return fmt.Errorf("cancel turn: unexpected reply %q", eventKind(frame.Event))
	}
}

func (t *Turn) waitCancel(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("wait for turn cancellation: %w", err)
	}
	select {
	case <-t.cancelDone:
		t.mu.Lock()
		err := t.cancelErr
		t.mu.Unlock()
		return err
	case <-ctx.Done():
		select {
		case <-t.cancelDone:
			t.mu.Lock()
			err := t.cancelErr
			t.mu.Unlock()
			return err
		default:
			return fmt.Errorf("wait for turn cancellation: %w", ctx.Err())
		}
	}
}

func (t *Turn) nextResult(event TypedEvent, ok bool) (TypedEvent, error) {
	if ok {
		return event, nil
	}
	result, terminal := t.terminalResult()
	if terminal && result.Err != nil {
		return nil, result.Err
	}
	return nil, io.EOF
}

func (t *Turn) finishAcceptance(err error) {
	t.mu.Lock()
	if !t.acceptanceSet {
		t.acceptanceSet = true
		t.acceptanceErr = err
		close(t.acceptedDone)
	}
	t.mu.Unlock()
}

func (t *Turn) finishTerminal(result TurnResult) bool {
	t.mu.Lock()
	if t.terminal {
		t.mu.Unlock()
		return false
	}
	if !t.acceptanceSet {
		t.acceptanceSet = true
		t.acceptanceErr = result.Err
		close(t.acceptedDone)
	}
	t.terminal = true
	t.state = turnTerminal
	t.result = result
	close(t.terminalDone)
	t.mu.Unlock()
	t.subscription.Close()
	return true
}

func (t *Turn) acceptanceResult() (error, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.acceptanceErr, t.acceptanceSet
}

func (t *Turn) terminalResult() (TurnResult, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.result, t.terminal
}
