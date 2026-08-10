package jcode

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

type safeObservationRecorder struct {
	mu           sync.Mutex
	observations []Observation
}

func (r *safeObservationRecorder) Observe(observation Observation) {
	r.mu.Lock()
	r.observations = append(r.observations, observation)
	r.mu.Unlock()
}

func (r *safeObservationRecorder) snapshot() []Observation {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Observation(nil), r.observations...)
}

func (r *safeObservationRecorder) waitForKind(t *testing.T, kind string) []Observation {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		observations := r.snapshot()
		for _, observation := range observations {
			if observation.Kind == kind {
				return observations
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("observation %q was not emitted: %+v", kind, r.snapshot())
	return nil
}

func TestTurnLifecycleObservationsAreBoundedAndRedacted(t *testing.T) {
	recorder := &safeObservationRecorder{}
	client, server := newTurnTestClient(t, Options{Observer: recorder})
	defer client.Close()
	defer server.Close()

	const (
		prompt    = "prompt-secret-value"
		sessionID = "session-private-identifier"
		response  = "response-and-tool-content"
	)
	serverDone := make(chan error, 1)
	go func() {
		if _, err := receiveTurnRequest(server); err != nil {
			serverDone <- err
			return
		}
		for _, frame := range [][]byte{
			mustEventFrame(t, "message_accepted", map[string]any{"session_id": sessionID}),
			mustEventFrame(t, "text_delta", map[string]any{"session_id": sessionID, "text": response}),
			mustEventFrame(t, "turn_done", map[string]any{"session_id": sessionID}),
		} {
			if err := server.Send(frame); err != nil {
				serverDone <- err
				return
			}
		}
		serverDone <- nil
	}()

	turn, err := (Session{client: client, ID: sessionID}).StartTurn(context.Background(), prompt, SendOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := turn.Accepted(ctx); err != nil {
		t.Fatal(err)
	}
	for {
		event, nextErr := turn.Next(ctx)
		if nextErr != nil {
			t.Fatal(nextErr)
		}
		if _, ok := event.(*TurnDone); ok {
			break
		}
	}
	result, err := turn.Wait(ctx)
	if err != nil || result.Kind != TurnResultCompleted {
		t.Fatalf("Wait() = %+v, %v, want completed", result, err)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}

	observations := recorder.waitForKind(t, "turn_terminal")
	assertObservationSubsequence(t, observations, []Observation{
		{Kind: "connect_start"},
		{Kind: "connect_ready"},
		{Kind: "turn_start"},
		{Kind: "turn_prompt_accepted"},
		{Kind: "turn_first_event"},
		{Kind: "turn_terminal", Outcome: TurnResultCompleted},
	})
	assertObservationsExclude(t, observations, prompt, sessionID, response,
		"credential-secret", "/private/runtime/api.sock", `{"raw":"protocol-frame"}`)
}

func TestTurnCancellationRequestObservationsAreSharedAndRedacted(t *testing.T) {
	recorder := &safeObservationRecorder{}
	client, server := newTurnTestClient(t, Options{Observer: recorder})
	defer client.Close()
	defer server.Close()

	const sessionID = "session-cancel-private"
	serverDone := make(chan error, 1)
	go func() {
		if _, err := receiveTurnRequest(server); err != nil {
			serverDone <- err
			return
		}
		if err := server.Send(mustEventFrame(t, "message_accepted", map[string]any{"session_id": sessionID})); err != nil {
			serverDone <- err
			return
		}
		request, err := receiveTurnRequest(server)
		if err != nil {
			serverDone <- err
			return
		}
		if request.Request.Req != "cancel" {
			serverDone <- fmt.Errorf("request = %q, want cancel", request.Request.Req)
			return
		}
		if err := server.Send(mustServerFrame(t, request.ID, "ok", nil)); err != nil {
			serverDone <- err
			return
		}
		serverDone <- server.Send(mustEventFrame(t, "turn_done", map[string]any{"session_id": sessionID}))
	}()

	turn, err := (Session{client: client, ID: sessionID}).StartTurn(context.Background(), "cancel-prompt-secret", SendOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := turn.Accepted(ctx); err != nil {
		t.Fatal(err)
	}
	if err := turn.Cancel(ctx); err != nil {
		t.Fatal(err)
	}
	result, err := turn.Wait(ctx)
	if err != nil || result.Kind != TurnResultCanceled {
		t.Fatalf("Wait() = %+v, %v, want canceled", result, err)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}

	_ = recorder.waitForKind(t, "turn_terminal")
	observations := recorder.waitForKind(t, "turn_cancel_request_complete")
	assertObservationCount(t, observations, "turn_cancel_request_start", 1)
	assertObservationCount(t, observations, "turn_cancel_request_complete", 1)
	assertObservationCount(t, observations, "turn_terminal", 1)
	assertObservationSubsequence(t, observations, []Observation{
		{Kind: "turn_cancel_request_start"},
		{Kind: "turn_first_event"},
		{Kind: "turn_terminal", Outcome: TurnResultCanceled},
	})
	assertObservationsExclude(t, observations, sessionID, "cancel-prompt-secret")
}

func TestLaunchInstanceFailureObservationsAreBoundedAndRedacted(t *testing.T) {
	recorder := &safeObservationRecorder{}
	const privateBinary = "/private/bin/jcode-with-secret-name"
	_, err := LaunchInstance(LaunchOptions{
		Binary:   privateBinary,
		Observer: recorder,
		Env:      map[string]string{"API_TOKEN": "credential-secret"},
	})
	if err == nil {
		t.Fatal("LaunchInstance() error = nil, want missing binary")
	}
	observations := recorder.snapshot()
	assertObservationSubsequence(t, observations, []Observation{
		{Kind: "launch_start"},
		{Kind: "launch_prepare"},
		{Kind: "launch_error", Error: string(LaunchMissingBinary)},
	})
	assertObservationsExclude(t, observations, privateBinary, "credential-secret")
}

func assertObservationSubsequence(t *testing.T, got, want []Observation) {
	t.Helper()
	index := 0
	for _, observation := range got {
		if index == len(want) {
			break
		}
		candidate := want[index]
		if observation.Kind == candidate.Kind && observation.Outcome == candidate.Outcome &&
			(candidate.Error == "" || observation.Error == candidate.Error) {
			index++
		}
	}
	if index != len(want) {
		t.Fatalf("observations = %+v, want subsequence %+v", got, want)
	}
}

func assertObservationCount(t *testing.T, observations []Observation, kind string, want int) {
	t.Helper()
	got := 0
	for _, observation := range observations {
		if observation.Kind == kind {
			got++
		}
	}
	if got != want {
		t.Fatalf("observation %q count = %d, want %d in %+v", kind, got, want, observations)
	}
}

func assertObservationsExclude(t *testing.T, observations []Observation, prohibited ...string) {
	t.Helper()
	encoded, err := json.Marshal(observations)
	if err != nil {
		t.Fatal(err)
	}
	serialized := string(encoded)
	for _, value := range prohibited {
		if value != "" && strings.Contains(serialized, value) {
			t.Fatalf("observations contain prohibited value %q: %s", value, serialized)
		}
	}
}
