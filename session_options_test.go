package jcode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ariel-frischer/jcode-go/protocol"
	"github.com/ariel-frischer/jcode-go/transport"
)

type sessionOptionFixture struct {
	client     *Client
	server     *transport.FakeServer
	recorder   *observationRecorder
	writes     *sessionOptionWriteCounter
	ownedTurns atomic.Uint64
}

type sessionOptionWriteCounter struct {
	transport.Transport
	writes      atomic.Uint64
	beforeWrite func()
}

func (c *sessionOptionWriteCounter) Write(data []byte) (int, error) {
	if c.beforeWrite != nil {
		c.beforeWrite()
	}
	written, err := c.Transport.Write(data)
	if written > 0 {
		c.writes.Add(1)
	}
	return written, err
}

type sessionOptionRequest struct {
	id     uint64
	kind   string
	fields map[string]json.RawMessage
}

func (r sessionOptionRequest) requireString(t *testing.T, field, want string) {
	t.Helper()
	var got string
	r.requireField(t, field, &got)
	if got != want {
		t.Fatalf("%s field mismatch", field)
	}
}

func (r sessionOptionRequest) requireBool(t *testing.T, field string, want bool) {
	t.Helper()
	var got bool
	r.requireField(t, field, &got)
	if got != want {
		t.Fatalf("%s field mismatch", field)
	}
}

func (r sessionOptionRequest) requireInt(t *testing.T, field string, want int) {
	t.Helper()
	var got int
	r.requireField(t, field, &got)
	if got != want {
		t.Fatalf("%s field mismatch", field)
	}
}

func (r sessionOptionRequest) requireImages(t *testing.T, want [][2]string) {
	t.Helper()
	var got [][2]string
	r.requireField(t, "images", &got)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("images field mismatch")
	}
}

func (r sessionOptionRequest) requireField(t *testing.T, field string, destination any) {
	t.Helper()
	raw, ok := r.fields[field]
	if !ok {
		t.Fatalf("%s field missing", field)
	}
	if err := json.Unmarshal(raw, destination); err != nil {
		t.Fatalf("%s field decode failed", field)
	}
}

type sessionOptionSideEffects struct {
	requests      uint64
	writes        uint64
	subscriptions uint64
	observations  uint64
	ownedTurns    uint64
}

func (s sessionOptionSideEffects) delta(before sessionOptionSideEffects) sessionOptionSideEffects {
	return sessionOptionSideEffects{
		requests:      s.requests - before.requests,
		writes:        s.writes - before.writes,
		subscriptions: s.subscriptions - before.subscriptions,
		observations:  s.observations - before.observations,
		ownedTurns:    s.ownedTurns - before.ownedTurns,
	}
}

func (s sessionOptionSideEffects) zero() bool {
	return s == (sessionOptionSideEffects{})
}

type sessionOptionCapture struct {
	request sessionOptionRequest
	delta   sessionOptionSideEffects
	turn    *Turn
}

func newSessionOptionFixture(t *testing.T) *sessionOptionFixture {
	t.Helper()
	clientSide, serverSide := transport.NewPipePair()
	server := transport.NewFakeServer(serverSide)
	writes := &sessionOptionWriteCounter{Transport: clientSide}
	recorder := &observationRecorder{}
	handshakeDone := make(chan error, 1)
	go func() {
		hello, err := receiveSessionOptionRequest(server)
		if err != nil {
			handshakeDone <- err
			return
		}
		handshakeDone <- server.Send(mustServerFrame(t, hello.id, "hello_ok", map[string]any{"version": 1}))
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	client, err := NewClient(ctx, writes, Options{Observer: recorder})
	if err != nil {
		server.Close()
		t.Fatalf("new option fixture client failed: %T", err)
	}
	if err := <-handshakeDone; err != nil {
		client.Close()
		server.Close()
		t.Fatalf("option fixture handshake failed: %T", err)
	}
	return &sessionOptionFixture{client: client, server: server, recorder: recorder, writes: writes}
}

func (f *sessionOptionFixture) close() {
	_ = f.client.Close()
	_ = f.server.Close()
}

func (f *sessionOptionFixture) snapshot() sessionOptionSideEffects {
	return sessionOptionSideEffects{
		requests:      f.client.nextID.Load(),
		writes:        f.writes.writes.Load(),
		subscriptions: f.client.nextSub.Load(),
		observations:  uint64(len(f.recorder.snapshot())),
		ownedTurns:    f.ownedTurns.Load(),
	}
}

func (f *sessionOptionFixture) captureCreateSession(t *testing.T, options CreateSessionOptions) sessionOptionCapture {
	t.Helper()
	before := f.snapshot()
	requestResult := make(chan sessionOptionRequest, 1)
	serverDone := make(chan error, 1)
	go func() {
		request, err := receiveSessionOptionRequest(f.server)
		if err != nil {
			serverDone <- err
			return
		}
		requestResult <- request
		serverDone <- f.server.Send(mustServerFrame(t, request.id, "attached", map[string]any{
			"session": map[string]any{"session_id": "fixture-session", "status": "active"},
		}))
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := f.client.CreateSession(ctx, options); err != nil {
		t.Fatalf("create_session fixture call failed: %T", err)
	}
	request := <-requestResult
	if err := <-serverDone; err != nil {
		t.Fatalf("create_session fixture peer failed: %T", err)
	}
	return sessionOptionCapture{request: request, delta: f.snapshot().delta(before)}
}

func (f *sessionOptionFixture) captureSend(t *testing.T, content string, options SendOptions) sessionOptionCapture {
	t.Helper()
	before := f.snapshot()
	requestResult := make(chan sessionOptionRequest, 1)
	serverDone := make(chan error, 1)
	go func() {
		request, err := receiveSessionOptionRequest(f.server)
		if err != nil {
			serverDone <- err
			return
		}
		requestResult <- request
		if options.NoReply {
			serverDone <- nil
			return
		}
		serverDone <- f.server.Send(mustEventFrame(t, "message_accepted", map[string]any{
			"session_id": "fixture-session",
		}))
	}()
	session := Session{client: f.client, ID: "fixture-session"}
	if err := session.Send(context.Background(), content, options); err != nil {
		t.Fatalf("send_message fixture call failed: %T", err)
	}
	request := <-requestResult
	if err := <-serverDone; err != nil {
		t.Fatalf("send_message fixture peer failed: %T", err)
	}
	return sessionOptionCapture{request: request, delta: f.snapshot().delta(before)}
}

func (f *sessionOptionFixture) captureStartTurn(t *testing.T, content string, options SendOptions) sessionOptionCapture {
	t.Helper()
	before := f.snapshot()
	requestResult, serverDone := f.captureNextRequest()
	turn, err := f.startTurn(context.Background(), content, options)
	if err != nil {
		t.Fatalf("owned turn fixture call failed: %T", err)
	}
	request := <-requestResult
	if err := <-serverDone; err != nil {
		t.Fatalf("owned turn fixture peer failed: %T", err)
	}
	return sessionOptionCapture{request: request, delta: f.snapshot().delta(before), turn: turn}
}

func (f *sessionOptionFixture) captureNextRequest() (<-chan sessionOptionRequest, <-chan error) {
	requestResult := make(chan sessionOptionRequest, 1)
	serverDone := make(chan error, 1)
	go func() {
		request, err := receiveSessionOptionRequest(f.server)
		if err == nil {
			requestResult <- request
		}
		serverDone <- err
	}()
	return requestResult, serverDone
}

func (f *sessionOptionFixture) startTurn(ctx context.Context, content string, options SendOptions) (*Turn, error) {
	turn, err := (Session{client: f.client, ID: "fixture-session"}).StartTurn(ctx, content, options)
	if turn != nil {
		f.ownedTurns.Add(1)
	}
	return turn, err
}

func receiveSessionOptionRequest(server *transport.FakeServer) (sessionOptionRequest, error) {
	frame, err := receiveTurnRequest(server)
	if err != nil {
		return sessionOptionRequest{}, err
	}
	fields := make(map[string]json.RawMessage)
	if err := json.Unmarshal(frame.Request.Fields, &fields); err != nil {
		return sessionOptionRequest{}, err
	}
	return sessionOptionRequest{id: frame.ID, kind: frame.Request.Req, fields: fields}, nil
}

func requireOptionError(t *testing.T, err error, field string, prohibited ...string) {
	t.Helper()
	if !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("option error does not match ErrInvalidOptions: %T", err)
	}
	var optionErr *OptionError
	if !errors.As(err, &optionErr) {
		t.Fatalf("option error does not expose *OptionError: %T", err)
	}
	if optionErr.Field != field {
		t.Fatalf("option error field mismatch: got %q, want %q", optionErr.Field, field)
	}
	for _, text := range []string{optionErr.Error(), err.Error()} {
		for _, sensitive := range prohibited {
			if strings.Contains(text, sensitive) {
				t.Fatalf("option error text exposes prohibited value for field %q", field)
			}
		}
	}
}

func TestOptionErrorContract(t *testing.T) {
	optionErrorType := reflect.TypeOf(OptionError{})
	if optionErrorType.NumField() != 1 || optionErrorType.Field(0).Name != "Field" {
		t.Fatalf("OptionError must expose only the safe Field classification")
	}

	prohibited := []string{
		"rejected-profile-secret",
		"2026-08-22T23:59:59Z",
		"prompt-secret",
		"response-secret",
		"credential-secret",
		`{"raw":"frame-secret"}`,
		"private-session-secret",
	}
	for _, field := range []string{"profile", "max_turns", "token_budget", "deadline"} {
		t.Run(field, func(t *testing.T) {
			err := fmt.Errorf("validate typed options: %w", &OptionError{Field: field})
			requireOptionError(t, err, field, prohibited...)
		})
	}
}

func TestSessionOptionFixtureCapturesRequestsAndSideEffects(t *testing.T) {
	fixture := newSessionOptionFixture(t)
	defer fixture.close()

	createResult := fixture.captureCreateSession(t, CreateSessionOptions{WorkingDir: "fixture-workdir"})
	if createResult.request.kind != "create_session" {
		t.Fatalf("request kind mismatch for create_session")
	}
	createResult.request.requireString(t, "working_dir", "fixture-workdir")
	if createResult.delta.requests != 1 || createResult.delta.writes != 1 {
		t.Fatalf("create_session request/write delta mismatch")
	}

	sendResult := fixture.captureSend(t, "fixture-content", SendOptions{NoReply: true})
	if sendResult.request.kind != "send_message" {
		t.Fatalf("request kind mismatch for send_message")
	}
	sendResult.request.requireString(t, "content", "fixture-content")
	sendResult.request.requireBool(t, "no_reply", true)
	if sendResult.delta.requests != 1 || sendResult.delta.writes != 1 {
		t.Fatalf("send_message request/write delta mismatch")
	}

	turnResult := fixture.captureStartTurn(t, "fixture-content", SendOptions{})
	if turnResult.turn == nil {
		t.Fatalf("owned turn fixture did not retain the created turn")
	}
	if turnResult.request.kind != "send_message" {
		t.Fatalf("request kind mismatch for owned turn")
	}
	if turnResult.delta.subscriptions != 1 || turnResult.delta.ownedTurns != 1 {
		t.Fatalf("owned turn side-effect delta mismatch")
	}

	before := fixture.snapshot()
	turn, err := fixture.startTurn(context.Background(), "fixture-content", SendOptions{NoReply: true})
	if turn != nil || !errors.Is(err, ErrTurnNoReply) {
		t.Fatalf("start turn rejection classification mismatch")
	}
	if delta := fixture.snapshot().delta(before); !delta.zero() {
		t.Fatalf("rejected start turn produced side effects: %+v", delta)
	}
}

func TestCreateSessionProfileMapping(t *testing.T) {
	tests := []struct {
		name        string
		options     CreateSessionOptions
		wantProfile string
		wantFields  int
	}{
		{
			name:       "zero value preserves legacy fields",
			options:    CreateSessionOptions{},
			wantFields: 0,
		},
		{
			name:        "profile is preserved exactly",
			options:     CreateSessionOptions{Profile: "  named-profile  "},
			wantProfile: "  named-profile  ",
			wantFields:  1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newSessionOptionFixture(t)
			defer fixture.close()

			result := fixture.captureCreateSession(t, test.options)
			if result.request.kind != "create_session" {
				t.Fatalf("request kind mismatch for create_session")
			}
			if len(result.request.fields) != test.wantFields {
				t.Fatalf("create_session field count mismatch: got %d, want %d", len(result.request.fields), test.wantFields)
			}
			if test.wantProfile == "" {
				if _, ok := result.request.fields["profile"]; ok {
					t.Fatalf("zero-value create_session unexpectedly included profile")
				}
				return
			}
			result.request.requireString(t, "profile", test.wantProfile)
		})
	}
}

func TestCreateSessionRejectsBlankProfileBeforeSideEffects(t *testing.T) {
	fixture := newSessionOptionFixture(t)
	defer fixture.close()

	before := fixture.snapshot()
	_, err := fixture.client.CreateSession(context.Background(), CreateSessionOptions{Profile: " \t\n "})
	requireOptionError(t, err, "profile", " \t\n ")
	if delta := fixture.snapshot().delta(before); !delta.zero() {
		t.Fatalf("blank profile produced side effects: %+v", delta)
	}
}

func TestSendSafetyOptionsMapping(t *testing.T) {
	const (
		content        = "fixture-content"
		futureZ        = "9999-12-31T23:59:59Z"
		futureOffset   = "9999-12-31T23:59:59+05:30"
		fixtureSession = "fixture-session"
	)
	images := [][2]string{{"image/png", "synthetic-image"}}
	tests := []struct {
		name    string
		options SendOptions
		want    map[string]any
	}{
		{name: "zero safety values are omitted", options: SendOptions{}},
		{
			name:    "maximum turns maps independently",
			options: SendOptions{MaxTurns: 3},
			want:    map[string]any{"max_turns": 3},
		},
		{
			name:    "token budget maps independently",
			options: SendOptions{TokenBudget: 4096},
			want:    map[string]any{"token_budget": 4096},
		},
		{
			name:    "UTC deadline maps independently",
			options: SendOptions{Deadline: futureZ},
			want:    map[string]any{"deadline": futureZ},
		},
		{
			name:    "numeric offset deadline maps independently",
			options: SendOptions{Deadline: futureOffset},
			want:    map[string]any{"deadline": futureOffset},
		},
		{
			name: "all safety values map together",
			options: SendOptions{
				MaxTurns:    7,
				TokenBudget: 8192,
				Deadline:    futureOffset,
			},
			want: map[string]any{
				"max_turns":    7,
				"token_budget": 8192,
				"deadline":     futureOffset,
			},
		},
	}
	paths := []struct {
		name    string
		capture func(*testing.T, *sessionOptionFixture, SendOptions) sessionOptionCapture
	}{
		{
			name: "acceptance wait send",
			capture: func(t *testing.T, fixture *sessionOptionFixture, options SendOptions) sessionOptionCapture {
				return fixture.captureSend(t, content, options)
			},
		},
		{
			name: "no reply send",
			capture: func(t *testing.T, fixture *sessionOptionFixture, options SendOptions) sessionOptionCapture {
				options.NoReply = true
				return fixture.captureSend(t, content, options)
			},
		},
		{
			name: "owned turn",
			capture: func(t *testing.T, fixture *sessionOptionFixture, options SendOptions) sessionOptionCapture {
				return fixture.captureStartTurn(t, content, options)
			},
		},
	}

	for _, path := range paths {
		t.Run(path.name, func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					fixture := newSessionOptionFixture(t)
					defer fixture.close()

					options := test.options
					options.Images = append([][2]string(nil), images...)
					result := path.capture(t, fixture, options)
					if result.request.kind != "send_message" {
						t.Fatalf("request kind mismatch for send_message")
					}
					result.request.requireString(t, "session_id", fixtureSession)
					result.request.requireString(t, "content", content)
					result.request.requireImages(t, images)
					wantFields := 3 + len(test.want)
					if path.name == "no reply send" {
						result.request.requireBool(t, "no_reply", true)
						wantFields++
					} else if _, ok := result.request.fields["no_reply"]; ok {
						t.Fatalf("replying send unexpectedly included no_reply")
					}
					if len(result.request.fields) != wantFields {
						t.Fatalf("send_message field count mismatch: got %d, want %d", len(result.request.fields), wantFields)
					}
					for _, field := range []string{"max_turns", "token_budget", "deadline"} {
						want, included := test.want[field]
						if !included {
							if _, ok := result.request.fields[field]; ok {
								t.Fatalf("send_message unexpectedly included %s", field)
							}
							continue
						}
						switch value := want.(type) {
						case int:
							result.request.requireInt(t, field, value)
						case string:
							result.request.requireString(t, field, value)
						default:
							t.Fatalf("unsupported expected field type for %s", field)
						}
					}
				})
			}
		})
	}
}

func TestSendSafetyOptionsRejectInvalidValuesBeforeSideEffects(t *testing.T) {
	tests := []struct {
		name       string
		options    SendOptions
		field      string
		prohibited string
	}{
		{
			name:       "negative maximum turns",
			options:    SendOptions{MaxTurns: -1},
			field:      "max_turns",
			prohibited: "-1",
		},
		{
			name:       "negative token budget",
			options:    SendOptions{TokenBudget: -2},
			field:      "token_budget",
			prohibited: "-2",
		},
		{
			name:       "malformed deadline",
			options:    SendOptions{Deadline: "not-a-deadline"},
			field:      "deadline",
			prohibited: "not-a-deadline",
		},
		{
			name:       "timezone-less deadline",
			options:    SendOptions{Deadline: "9999-12-31T23:59:59"},
			field:      "deadline",
			prohibited: "9999-12-31T23:59:59",
		},
		{
			name:       "expired deadline",
			options:    SendOptions{Deadline: "2000-01-01T00:00:00Z"},
			field:      "deadline",
			prohibited: "2000-01-01T00:00:00Z",
		},
	}
	paths := []struct {
		name string
		run  func(context.Context, *sessionOptionFixture, SendOptions) error
	}{
		{
			name: "acceptance wait send",
			run: func(ctx context.Context, fixture *sessionOptionFixture, options SendOptions) error {
				return (Session{client: fixture.client, ID: "fixture-session"}).Send(ctx, "fixture-content", options)
			},
		},
		{
			name: "no reply send",
			run: func(ctx context.Context, fixture *sessionOptionFixture, options SendOptions) error {
				options.NoReply = true
				return (Session{client: fixture.client, ID: "fixture-session"}).Send(ctx, "fixture-content", options)
			},
		},
		{
			name: "owned turn",
			run: func(ctx context.Context, fixture *sessionOptionFixture, options SendOptions) error {
				_, err := fixture.startTurn(ctx, "fixture-content", options)
				return err
			},
		},
	}

	for _, path := range paths {
		t.Run(path.name, func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					fixture := newSessionOptionFixture(t)
					defer fixture.close()
					before := fixture.snapshot()
					ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
					defer cancel()

					err := path.run(ctx, fixture, test.options)
					requireOptionError(t, err, test.field, test.prohibited)
					if delta := fixture.snapshot().delta(before); !delta.zero() {
						t.Fatalf("invalid %s produced side effects: %+v", test.field, delta)
					}
				})
			}
		})
	}
}

func TestLegacyTypedSessionCallsPreserveExactPayloads(t *testing.T) {
	fixture := newSessionOptionFixture(t)
	defer fixture.close()

	create := fixture.captureCreateSession(t, CreateSessionOptions{})
	if create.request.kind != "create_session" || len(create.request.fields) != 0 {
		t.Fatalf("zero-value create_session payload changed")
	}

	send := fixture.captureSend(t, "fixture-content", SendOptions{})
	if send.request.kind != "send_message" || len(send.request.fields) != 2 {
		t.Fatalf("zero-value send_message payload changed")
	}
	send.request.requireString(t, "session_id", "fixture-session")
	send.request.requireString(t, "content", "fixture-content")

	noReply := fixture.captureSend(t, "fixture-content", SendOptions{NoReply: true})
	if noReply.request.kind != "send_message" || len(noReply.request.fields) != 3 {
		t.Fatalf("notification-only send_message payload changed")
	}
	noReply.request.requireBool(t, "no_reply", true)

	before := fixture.snapshot()
	turn, err := fixture.startTurn(context.Background(), "fixture-content", SendOptions{NoReply: true})
	if turn != nil || !errors.Is(err, ErrTurnNoReply) {
		t.Fatalf("StartTurn NoReply error changed: %T", err)
	}
	if delta := fixture.snapshot().delta(before); !delta.zero() {
		t.Fatalf("StartTurn NoReply produced side effects: %+v", delta)
	}
}

func TestRawRequestsBypassTypedOptionValidation(t *testing.T) {
	rawFields := map[string]any{
		"max_turns":    -1,
		"deadline":     "not-a-deadline",
		"future_field": true,
	}
	tests := []struct {
		name string
		send func(context.Context, *Client, protocol.RawRequest) error
	}{
		{
			name: "request",
			send: func(ctx context.Context, client *Client, request protocol.RawRequest) error {
				_, err := client.Request(ctx, request)
				return err
			},
		},
		{
			name: "notify",
			send: func(_ context.Context, client *Client, request protocol.RawRequest) error {
				return client.Notify(request)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newSessionOptionFixture(t)
			defer fixture.close()

			request, err := protocol.NewRawRequest("future_operation", rawFields)
			if err != nil {
				t.Fatalf("NewRawRequest: %v", err)
			}
			captured := make(chan sessionOptionRequest, 1)
			serverDone := make(chan error, 1)
			go func() {
				got, receiveErr := receiveSessionOptionRequest(fixture.server)
				if receiveErr != nil {
					serverDone <- receiveErr
					return
				}
				captured <- got
				if test.name == "request" {
					serverDone <- fixture.server.Send(mustServerFrame(t, got.id, "ok", nil))
					return
				}
				serverDone <- nil
			}()

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := test.send(ctx, fixture.client, request); err != nil {
				if errors.Is(err, ErrInvalidOptions) {
					t.Fatalf("raw %s was subjected to typed option validation", test.name)
				}
				t.Fatalf("raw %s failed: %v", test.name, err)
			}
			got := <-captured
			if got.kind != "future_operation" || len(got.fields) != len(rawFields) {
				t.Fatalf("raw %s payload changed", test.name)
			}
			got.requireInt(t, "max_turns", -1)
			got.requireString(t, "deadline", "not-a-deadline")
			got.requireBool(t, "future_field", true)
			if err := <-serverDone; err != nil {
				t.Fatalf("raw %s peer failed: %v", test.name, err)
			}
		})
	}
}

func TestStartTurnSafetyOptionsPreserveOwnedLifecycle(t *testing.T) {
	fixture := newSessionOptionFixture(t)
	defer fixture.close()

	beforeSubscriptions := fixture.client.nextSub.Load()
	checkedWriteOrder := false
	fixture.writes.beforeWrite = func() {
		if checkedWriteOrder {
			return
		}
		checkedWriteOrder = true
		if got := fixture.client.nextSub.Load(); got != beforeSubscriptions+1 {
			t.Fatalf("send_message write began with %d subscriptions, want %d", got, beforeSubscriptions+1)
		}
	}

	requestSeen := make(chan sessionOptionRequest, 1)
	releaseTerminal := make(chan struct{})
	serverDone := make(chan error, 1)
	go func() {
		request, err := receiveSessionOptionRequest(fixture.server)
		if err != nil {
			serverDone <- err
			return
		}
		requestSeen <- request
		for _, frame := range [][]byte{
			mustEventFrame(t, "message_accepted", map[string]any{"session_id": "fixture-session"}),
			mustEventFrame(t, "text_delta", map[string]any{"session_id": "fixture-session", "text": "first"}),
			mustEventFrame(t, "reasoning_delta", map[string]any{"session_id": "fixture-session", "text": "second"}),
		} {
			if err := fixture.server.Send(frame); err != nil {
				serverDone <- err
				return
			}
		}
		cancelRequest, err := receiveSessionOptionRequest(fixture.server)
		if err != nil {
			serverDone <- err
			return
		}
		if cancelRequest.kind != "cancel" {
			serverDone <- fmt.Errorf("request=%q, want cancel", cancelRequest.kind)
			return
		}
		if err := fixture.server.Send(mustServerFrame(t, cancelRequest.id, "ok", nil)); err != nil {
			serverDone <- err
			return
		}
		<-releaseTerminal
		serverDone <- fixture.server.Send(mustEventFrame(t, "turn_done", map[string]any{"session_id": "fixture-session"}))
	}()

	options := SendOptions{
		MaxTurns:    7,
		TokenBudget: 8192,
		Deadline:    "9999-12-31T23:59:59+05:30",
	}
	turn, err := fixture.startTurn(context.Background(), "fixture-content", options)
	if err != nil {
		t.Fatalf("StartTurn: %v", err)
	}
	request := <-requestSeen
	if !checkedWriteOrder {
		t.Fatal("send_message write order was not checked")
	}
	if request.kind != "send_message" || len(request.fields) != 5 {
		t.Fatalf("typed send_message payload exposed unexpected fields")
	}
	request.requireString(t, "session_id", "fixture-session")
	request.requireString(t, "content", "fixture-content")
	request.requireInt(t, "max_turns", options.MaxTurns)
	request.requireInt(t, "token_budget", options.TokenBudget)
	request.requireString(t, "deadline", options.Deadline)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := turn.Accepted(ctx); err != nil {
		t.Fatalf("Accepted: %v", err)
	}
	first, err := turn.Next(ctx)
	if err != nil {
		t.Fatalf("first Next: %v", err)
	}
	second, err := turn.Next(ctx)
	if err != nil {
		t.Fatalf("second Next: %v", err)
	}
	if text, ok := first.(*TextDelta); !ok || text.Text != "first" {
		t.Fatalf("first event=%#v, want ordered TextDelta", first)
	}
	if reasoning, ok := second.(*ReasoningDelta); !ok || reasoning.Text != "second" {
		t.Fatalf("second event=%#v, want ordered ReasoningDelta", second)
	}

	waitCtx, cancelWait := context.WithCancel(context.Background())
	cancelWait()
	if result, err := turn.Wait(waitCtx); !errors.Is(err, context.Canceled) || result != (TurnResult{}) {
		t.Fatalf("canceled Wait result=%+v err=%v", result, err)
	}
	if _, terminal := turn.terminalResult(); terminal {
		t.Fatal("wait context terminated the owned turn")
	}
	if err := turn.Cancel(ctx); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	close(releaseTerminal)
	result, err := turn.Wait(ctx)
	if err != nil || result.Kind != TurnResultCanceled || !errors.Is(result.Err, ErrTurnCanceled) {
		t.Fatalf("Wait result=%+v err=%v, want canceled", result, err)
	}
	turn.finishTerminal(TurnResult{Kind: TurnResultProviderError, Err: EventError{Code: "synthetic"}})
	again, err := turn.Wait(ctx)
	if err != nil || !reflect.DeepEqual(again, result) {
		t.Fatalf("later Wait result=%+v err=%v, want immutable %+v", again, err, result)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
}
