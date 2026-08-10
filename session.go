package jcode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/ariel-frischer/jcode-go/protocol"
)

// SessionInfo is the stable session metadata returned by the harness API.
type SessionInfo struct {
	ID              string `json:"session_id"`
	WorkingDir      string `json:"working_dir,omitempty"`
	Title           string `json:"title,omitempty"`
	Status          string `json:"status"`
	TranscriptBytes uint64 `json:"transcript_bytes,omitempty"`
	Archived        bool   `json:"archived,omitempty"`
	ArchivedAtMS    uint64 `json:"archived_at_ms,omitempty"`
}

type CreateSessionOptions struct {
	WorkingDir string
}

type SendOptions struct {
	Images  [][2]string
	NoReply bool
}

// EventError reports a terminal error emitted by the harness for a session
// turn. It preserves the protocol code and message for callers that need to
// classify provider failures without inspecting raw protocol frames.
type EventError struct {
	Code    string
	Message string
}

func (e EventError) Error() string {
	if e.Code == "" {
		return e.Message
	}
	if e.Message == "" {
		return e.Code
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

type Session struct {
	client *Client
	Info   SessionInfo
	ID     string
}

func (c *Client) CreateSession(ctx context.Context, options CreateSessionOptions) (Session, error) {
	c.emit(Observation{Kind: "create_session_start", Request: "create_session"})
	req, err := protocol.NewRawRequest("create_session", struct {
		WorkingDir string `json:"working_dir,omitempty"`
	}{options.WorkingDir})
	if err != nil {
		c.emit(Observation{Kind: "create_session_error", Request: "create_session", Error: "request_encode"})
		return Session{}, err
	}
	frame, err := c.Request(ctx, req)
	if err != nil {
		errorCode := "request_failed"
		if errors.Is(err, context.DeadlineExceeded) {
			errorCode = "request_timeout"
		} else if errors.Is(err, context.Canceled) {
			errorCode = "request_canceled"
		}
		c.emit(Observation{Kind: "create_session_error", Request: "create_session", Error: errorCode})
		return Session{}, err
	}
	if value, ok := frame.Event.(protocol.Error); ok {
		c.emit(Observation{Kind: "create_session_error", Request: "create_session", Error: value.Code})
		return Session{}, fmt.Errorf("%s: %s", value.Code, value.Message)
	}
	fields, ok := protocol.FieldsJSON(frame.Event)
	if !ok {
		c.emit(Observation{Kind: "create_session_error", Request: "create_session", Error: "unexpected_reply"})
		return Session{}, fmt.Errorf("unexpected create_session reply: %s", eventKind(frame.Event))
	}
	var response struct {
		Session SessionInfo `json:"session"`
	}
	if err := json.Unmarshal(fields, &response); err != nil {
		c.emit(Observation{Kind: "create_session_error", Request: "create_session", Error: "invalid_reply"})
		return Session{}, err
	}
	c.emit(Observation{Kind: "create_session_ok", Request: "create_session"})
	return Session{client: c, Info: response.Session, ID: response.Session.ID}, nil
}

func (c *Client) AttachSession(ctx context.Context, id string) (Session, error) {
	req, err := protocol.NewRawRequest("attach_session", struct {
		SessionID string `json:"session_id"`
	}{id})
	if err != nil {
		return Session{}, err
	}
	frame, err := c.Request(ctx, req)
	if err != nil {
		return Session{}, err
	}
	fields, ok := protocol.FieldsJSON(frame.Event)
	if !ok {
		return Session{}, fmt.Errorf("unexpected attach_session reply: %s", eventKind(frame.Event))
	}
	var response struct {
		Session SessionInfo `json:"session"`
	}
	if err := json.Unmarshal(fields, &response); err != nil {
		return Session{}, err
	}
	return Session{client: c, Info: response.Session, ID: response.Session.ID}, nil
}

func (s Session) Send(ctx context.Context, content string, options SendOptions) error {
	if ctx == nil {
		ctx = context.Background()
	}
	images := make([][2]string, len(options.Images))
	copy(images, options.Images)
	req, err := protocol.NewRawRequest("send_message", struct {
		SessionID string      `json:"session_id"`
		Content   string      `json:"content"`
		Images    [][2]string `json:"images,omitempty"`
		NoReply   bool        `json:"no_reply,omitempty"`
	}{s.ID, content, images, options.NoReply})
	if err != nil {
		return err
	}
	if options.NoReply {
		return s.client.Notify(req)
	}

	// Subscribe before writing so a fast server cannot emit message_accepted
	// between the notification and subscription setup.
	sub := s.client.Subscribe(s.ID)
	defer sub.Close()
	if err := s.client.Notify(req); err != nil {
		return err
	}
	for {
		event, err := sub.Next(ctx)
		if err != nil {
			return err
		}
		if event.Kind == "message_accepted" {
			return nil
		}
		if event.Kind == "error" {
			if value, ok := event.Frame.Event.(protocol.Error); ok {
				return fmt.Errorf("%s: %s", value.Code, value.Message)
			}
		}
	}
}

// StartTurn starts one owned turn. Its subscription and dispatcher are ready
// before send_message is written, so fast acceptance and terminal events cannot
// be missed. The method returns after the write succeeds, before acceptance.
func (s Session) StartTurn(lifecycleCtx context.Context, content string, options SendOptions) (*Turn, error) {
	if options.NoReply {
		return nil, ErrTurnNoReply
	}
	if lifecycleCtx == nil {
		lifecycleCtx = context.Background()
	}
	if err := lifecycleCtx.Err(); err != nil {
		cause := context.Cause(lifecycleCtx)
		if cause == nil {
			cause = err
		}
		return nil, fmt.Errorf("start turn lifecycle: %w", cause)
	}

	images := make([][2]string, len(options.Images))
	copy(images, options.Images)
	req, err := protocol.NewRawRequest("send_message", struct {
		SessionID string      `json:"session_id"`
		Content   string      `json:"content"`
		Images    [][2]string `json:"images,omitempty"`
	}{s.ID, content, images})
	if err != nil {
		return nil, fmt.Errorf("encode turn message: %w", err)
	}

	// The Turn owns this single subscription until its first terminal result.
	// The internal subscription reserves slots for acceptance and the event that
	// proves the public Turn queue has reached its configured bound.
	subscription := s.client.subscribe(s.ID, s.client.options.EventBuffer+2)
	turn := newTurn(s.client, s.ID, subscription)
	turn.start(lifecycleCtx)
	if err := s.client.Notify(req); err != nil {
		wrapped := fmt.Errorf("send turn message: %w", err)
		turn.finishTerminal(TurnResult{Kind: turnResultFailed, Err: wrapped})
		return nil, wrapped
	}
	turn.markWriteComplete()
	return turn, nil
}

func (s Session) Events(ctx context.Context) *TypedEventStream {
	return &TypedEventStream{subscription: s.client.Subscribe(s.ID)}
}

type TypedEventStream struct {
	subscription *Subscription
}

func (s *TypedEventStream) Next(ctx context.Context) (TypedEvent, error) {
	event, err := s.subscription.Next(ctx)
	if err != nil {
		return nil, err
	}
	if value, ok := event.Frame.Event.(protocol.Error); ok {
		return nil, EventError{Code: value.Code, Message: value.Message}
	}
	return decodeTypedEvent(event)
}

func (s *TypedEventStream) Close() { s.subscription.Close() }

type TypedEvent interface{ typedEvent() }

type TextDelta struct {
	SessionID string `json:"session_id"`
	Text      string `json:"text"`
}

func (TextDelta) typedEvent() {}

type ReasoningDelta struct {
	SessionID string `json:"session_id"`
	Text      string `json:"text"`
}

func (ReasoningDelta) typedEvent() {}

type ToolStart struct {
	SessionID string `json:"session_id"`
	CallID    string `json:"call_id"`
	Name      string `json:"name"`
}

func (ToolStart) typedEvent() {}

type ToolInputDelta struct {
	SessionID string `json:"session_id"`
	CallID    string `json:"call_id"`
	Delta     string `json:"delta"`
}

func (ToolInputDelta) typedEvent() {}

type ToolDone struct {
	SessionID string `json:"session_id"`
	CallID    string `json:"call_id"`
	Name      string `json:"name"`
	Output    string `json:"output"`
	Error     string `json:"error,omitempty"`
}

func (ToolDone) typedEvent() {}

type TokenUsage struct {
	SessionID      string `json:"session_id"`
	Input          int64  `json:"input"`
	Output         int64  `json:"output"`
	CacheReadInput int64  `json:"cache_read_input,omitempty"`
}

func (TokenUsage) typedEvent() {}

type TurnDone struct {
	SessionID string `json:"session_id"`
}

func (TurnDone) typedEvent() {}

type PermissionRequest struct {
	SessionID   string `json:"session_id"`
	RequestID   string `json:"request_id"`
	ToolName    string `json:"tool_name"`
	Description string `json:"description"`
}

func (PermissionRequest) typedEvent() {}

type ModelInfo struct {
	SessionID string `json:"session_id"`
	Provider  string `json:"provider,omitempty"`
	Model     string `json:"model,omitempty"`
}

func (ModelInfo) typedEvent() {}

type UnknownEvent struct {
	Kind   string
	Fields json.RawMessage
}

func (UnknownEvent) typedEvent() {}

func decodeTypedEvent(event Event) (TypedEvent, error) {
	var value TypedEvent
	switch event.Kind {
	case "text_delta":
		value = &TextDelta{}
	case "reasoning_delta":
		value = &ReasoningDelta{}
	case "tool_start":
		value = &ToolStart{}
	case "tool_input_delta":
		value = &ToolInputDelta{}
	case "tool_done":
		value = &ToolDone{}
	case "token_usage":
		value = &TokenUsage{}
	case "turn_done":
		value = &TurnDone{}
	case "permission_request":
		value = &PermissionRequest{}
	case "model_info":
		value = &ModelInfo{}
	default:
		return UnknownEvent{Kind: event.Kind, Fields: event.Fields}, nil
	}
	if err := event.Decode(value); err != nil {
		return nil, err
	}
	return value, nil
}
