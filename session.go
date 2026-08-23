package jcode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/ariel-frischer/jcode-go/protocol"
)

// ErrInvalidOptions classifies validation failures in typed SDK options.
var ErrInvalidOptions = errors.New("invalid options")

// OptionError identifies the typed option field that failed validation.
// It intentionally retains no rejected value.
type OptionError struct {
	Field string
}

func (e *OptionError) Error() string {
	return fmt.Sprintf("invalid option %s", e.Field)
}

func (*OptionError) Unwrap() error { return ErrInvalidOptions }

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
	Profile    string
}

type SendOptions struct {
	Images      [][2]string
	NoReply     bool
	MaxTurns    int
	TokenBudget int
	Deadline    string
}

type sendMessageFields struct {
	SessionID   string      `json:"session_id"`
	Content     string      `json:"content"`
	Images      [][2]string `json:"images,omitempty"`
	NoReply     bool        `json:"no_reply,omitempty"`
	MaxTurns    int         `json:"max_turns,omitempty"`
	TokenBudget int         `json:"token_budget,omitempty"`
	Deadline    string      `json:"deadline,omitempty"`
}

// EventError reports a terminal error emitted by the harness for a session
// turn. It preserves the protocol code and message for callers that need to
// classify provider failures without inspecting raw protocol frames.
type EventError struct {
	Code         string
	Message      string
	ProviderCode string
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
	if err := validateCreateSessionOptions(options); err != nil {
		return Session{}, err
	}
	c.emit(Observation{Kind: "create_session_start", Request: "create_session"})
	req, err := protocol.NewRawRequest("create_session", struct {
		WorkingDir string `json:"working_dir,omitempty"`
		Profile    string `json:"profile,omitempty"`
	}{options.WorkingDir, options.Profile})
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

func validateCreateSessionOptions(options CreateSessionOptions) error {
	if options.Profile != "" && strings.TrimSpace(options.Profile) == "" {
		return &OptionError{Field: "profile"}
	}
	return nil
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
	if err := validateSendOptions(options, time.Now()); err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	req, err := newSendMessageRequest(s.ID, content, options)
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
	if err := validateSendOptions(options, time.Now()); err != nil {
		return nil, err
	}
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
	s.client.emit(Observation{Kind: "turn_start"})

	req, err := newSendMessageRequest(s.ID, content, options)
	if err != nil {
		return nil, fmt.Errorf("encode turn message: %w", err)
	}

	// The Turn owns this single subscription until its first terminal result.
	// The internal subscription reserves slots for acceptance and the event that
	// proves the public Turn queue has reached its configured bound.
	subscription := s.client.subscribeTurn(s.ID, s.client.options.EventBuffer+2)
	turn := newTurn(s.client, s.ID, subscription)
	turn.start(lifecycleCtx)
	if err := s.client.Notify(req); err != nil {
		result := turnResultFromSubscriptionError(err)
		wrapped := fmt.Errorf("send turn message: %w", result.Err)
		turn.finishTerminal(result)
		return nil, wrapped
	}
	turn.markWriteComplete()
	return turn, nil
}

func validateSendOptions(options SendOptions, submissionTime time.Time) error {
	if options.MaxTurns < 0 {
		return &OptionError{Field: "max_turns"}
	}
	if options.TokenBudget < 0 {
		return &OptionError{Field: "token_budget"}
	}
	if options.Deadline == "" {
		return nil
	}
	deadline, err := time.Parse(time.RFC3339, options.Deadline)
	if err != nil || !deadline.After(submissionTime) {
		return &OptionError{Field: "deadline"}
	}
	return nil
}

func newSendMessageRequest(sessionID, content string, options SendOptions) (protocol.RawRequest, error) {
	images := make([][2]string, len(options.Images))
	copy(images, options.Images)
	return protocol.NewRawRequest("send_message", sendMessageFields{
		SessionID:   sessionID,
		Content:     content,
		Images:      images,
		NoReply:     options.NoReply,
		MaxTurns:    options.MaxTurns,
		TokenBudget: options.TokenBudget,
		Deadline:    options.Deadline,
	})
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
		return nil, EventError{
			Code:         value.Code,
			Message:      value.Message,
			ProviderCode: value.ProviderCode,
		}
	}
	return decodeTypedEvent(event)
}

func (s *TypedEventStream) Close() { s.subscription.Close() }

type TypedEvent interface{ typedEvent() }

// EventSemanticClass is the closed handling policy for an owned-turn event.
type EventSemanticClass string

const (
	EventSemanticClassContentProgress   EventSemanticClass = "content_progress"
	EventSemanticClassAdvisoryLifecycle EventSemanticClass = "advisory_lifecycle"
	EventSemanticClassTerminal          EventSemanticClass = "terminal"
	EventSemanticClassPermission        EventSemanticClass = "permission"
	EventSemanticClassToolEffect        EventSemanticClass = "tool_effect"
	// Short aliases keep handler switches readable while the prefixed names
	// remain unambiguous in generated documentation and downstream code.
	SemanticClassContentProgress   = EventSemanticClassContentProgress
	SemanticClassAdvisoryLifecycle = EventSemanticClassAdvisoryLifecycle
	SemanticClassTerminal          = EventSemanticClassTerminal
	SemanticClassPermission        = EventSemanticClassPermission
	SemanticClassToolEffect        = EventSemanticClassToolEffect
)

const maxCompatibilityDiagnosticBytes = 256
const maxCompatibilityIdentifierBytes = 96

// CompatibilityError is a payload-free, bounded failure for an unsupported or
// semantically unclassified owned-turn event.
type CompatibilityError struct {
	Kind      string
	EventType string
}

func (e *CompatibilityError) Error() string {
	if e == nil {
		return "unsupported_typed_event"
	}
	return boundedCompatibilityMessage(e.Kind, e.EventType)
}

func (*CompatibilityError) Unwrap() error { return ErrProtocolFailure }

func newCompatibilityError(kind, eventType, _ string) *CompatibilityError {
	return &CompatibilityError{
		Kind:      sanitizeCompatibilityIdentifier(kind, "unknown_kind"),
		EventType: sanitizeCompatibilityIdentifier(eventType, "unknown_type"),
	}
}

func boundedCompatibilityMessage(kind, eventType string) string {
	message := "unsupported_typed_event: kind=" + sanitizeCompatibilityIdentifier(kind, "unknown_kind") +
		" type=" + sanitizeCompatibilityIdentifier(eventType, "unknown_type")
	return truncateCompatibilityUTF8(message, maxCompatibilityDiagnosticBytes)
}

func sanitizeCompatibilityIdentifier(raw, fallback string) string {
	var builder strings.Builder
	for _, r := range raw {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '-' || r == '.' {
			builder.WriteRune(r)
		} else {
			builder.WriteByte('_')
		}
	}
	value := builder.String()
	if value == "" || strings.Trim(value, "_") == "" {
		value = fallback
	}
	return truncateCompatibilityUTF8(value, maxCompatibilityIdentifierBytes)
}

func truncateCompatibilityUTF8(value string, maxBytes int) string {
	if len(value) <= maxBytes {
		return value
	}
	end := maxBytes
	for end > 0 && (value[end]&0xc0) == 0x80 {
		end--
	}
	return value[:end]
}

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

type ReasoningDone struct {
	SessionID    string  `json:"session_id"`
	DurationSecs float64 `json:"duration_secs,omitempty"`
}

func (ReasoningDone) typedEvent() {}

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

type ToolExec struct {
	SessionID string `json:"session_id"`
	CallID    string `json:"call_id"`
	Name      string `json:"name"`
}

func (ToolExec) typedEvent() {}

type ToolDone struct {
	SessionID string `json:"session_id"`
	CallID    string `json:"call_id"`
	Name      string `json:"name"`
	Output    string `json:"output"`
	Error     string `json:"error,omitempty"`
}

func (ToolDone) typedEvent() {}

// RenderedImageSource identifies where a rendered image originated.
type RenderedImageSource struct {
	Kind     string `json:"kind"`
	ToolName string `json:"tool_name,omitempty"`
	Role     string `json:"role,omitempty"`
}

// RenderedImageAnchor identifies where a rendered image belongs in a transcript.
type RenderedImageAnchor struct {
	Kind    string `json:"kind"`
	ID      string `json:"id,omitempty"`
	Ordinal uint64 `json:"ordinal,omitempty"`
}

// RenderedImage is an image and its optional transcript placement metadata.
type RenderedImage struct {
	MediaType string               `json:"media_type"`
	Data      string               `json:"data"`
	Label     *string              `json:"label,omitempty"`
	Source    RenderedImageSource  `json:"source"`
	Anchor    *RenderedImageAnchor `json:"anchor,omitempty"`
}

// SidePaneImages reports images produced for the attached session.
type SidePaneImages struct {
	SessionID string          `json:"session_id"`
	Images    []RenderedImage `json:"images"`
}

func (SidePaneImages) typedEvent() {}

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

type BackgroundProgress struct {
	SessionID string  `json:"session_id"`
	TaskID    string  `json:"task_id"`
	Label     string  `json:"label"`
	Percent   float64 `json:"percent,omitempty"`
	Summary   string  `json:"summary"`
	Done      bool    `json:"done,omitempty"`
}

func (BackgroundProgress) typedEvent() {}

type MessageAccepted struct {
	SessionID string `json:"session_id"`
}

func (MessageAccepted) typedEvent() {}

type SessionStatus struct {
	SessionID string `json:"session_id"`
	Status    string `json:"status"`
}

func (SessionStatus) typedEvent() {}

type ConnectionPhase struct {
	SessionID string `json:"session_id"`
	Phase     string `json:"phase"`
}

func (ConnectionPhase) typedEvent() {}

type ModelInfo struct {
	SessionID       string `json:"session_id"`
	Provider        string `json:"provider,omitempty"`
	Model           string `json:"model,omitempty"`
	ReasoningEffort string `json:"reasoning_effort,omitempty"`
}

func (ModelInfo) typedEvent() {}

// ModelRouteInfo describes one provider route exposed by the runtime.
type ModelRouteInfo struct {
	Model     string `json:"model"`
	Provider  string `json:"provider"`
	APIMethod string `json:"api_method"`
	Available bool   `json:"available"`
	Detail    string `json:"detail"`
}

// Models reports the models available to a session and its current model.
type Models struct {
	SessionID string   `json:"session_id"`
	Models    []string `json:"models"`
	Current   string   `json:"current,omitempty"`
}

func (Models) typedEvent() {}

// RuntimeInfo reports provider identity and every route exposed by the runtime.
type RuntimeInfo struct {
	SessionID       string           `json:"session_id"`
	Provider        string           `json:"provider,omitempty"`
	Model           string           `json:"model,omitempty"`
	ReasoningEffort string           `json:"reasoning_effort,omitempty"`
	Routes          []ModelRouteInfo `json:"routes"`
}

func (RuntimeInfo) typedEvent() {}

// CredentialUpdated reports whether a provider credential is configured.
type CredentialUpdated struct {
	Provider   string `json:"provider"`
	Configured bool   `json:"configured"`
}

func (CredentialUpdated) typedEvent() {}

// FileContent is the result of reading a file through the harness.
type FileContent struct {
	SessionID string `json:"session_id"`
	Path      string `json:"path"`
	Content   string `json:"content"`
	Size      uint64 `json:"size"`
	Truncated bool   `json:"truncated"`
}

func (FileContent) typedEvent() {}

// Files is the result of finding files through the harness.
type Files struct {
	SessionID string   `json:"session_id"`
	Paths     []string `json:"paths"`
}

func (Files) typedEvent() {}

// TextMatch identifies one text search result.
type TextMatch struct {
	Path    string `json:"path"`
	Line    uint32 `json:"line"`
	Column  uint32 `json:"column"`
	Preview string `json:"preview"`
}

// TextMatches is the result of searching text through the harness.
type TextMatches struct {
	SessionID string      `json:"session_id"`
	Matches   []TextMatch `json:"matches"`
}

func (TextMatches) typedEvent() {}

// FileStatus reports file existence and optional metadata.
type FileStatus struct {
	SessionID  string  `json:"session_id"`
	Path       string  `json:"path"`
	Exists     bool    `json:"exists"`
	Kind       string  `json:"kind"`
	Size       *uint64 `json:"size,omitempty"`
	ModifiedMS *uint64 `json:"modified_ms,omitempty"`
}

func (FileStatus) typedEvent() {}

// Compacted reports that session compaction was scheduled.
type Compacted struct {
	SessionID string `json:"session_id"`
	Message   string `json:"message"`
}

func (Compacted) typedEvent() {}

// SessionRenamed reports a session title change. Title is nil when cleared.
type SessionRenamed struct {
	SessionID    string  `json:"session_id"`
	Title        *string `json:"title,omitempty"`
	DisplayTitle string  `json:"display_title"`
}

func (SessionRenamed) typedEvent() {}

type UnknownEvent struct {
	Kind   string
	Fields json.RawMessage
}

func (UnknownEvent) typedEvent() {}

// SemanticClassOf returns the reviewed class for a known concrete event.
// Unknown, nil, and unclassified values deliberately return false.
func SemanticClassOf(event TypedEvent) (EventSemanticClass, bool) {
	switch event.(type) {
	case TextDelta, *TextDelta, ReasoningDelta, *ReasoningDelta,
		ToolStart, *ToolStart, ToolInputDelta, *ToolInputDelta,
		ToolDone, *ToolDone, SidePaneImages, *SidePaneImages,
		TokenUsage, *TokenUsage:
		return EventSemanticClassContentProgress, true
	case ReasoningDone, *ReasoningDone, BackgroundProgress, *BackgroundProgress,
		MessageAccepted, *MessageAccepted, SessionStatus, *SessionStatus,
		ConnectionPhase, *ConnectionPhase, ModelInfo, *ModelInfo:
		return EventSemanticClassAdvisoryLifecycle, true
	case TurnDone, *TurnDone:
		return EventSemanticClassTerminal, true
	case PermissionRequest, *PermissionRequest:
		return EventSemanticClassPermission, true
	case ToolExec, *ToolExec:
		return EventSemanticClassToolEffect, true
	default:
		return "", false
	}
}

func typedEventType(event TypedEvent) string {
	if event == nil {
		return "unknown_type"
	}
	return reflect.TypeOf(event).String()
}

func decodeTypedEvent(event Event) (TypedEvent, error) {
	var value TypedEvent
	switch event.Kind {
	case "text_delta":
		value = &TextDelta{}
	case "reasoning_delta":
		value = &ReasoningDelta{}
	case "reasoning_done":
		value = &ReasoningDone{}
	case "tool_start":
		value = &ToolStart{}
	case "tool_input_delta":
		value = &ToolInputDelta{}
	case "tool_exec":
		value = &ToolExec{}
	case "tool_done":
		value = &ToolDone{}
	case "side_pane_images":
		value = &SidePaneImages{}
	case "token_usage":
		value = &TokenUsage{}
	case "turn_done":
		value = &TurnDone{}
	case "background_progress":
		value = &BackgroundProgress{}
	case "message_accepted":
		value = &MessageAccepted{}
	case "permission_request":
		value = &PermissionRequest{}
	case "session_status":
		value = &SessionStatus{}
	case "connection_phase":
		value = &ConnectionPhase{}
	case "model_info":
		value = &ModelInfo{}
	case "models":
		value = &Models{}
	case "runtime_info":
		value = &RuntimeInfo{}
	case "credential_updated":
		value = &CredentialUpdated{}
	case "file_content":
		value = &FileContent{}
	case "files":
		value = &Files{}
	case "text_matches":
		value = &TextMatches{}
	case "file_status":
		value = &FileStatus{}
	case "compacted":
		value = &Compacted{}
	case "session_renamed":
		value = &SessionRenamed{}
	default:
		return UnknownEvent{Kind: event.Kind, Fields: event.Fields}, nil
	}
	if len(event.Fields) == 0 || string(event.Fields) == "null" || event.Fields[0] != '{' {
		return nil, newCompatibilityError(event.Kind, typedEventType(value), "")
	}
	if err := event.Decode(value); err != nil {
		return nil, newCompatibilityError(event.Kind, typedEventType(value), "")
	}
	return value, nil
}
