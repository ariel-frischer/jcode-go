package protocol

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
)

const (
	APIVersionMajor = 1
	APIVersionMinor = 0
	DefaultClient   = "jcode-sdk-go"
)

// IsCompatibleVersion accepts the current major protocol version and future
// minor revisions. Unknown major versions are rejected because their framing
// and request semantics may not be safe to assume.
func IsCompatibleVersion(version int) bool { return version == APIVersionMajor }

var (
	ErrMalformedFrame = errors.New("malformed protocol frame")
	ErrInvalidFrame   = errors.New("invalid protocol frame")
	ErrFrameTooLarge  = errors.New("frame too large")
)

type RawRequest struct {
	Req    string
	Fields json.RawMessage
}

func NewRawRequest(req string, fields any) (RawRequest, error) {
	if req == "" {
		return RawRequest{}, fmt.Errorf("request tag is empty: %w", ErrInvalidFrame)
	}
	payload, err := json.Marshal(fields)
	if err != nil {
		return RawRequest{}, fmt.Errorf("marshal %s fields: %w", req, err)
	}
	if len(payload) == 0 || payload[0] != '{' {
		return RawRequest{}, fmt.Errorf("request fields must be an object: %w", ErrInvalidFrame)
	}
	return RawRequest{Req: req, Fields: payload}, nil
}

func (r RawRequest) MarshalJSON() ([]byte, error) {
	if r.Req == "" || len(r.Fields) == 0 || r.Fields[0] != '{' {
		return nil, fmt.Errorf("invalid request %q: %w", r.Req, ErrInvalidFrame)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(r.Fields, &fields); err != nil {
		return nil, fmt.Errorf("request fields: %w", err)
	}
	fields["req"], _ = json.Marshal(r.Req)
	return json.Marshal(fields)
}

type ClientFrame struct {
	V       int
	ID      uint64
	Request RawRequest
}

func (f ClientFrame) MarshalJSON() ([]byte, error) {
	request, err := f.Request.MarshalJSON()
	if err != nil {
		return nil, err
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(request, &object); err != nil {
		return nil, err
	}
	object["v"], _ = json.Marshal(f.V)
	object["id"], _ = json.Marshal(f.ID)
	return json.Marshal(object)
}

type Event interface{ event() }
type HelloOK struct {
	Version      int
	Server       string
	Capabilities []string
}

func (HelloOK) event() {}

type OK struct{}

func (OK) event() {}

type Error struct {
	Code         string
	Message      string
	ProviderCode string
}

func (Error) event() {}

type RawEvent struct {
	Kind   string
	Fields json.RawMessage
}

func (RawEvent) event() {}

type UnknownEvent struct {
	Kind   string
	Fields json.RawMessage
}

func (UnknownEvent) event() {}

type ServerFrame struct {
	V       int
	ReplyTo *uint64
	Event   Event
}

var knownEvents = map[string]struct{}{
	"hello_ok": {}, "ok": {}, "error": {}, "sessions": {}, "attached": {}, "history": {}, "pong": {},
	"text_delta": {}, "reasoning_delta": {}, "reasoning_done": {}, "tool_start": {}, "tool_input_delta": {},
	"tool_exec": {}, "tool_done": {}, "token_usage": {}, "turn_done": {}, "background_progress": {},
	"message_accepted": {}, "permission_request": {}, "session_status": {}, "model_info": {}, "models": {},
	"runtime_info": {}, "credential_updated": {}, "file_content": {}, "files": {}, "text_matches": {},
	"file_status": {}, "compacted": {}, "connection_phase": {}, "session_renamed": {},
}

func IsKnownEvent(kind string) bool { _, ok := knownEvents[kind]; return ok }

func DecodeServerFrame(data []byte) (ServerFrame, error) {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(data, &object); err != nil {
		return ServerFrame{}, fmt.Errorf("decode server frame: %w: %v", ErrMalformedFrame, err)
	}
	if object == nil {
		return ServerFrame{}, fmt.Errorf("server frame is not an object: %w", ErrInvalidFrame)
	}
	var frame ServerFrame
	if err := json.Unmarshal(object["v"], &frame.V); err != nil || frame.V <= 0 {
		return ServerFrame{}, fmt.Errorf("missing or invalid v: %w", ErrInvalidFrame)
	}
	if raw, ok := object["reply_to"]; ok && string(raw) != "null" {
		var id uint64
		if err := json.Unmarshal(raw, &id); err != nil {
			return ServerFrame{}, fmt.Errorf("invalid reply_to: %w", ErrInvalidFrame)
		}
		frame.ReplyTo = &id
	}
	var kind string
	if err := json.Unmarshal(object["ev"], &kind); err != nil || kind == "" {
		return ServerFrame{}, fmt.Errorf("missing event tag: %w", ErrInvalidFrame)
	}
	fields := make(map[string]json.RawMessage, len(object))
	for key, value := range object {
		if key != "v" && key != "reply_to" && key != "ev" {
			fields[key] = value
		}
	}
	payload, _ := json.Marshal(fields)
	switch kind {
	case "hello_ok":
		var value struct {
			Version      int      `json:"version"`
			Server       string   `json:"server"`
			Capabilities []string `json:"capabilities"`
		}
		if err := json.Unmarshal(data, &value); err != nil {
			return ServerFrame{}, fmt.Errorf("decode hello_ok: %w", err)
		}
		frame.Event = HelloOK{Version: value.Version, Server: value.Server, Capabilities: value.Capabilities}
	case "ok":
		frame.Event = OK{}
	case "error":
		var value struct {
			Code         string `json:"code"`
			Message      string `json:"message"`
			ProviderCode string `json:"provider_code"`
		}
		if err := json.Unmarshal(data, &value); err != nil {
			return ServerFrame{}, fmt.Errorf("decode error event: %w", err)
		}
		frame.Event = Error{
			Code:         value.Code,
			Message:      value.Message,
			ProviderCode: value.ProviderCode,
		}
	default:
		if IsKnownEvent(kind) {
			frame.Event = RawEvent{Kind: kind, Fields: payload}
		} else {
			frame.Event = UnknownEvent{Kind: kind, Fields: payload}
		}
	}
	return frame, nil
}

func EncodeServerFrame(v int, replyTo *uint64, kind string, fields map[string]any) ([]byte, error) {
	if v <= 0 || kind == "" {
		return nil, fmt.Errorf("invalid server frame: %w", ErrInvalidFrame)
	}
	object := map[string]any{"v": v, "ev": kind}
	if replyTo != nil {
		object["reply_to"] = *replyTo
	}
	for key, value := range fields {
		if key != "v" && key != "ev" && key != "reply_to" {
			object[key] = value
		}
	}
	return json.Marshal(object)
}
func FieldsJSON(event Event) (json.RawMessage, bool) {
	switch value := event.(type) {
	case RawEvent:
		return bytes.Clone(value.Fields), true
	case UnknownEvent:
		return bytes.Clone(value.Fields), true
	default:
		return nil, false
	}
}
