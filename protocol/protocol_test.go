package protocol

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestFrameRoundTripAndUnknownFields(t *testing.T) {
	req, err := NewRawRequest("create_session", map[string]any{"working_dir": "/tmp", "future": true})
	if err != nil {
		t.Fatal(err)
	}
	var wire bytes.Buffer
	if err := NewEncoder(&wire).Write(ClientFrame{V: APIVersionMajor, ID: 9, Request: req}); err != nil {
		t.Fatal(err)
	}
	frame, err := NewDecoder(&wire).ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	var object map[string]any
	if err := json.Unmarshal(frame, &object); err != nil {
		t.Fatal(err)
	}
	if object["req"] != "create_session" || object["id"] != float64(9) || object["future"] != true {
		t.Fatalf("unexpected wire frame: %s", frame)
	}

	server := `{"v":1,"reply_to":9,"ev":"new_event","value":"kept","future":{"x":1}}`
	decoded, err := DecodeServerFrame([]byte(server))
	if err != nil {
		t.Fatal(err)
	}
	unknown, ok := decoded.Event.(UnknownEvent)
	if !ok || unknown.Kind != "new_event" {
		t.Fatalf("event=%#v", decoded.Event)
	}
	fields, ok := FieldsJSON(decoded.Event)
	if !ok || !strings.Contains(string(fields), `"future"`) {
		t.Fatalf("fields=%s", fields)
	}
	if decoded.ReplyTo == nil || *decoded.ReplyTo != 9 {
		t.Fatalf("reply_to=%v", decoded.ReplyTo)
	}
}

func TestEventKindPreservesKnownAndUnknownProvenance(t *testing.T) {
	known, err := DecodeServerFrame([]byte(`{"v":1,"ev":"connection_phase","session_id":"s","phase":"connecting"}`))
	if err != nil {
		t.Fatal(err)
	}
	if got := EventKind(known.Event); got != "connection_phase" {
		t.Fatalf("known kind=%q, want connection_phase", got)
	}
	unknown, err := DecodeServerFrame([]byte(`{"v":1,"ev":"future_event","payload":"SYNTHETIC_PAYLOAD"}`))
	if err != nil {
		t.Fatal(err)
	}
	if got := EventKind(unknown.Event); got != "future_event" {
		t.Fatalf("unknown kind=%q, want future_event", got)
	}
	fields, ok := FieldsJSON(unknown.Event)
	if !ok || !strings.Contains(string(fields), "SYNTHETIC_PAYLOAD") {
		t.Fatalf("unknown fields=%s, want preserved legacy provenance", fields)
	}

	raw := &RawEvent{Kind: "pointer_raw"}
	unknownValue := &UnknownEvent{Kind: "pointer_unknown"}
	if got := EventKind(&HelloOK{}); got != "hello_ok" {
		t.Fatalf("pointer hello kind=%q, want hello_ok", got)
	}
	if got := EventKind(&OK{}); got != "ok" {
		t.Fatalf("pointer ok kind=%q, want ok", got)
	}
	if got := EventKind(&Error{}); got != "error" {
		t.Fatalf("pointer error kind=%q, want error", got)
	}
	if got := EventKind(raw); got != "pointer_raw" {
		t.Fatalf("pointer raw kind=%q, want pointer_raw", got)
	}
	if got := EventKind(unknownValue); got != "pointer_unknown" {
		t.Fatalf("pointer unknown kind=%q, want pointer_unknown", got)
	}
	var nilRaw *RawEvent
	var nilUnknown *UnknownEvent
	if got := EventKind(nilRaw); got != "" {
		t.Fatalf("nil raw kind=%q, want empty", got)
	}
	if got := EventKind(nilUnknown); got != "" {
		t.Fatalf("nil unknown kind=%q, want empty", got)
	}
}

func TestSidePaneImagesIsKnownEvent(t *testing.T) {
	if !IsKnownEvent("side_pane_images") {
		t.Fatal("side_pane_images is absent from the canonical known-event inventory")
	}
}

func TestHandshakeAndErrorClassification(t *testing.T) {
	frame, err := DecodeServerFrame([]byte(`{"v":1,"reply_to":1,"ev":"hello_ok","version":1,"server":"h","capabilities":["events"],"extra":7}`))
	if err != nil {
		t.Fatal(err)
	}
	hello, ok := frame.Event.(HelloOK)
	if !ok || hello.Version != 1 || hello.Server != "h" || len(hello.Capabilities) != 1 {
		t.Fatalf("hello=%#v", frame.Event)
	}
	frame, err = DecodeServerFrame([]byte(`{"v":1,"reply_to":2,"ev":"error","code":"internal","message":"private provider detail","provider_code":"temporarily_unavailable"}`))
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := frame.Event.(Error); !ok || got.Code != "internal" || got.ProviderCode != "temporarily_unavailable" || got.Message != "private provider detail" {
		t.Fatalf("error=%#v", frame.Event)
	}
}

func TestToolExecRawEventContract(t *testing.T) {
	frame, err := DecodeServerFrame([]byte(`{"v":1,"ev":"tool_exec","session_id":"session_exec","call_id":"call_exec","name":"bash"}`))
	if err != nil {
		t.Fatal(err)
	}
	event, ok := frame.Event.(RawEvent)
	if !ok || event.Kind != "tool_exec" {
		t.Fatalf("event=%#v, want tool_exec RawEvent", frame.Event)
	}
	fieldsJSON, ok := FieldsJSON(event)
	if !ok {
		t.Fatal("tool_exec RawEvent fields unavailable")
	}
	var fields struct {
		SessionID string `json:"session_id"`
		CallID    string `json:"call_id"`
		Name      string `json:"name"`
	}
	if err := json.Unmarshal(fieldsJSON, &fields); err != nil {
		t.Fatal(err)
	}
	if fields.SessionID != "session_exec" || fields.CallID != "call_exec" || fields.Name != "bash" {
		t.Fatalf("fields=%+v, want exact canonical tool_exec values", fields)
	}
}

func TestAPIVersionCompatibilityMatrix(t *testing.T) {
	for _, tc := range []struct {
		version int
		want    bool
	}{
		{0, false},
		{1, true},
		{2, false},
	} {
		if got := IsCompatibleVersion(tc.version); got != tc.want {
			t.Fatalf("version %d compatible=%v, want %v", tc.version, got, tc.want)
		}
	}
}

func TestDecoderBoundariesMalformedAndOversized(t *testing.T) {
	var wire bytes.Buffer
	wire.WriteString(`{"v":1,"ev":"pong"}`)
	wire.WriteByte('\n')
	wire.WriteString(`{"v":1,"ev":"ok"}`)
	wire.WriteByte('\n')
	decoder := NewDecoder(&wire)
	for range 2 {
		if _, err := decoder.ReadFrame(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := decoder.ReadFrame(); !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF, got %v", err)
	}
	bad := NewDecoder(strings.NewReader("not-json\n"))
	if _, err := bad.ReadFrame(); !errors.Is(err, ErrMalformedFrame) {
		t.Fatalf("bad=%v", err)
	}
	large := NewDecoder(strings.NewReader("{\"x\":\"12345\"}\n"))
	large.MaxSize = 5
	if _, err := large.ReadFrame(); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("large=%v", err)
	}
}
