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

func TestHandshakeAndErrorClassification(t *testing.T) {
	frame, err := DecodeServerFrame([]byte(`{"v":1,"reply_to":1,"ev":"hello_ok","version":1,"server":"h","capabilities":["events"],"extra":7}`))
	if err != nil {
		t.Fatal(err)
	}
	hello, ok := frame.Event.(HelloOK)
	if !ok || hello.Version != 1 || hello.Server != "h" || len(hello.Capabilities) != 1 {
		t.Fatalf("hello=%#v", frame.Event)
	}
	frame, err = DecodeServerFrame([]byte(`{"v":1,"reply_to":2,"ev":"error","code":"unknown_request","message":"nope"}`))
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := frame.Event.(Error); !ok || got.Code != "unknown_request" {
		t.Fatalf("error=%#v", frame.Event)
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
