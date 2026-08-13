package protocol

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// TestRustSchemaParity keeps the hand-written Go tag classifier aligned with
// the canonical Rust harness contract. It intentionally reads source rather
// than generated output so it works in a checkout without Rust build artifacts.
func TestRustSchemaParity(t *testing.T) {
	root := repositoryRoot(t)
	for _, tc := range []struct {
		file string
		name string
		got  []string
	}{
		{"crates/jcode-harness-api/src/requests.rs", "ApiRequest", requestTags()},
		{"crates/jcode-harness-api/src/events.rs", "ApiEvent", eventTags()},
	} {
		want := rustEnumTags(t, filepath.Join(root, tc.file), tc.name)
		got := append([]string(nil), tc.got...)
		sort.Strings(got)
		sort.Strings(want)
		gotJSON, _ := json.Marshal(got)
		wantJSON, _ := json.Marshal(want)
		if !bytes.Equal(gotJSON, wantJSON) {
			t.Fatalf("%s drifted: go=%v rust=%v", tc.name, got, want)
		}
	}
}

func TestProtocolFixtureParity(t *testing.T) {
	requests := requestTags()
	if len(requests) != 31 {
		t.Fatalf("request fixture count=%d, want 31", len(requests))
	}
	for _, tag := range requests {
		req, err := NewRawRequest(tag, map[string]any{"fixture": true})
		if err != nil {
			t.Fatalf("%s: NewRawRequest: %v", tag, err)
		}
		wire, err := json.Marshal(ClientFrame{V: APIVersionMajor, ID: 42, Request: req})
		if err != nil {
			t.Fatalf("%s: marshal: %v", tag, err)
		}
		var object map[string]any
		if err := json.Unmarshal(wire, &object); err != nil || object["req"] != tag || object["id"] != float64(42) {
			t.Fatalf("%s: invalid fixture wire %s", tag, wire)
		}
	}
	for _, tag := range eventTags() {
		wire, err := EncodeServerFrame(1, nil, tag, map[string]any{"fixture": true})
		if err != nil {
			t.Fatalf("%s: encode: %v", tag, err)
		}
		frame, err := DecodeServerFrame(wire)
		if err != nil {
			t.Fatalf("%s: decode: %v", tag, err)
		}
		if frame.Event == nil || eventKindForTest(frame.Event) != tag {
			t.Fatalf("%s: decoded %#v", tag, frame.Event)
		}
	}
}

func requestTags() []string {
	return []string{"hello", "list_sessions", "archive_session", "restore_session", "set_retention_policy", "create_session", "attach_session", "detach_session", "send_message", "cancel", "soft_interrupt", "get_history", "peek_session", "clear", "rewind", "permission_response", "list_models", "get_runtime_info", "set_api_key", "clear_api_key", "read_file", "find_files", "search_text", "file_status", "set_model", "set_reasoning_effort", "compact", "rename_session", "rewind_undo", "cancel_soft_interrupts", "ping"}
}

func eventTags() []string {
	return []string{"hello_ok", "ok", "error", "sessions", "attached", "history", "pong", "text_delta", "reasoning_delta", "reasoning_done", "tool_start", "tool_input_delta", "tool_exec", "tool_done", "token_usage", "turn_done", "background_progress", "message_accepted", "permission_request", "session_status", "model_info", "models", "runtime_info", "connection_phase", "credential_updated", "file_content", "files", "text_matches", "file_status", "compacted", "session_renamed"}
}

func eventKindForTest(event Event) string {
	switch value := event.(type) {
	case HelloOK:
		return "hello_ok"
	case OK:
		return "ok"
	case Error:
		return "error"
	case RawEvent:
		return value.Kind
	case UnknownEvent:
		return value.Kind
	default:
		return ""
	}
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "Cargo.toml")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Skip("protocol parity requires a repository checkout")
		}
		dir = parent
	}
}

func rustEnumTags(t *testing.T, file, name string) []string {
	t.Helper()
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	source := string(data)
	start := strings.Index(source, "pub enum "+name+" {")
	if start < 0 {
		t.Fatalf("enum %s not found in %s", name, file)
	}
	body := source[start:]
	if end := strings.Index(body, "\n}"); end >= 0 {
		body = body[:end]
	}
	re := regexp.MustCompile(`(?m)^    ([A-Z][A-Za-z0-9]*)\s*[{(,]`)
	var tags []string
	for _, match := range re.FindAllStringSubmatch(body, -1) {
		if match[1] == "Unknown" {
			continue
		}
		var b strings.Builder
		for i, r := range match[1] {
			if i > 0 && r >= 'A' && r <= 'Z' {
				b.WriteByte('_')
			}
			b.WriteRune(r)
		}
		tags = append(tags, strings.ToLower(b.String()))
	}
	return tags
}
