package protocol

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
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
	root := jcodeRepositoryRoot(t)
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
	if len(requests) != 32 {
		t.Fatalf("request fixture count=%d, want 32", len(requests))
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

// TestOwnedTurnEventCoverage keeps the public Go event boundary aligned with
// the exhaustive Rust publication contract. The protocol package cannot
// import its parent package without creating a production dependency cycle,
// so this gate checks the canonical source seams directly: Rust's contract
// match, the Go decoder cases, and the Go semantic-class switch.
func TestOwnedTurnEventCoverage(t *testing.T) {
	root := jcodeRepositoryRoot(t)
	rustPath := filepath.Join(root, "crates/jcode-harness-api/src/events.rs")
	goPath := filepath.Join(sdkModuleRoot(t), "session.go")
	rustContracts := rustPublicationContracts(t, rustPath)
	goSourceBytes, err := os.ReadFile(goPath)
	if err != nil {
		t.Fatal(err)
	}
	goSource := string(goSourceBytes)

	if got, want := len(rustContracts), len(rustEnumVariants(t, rustPath, "ApiEvent")); got != want {
		t.Fatalf("Rust ApiEvent publication contract covers %d variants, want %d", got, want)
	}
	for _, problem := range ownedTurnCoverageErrors(rustContracts, goSource, ownedGoEventCoverage) {
		t.Error(problem)
	}
}

func TestOwnedTurnEventCoverageRejectsControlledGaps(t *testing.T) {
	root := jcodeRepositoryRoot(t)
	rustPath := filepath.Join(root, "crates/jcode-harness-api/src/events.rs")
	goSourceBytes, err := os.ReadFile(filepath.Join(sdkModuleRoot(t), "session.go"))
	if err != nil {
		t.Fatal(err)
	}
	rustContracts := rustPublicationContracts(t, rustPath)
	goSource := string(goSourceBytes)

	missing := maps.Clone(ownedGoEventCoverage)
	delete(missing, "connection_phase")
	if problems := ownedTurnCoverageErrors(rustContracts, goSource, missing); len(problems) == 0 {
		t.Fatal("coverage gate accepted an owned event with no Go representation")
	}

	unclassified := maps.Clone(ownedGoEventCoverage)
	unclassified["connection_phase"] = ownedEventCoverage{goType: "ConnectionPhase", class: ""}
	if problems := ownedTurnCoverageErrors(rustContracts, goSource, unclassified); len(problems) == 0 {
		t.Fatal("coverage gate accepted an owned event with no semantic class")
	}

	undeclared := maps.Clone(ownedGoEventCoverage)
	undeclared["synthetic_untyped"] = ownedEventCoverage{goType: "SyntheticUntyped", class: "content_progress"}
	if problems := ownedTurnCoverageErrors(rustContracts, goSource, undeclared); len(problems) == 0 {
		t.Fatal("coverage gate accepted an undeclared Go inventory entry")
	}
}

type ownedEventCoverage struct {
	goType string
	class  string
}

// Keep this inventory next to the parity gate so a new owned Rust event must
// receive a reviewed Go representation and class before the test can pass.
var ownedGoEventCoverage = map[string]ownedEventCoverage{
	"error":               {goType: "EventError", class: "terminal"},
	"text_delta":          {goType: "TextDelta", class: "content_progress"},
	"reasoning_delta":     {goType: "ReasoningDelta", class: "content_progress"},
	"reasoning_done":      {goType: "ReasoningDone", class: "advisory_lifecycle"},
	"tool_start":          {goType: "ToolStart", class: "content_progress"},
	"tool_input_delta":    {goType: "ToolInputDelta", class: "content_progress"},
	"tool_exec":           {goType: "ToolExec", class: "tool_effect"},
	"tool_done":           {goType: "ToolDone", class: "content_progress"},
	"side_pane_images":    {goType: "SidePaneImages", class: "content_progress"},
	"token_usage":         {goType: "TokenUsage", class: "content_progress"},
	"turn_done":           {goType: "TurnDone", class: "terminal"},
	"background_progress": {goType: "BackgroundProgress", class: "advisory_lifecycle"},
	"message_accepted":    {goType: "MessageAccepted", class: "advisory_lifecycle"},
	"permission_request":  {goType: "PermissionRequest", class: "permission"},
	"session_status":      {goType: "SessionStatus", class: "advisory_lifecycle"},
	"connection_phase":    {goType: "ConnectionPhase", class: "advisory_lifecycle"},
	"model_info":          {goType: "ModelInfo", class: "advisory_lifecycle"},
}

func ownedTurnCoverageErrors(rustContracts map[string]rustEventContract, goSource string, goCoverage map[string]ownedEventCoverage) []string {
	var problems []string
	for kind, contract := range rustContracts {
		if !IsKnownEvent(kind) && kind != "unknown" {
			problems = append(problems, fmt.Sprintf("Rust event %q is absent from Go protocol known-event inventory", kind))
		}
		if contract.disposition != "owned" {
			continue
		}
		coverage, ok := goCoverage[kind]
		if !ok {
			problems = append(problems, fmt.Sprintf("owned Rust event %q has no Go typed/classified coverage", kind))
			continue
		}
		if coverage.class == "" {
			problems = append(problems, fmt.Sprintf("owned Rust event %q has no semantic class", kind))
		}
		if coverage.goType == "EventError" {
			if !strings.Contains(goSource, "event.Frame.Event.(protocol.Error)") {
				problems = append(problems, fmt.Sprintf("%s has no typed terminal error path", kind))
			}
			continue
		}
		if kind == "message_accepted" {
			if !strings.Contains(goSource, `event.Kind == "message_accepted"`) {
				problems = append(problems, fmt.Sprintf("%s has no owned-turn acceptance path", kind))
			}
		} else {
			decoderNeedle := fmt.Sprintf("case %q:", kind)
			decoderStart := strings.Index(goSource, decoderNeedle)
			if decoderStart < 0 || !strings.Contains(goSource[decoderStart:], "&"+coverage.goType+"{}") {
				problems = append(problems, fmt.Sprintf("owned Rust event %q is not decoded as *%s", kind, coverage.goType))
			}
		}
		gotClass, count := goSemanticClassForType(goSource, coverage.goType)
		if count != 1 || gotClass != coverage.class {
			problems = append(problems, fmt.Sprintf("Go type %s has semantic classes %q (%d matches), want exactly %q", coverage.goType, gotClass, count, coverage.class))
		}
	}
	for kind := range goCoverage {
		contract, ok := rustContracts[kind]
		if !ok || contract.disposition != "owned" {
			problems = append(problems, fmt.Sprintf("Go owned-event inventory contains undeclared/non-owned event %q", kind))
		}
	}
	return problems
}

type rustEventContract struct {
	disposition string
}

func rustPublicationContracts(t *testing.T, path string) map[string]rustEventContract {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	body := string(data)
	start := strings.Index(body, "match self {")
	if start < 0 {
		t.Fatalf("publication contract match not found in %s", path)
	}
	body = body[start:]
	if end := strings.Index(body, "\n        }\n    }\n}"); end >= 0 {
		body = body[:end]
	}
	arm := regexp.MustCompile(`(?m)^\s*Self::([A-Za-z0-9_]+)(?:\s*\{[^}]*\})?\s*=>\s*(owned|outside|filtered)\("([^"]+)"`)
	contracts := make(map[string]rustEventContract)
	for _, match := range arm.FindAllStringSubmatch(body, -1) {
		contracts[match[3]] = rustEventContract{disposition: match[2]}
	}
	if len(contracts) == 0 {
		t.Fatalf("no publication contract arms found in %s", path)
	}
	return contracts
}

func rustEnumVariants(t *testing.T, path, name string) []string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	source := string(data)
	start := strings.Index(source, "pub enum "+name+" {")
	if start < 0 {
		t.Fatalf("enum %s not found in %s", name, path)
	}
	body := source[start:]
	if end := strings.Index(body, "\n}"); end >= 0 {
		body = body[:end]
	}
	re := regexp.MustCompile(`(?m)^    ([A-Z][A-Za-z0-9]*)\s*[{(,]`)
	matches := re.FindAllStringSubmatch(body, -1)
	variants := make([]string, 0, len(matches))
	for _, match := range matches {
		variants = append(variants, match[1])
	}
	return variants
}

func goSemanticClassForType(source, typeName string) (string, int) {
	start := strings.Index(source, "func SemanticClassOf")
	if start < 0 {
		return "", 0
	}
	body := source[start:]
	group := regexp.MustCompile(`(?s)case ([^:]+):\s*return EventSemanticClass([A-Za-z]+), true`)
	count := 0
	class := ""
	for _, match := range group.FindAllStringSubmatch(body, -1) {
		if strings.Contains(match[1], typeName) {
			count++
			class = semanticClassValue(match[2])
		}
	}
	return class, count
}

func semanticClassValue(name string) string {
	var out strings.Builder
	for i, r := range name {
		if i > 0 && r >= 'A' && r <= 'Z' {
			out.WriteByte('_')
		}
		out.WriteRune(r)
	}
	return strings.ToLower(out.String())
}

func requestTags() []string {
	return []string{"hello", "list_sessions", "archive_session", "restore_session", "set_retention_policy", "create_session", "attach_session", "fork_session", "detach_session", "send_message", "cancel", "soft_interrupt", "get_history", "peek_session", "clear", "rewind", "permission_response", "list_models", "get_runtime_info", "set_api_key", "clear_api_key", "read_file", "find_files", "search_text", "file_status", "set_model", "set_reasoning_effort", "compact", "rename_session", "rewind_undo", "cancel_soft_interrupts", "ping"}
}

func eventTags() []string {
	return []string{"hello_ok", "ok", "error", "sessions", "attached", "session_forked", "history", "pong", "text_delta", "reasoning_delta", "reasoning_done", "tool_start", "tool_input_delta", "tool_exec", "tool_done", "side_pane_images", "token_usage", "turn_done", "wake_requested", "background_progress", "message_accepted", "permission_request", "session_status", "model_info", "models", "runtime_info", "connection_phase", "credential_updated", "file_content", "files", "text_matches", "file_status", "compacted", "session_renamed"}
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

// jcodeRepositoryRoot locates the separate Jcode repository that owns the Rust
// wire contract. The SDK source remains in this module; JCODE_REPO_ROOT is the
// explicit cross-repository integration boundary.
func jcodeRepositoryRoot(t *testing.T) string {
	t.Helper()
	if root := os.Getenv("JCODE_REPO_ROOT"); root != "" {
		root, err := filepath.Abs(root)
		if err != nil {
			t.Fatalf("resolve JCODE_REPO_ROOT: %v", err)
		}
		if _, err := os.Stat(filepath.Join(root, "Cargo.toml")); err != nil {
			t.Fatalf("JCODE_REPO_ROOT does not identify a Jcode checkout: %v", err)
		}
		return root
	}
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

// sdkModuleRoot locates this jcode-go checkout independently of the Jcode wire
// repository. Keeping these roots separate prevents parity validation from
// requiring or recreating an embedded sdk/go source tree.
func sdkModuleRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if module, err := os.ReadFile(filepath.Join(dir, "go.mod")); err == nil {
			if regexp.MustCompile(`(?m)^module[[:space:]]+github\.com/ariel-frischer/jcode-go[[:space:]]*$`).Match(module) {
				return dir
			}
			t.Fatalf("go.mod at %s is not the jcode-go module", dir)
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found")
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
