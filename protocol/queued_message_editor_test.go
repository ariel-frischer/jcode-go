package protocol

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
)

func TestQueuedMessageEditorRequestWireContract(t *testing.T) {
	tests := []struct {
		name      string
		operation QueuedMessageEditorOperation
		want      string
	}{
		{
			name:      "start",
			operation: QueuedMessageEditorOperation{Kind: QueuedMessageEditorStart},
			want:      `{"id":7,"navigation_session_id":"nav-1","operation":{"kind":"start"},"operation_id":"op-1","req":"queued_message_editor","session_id":"session-1","v":1}`,
		},
		{
			name: "move older with ordered images",
			operation: QueuedMessageEditorOperation{
				Kind:              QueuedMessageEditorMove,
				Direction:         QueuedMessageEditorOlder,
				SelectedMessageID: "message-2",
				Draft: &QueuedMessageEditorDraft{
					Content: "edited",
					Images:  [][2]string{{"first.png", "image/png"}, {"second.jpg", "image/jpeg"}},
				},
			},
			want: `{"id":7,"navigation_session_id":"nav-1","operation":{"kind":"move","direction":"older","selected_message_id":"message-2","draft":{"content":"edited","images":[["first.png","image/png"],["second.jpg","image/jpeg"]]}},"operation_id":"op-1","req":"queued_message_editor","session_id":"session-1","v":1}`,
		},
		{
			name: "finish omits empty images",
			operation: QueuedMessageEditorOperation{
				Kind:              QueuedMessageEditorFinish,
				SelectedMessageID: "message-3",
				Draft:             &QueuedMessageEditorDraft{Content: "final"},
			},
			want: `{"id":7,"navigation_session_id":"nav-1","operation":{"kind":"finish","selected_message_id":"message-3","draft":{"content":"final"}},"operation_id":"op-1","req":"queued_message_editor","session_id":"session-1","v":1}`,
		},
		{
			name:      "release",
			operation: QueuedMessageEditorOperation{Kind: QueuedMessageEditorRelease},
			want:      `{"id":7,"navigation_session_id":"nav-1","operation":{"kind":"release"},"operation_id":"op-1","req":"queued_message_editor","session_id":"session-1","v":1}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request, err := NewRawRequest(QueuedMessageEditorRequestTag, QueuedMessageEditorRequest{
				SessionID:           "session-1",
				NavigationSessionID: "nav-1",
				OperationID:         "op-1",
				Operation:           test.operation,
			})
			if err != nil {
				t.Fatal(err)
			}
			wire, err := json.Marshal(ClientFrame{V: APIVersionMajor, ID: 7, Request: request})
			if err != nil {
				t.Fatal(err)
			}
			if string(wire) != test.want {
				t.Fatalf("wire=%s\nwant=%s", wire, test.want)
			}
		})
	}
}

func TestQueuedMessageEditorResultWireContract(t *testing.T) {
	wire := []byte(`{"v":1,"ev":"queued_message_editor_result","session_id":"session-1","navigation_session_id":"nav-1","operation_id":"op-2","outcome":"stale_placement","selection":{"message_id":"message-2","content":"edited","images":[["first.png","image/png"],["second.jpg","image/jpeg"]],"older_available":true,"newer_available":false},"placement":"stale_best_effort","message":"queue changed"}`)
	frame, err := DecodeServerFrame(wire)
	if err != nil {
		t.Fatal(err)
	}
	if EventKind(frame.Event) != QueuedMessageEditorResultEventTag {
		t.Fatalf("event kind=%q, want %q", EventKind(frame.Event), QueuedMessageEditorResultEventTag)
	}
	fields, ok := FieldsJSON(frame.Event)
	if !ok {
		t.Fatalf("event=%#v has no raw fields", frame.Event)
	}
	var result QueuedMessageEditorResult
	if err := json.Unmarshal(fields, &result); err != nil {
		t.Fatal(err)
	}
	want := QueuedMessageEditorResult{
		SessionID:           "session-1",
		NavigationSessionID: "nav-1",
		OperationID:         "op-2",
		Outcome:             QueuedMessageEditorStalePlacement,
		Selection: &QueuedMessageEditorSelection{
			MessageID:      "message-2",
			Content:        "edited",
			Images:         [][2]string{{"first.png", "image/png"}, {"second.jpg", "image/jpeg"}},
			OlderAvailable: true,
			NewerAvailable: false,
		},
		Placement: QueuedMessageEditorPlacementStaleBestEffort,
		Message:   stringPointer("queue changed"),
	}
	if !reflect.DeepEqual(result, want) {
		t.Fatalf("result=%#v\nwant=%#v", result, want)
	}
}

func stringPointer(value string) *string { return &value }

func TestQueuedMessageEditorOptionalResultFieldsAreAdditive(t *testing.T) {
	result := QueuedMessageEditorResult{
		SessionID:           "session-1",
		NavigationSessionID: "nav-1",
		OperationID:         "op-3",
		Outcome:             QueuedMessageEditorReleased,
		Placement:           QueuedMessageEditorPlacementNotApplied,
	}
	wire, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(wire), `{"session_id":"session-1","navigation_session_id":"nav-1","operation_id":"op-3","outcome":"released","placement":"not_applied"}`; got != want {
		t.Fatalf("wire=%s\nwant=%s", got, want)
	}
}

func TestQueuedMessageEditorRejectsInvalidOperationShapes(t *testing.T) {
	tests := []QueuedMessageEditorOperation{
		{},
		{Kind: QueuedMessageEditorMove, Direction: "sideways", Draft: &QueuedMessageEditorDraft{}},
		{Kind: QueuedMessageEditorMove, Direction: QueuedMessageEditorOlder},
		{Kind: QueuedMessageEditorFinish},
	}
	for _, operation := range tests {
		if _, err := json.Marshal(operation); !errors.Is(err, ErrInvalidFrame) {
			t.Fatalf("operation=%+v error=%v, want ErrInvalidFrame", operation, err)
		}
	}
}
