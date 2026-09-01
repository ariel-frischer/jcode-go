package protocol

import (
	"encoding/json"
	"fmt"
)

const (
	QueuedMessageEditorRequestTag     = "queued_message_editor"
	QueuedMessageEditorResultEventTag = "queued_message_editor_result"
	QueuedMessageNavigationCapability = "queued_message_navigation_v1"
)

type QueuedMessageEditorDirection string

const (
	QueuedMessageEditorOlder QueuedMessageEditorDirection = "older"
	QueuedMessageEditorNewer QueuedMessageEditorDirection = "newer"
)

type QueuedMessageEditorDraft struct {
	Content string      `json:"content"`
	Images  [][2]string `json:"images,omitempty"`
}

type QueuedMessageEditorOperationKind string

const (
	QueuedMessageEditorStart   QueuedMessageEditorOperationKind = "start"
	QueuedMessageEditorMove    QueuedMessageEditorOperationKind = "move"
	QueuedMessageEditorFinish  QueuedMessageEditorOperationKind = "finish"
	QueuedMessageEditorRelease QueuedMessageEditorOperationKind = "release"
)

// QueuedMessageEditorOperation is the protocol-v1 tagged operation union.
// Move and Finish require the fields documented on those Rust enum variants;
// Start and Release serialize only their kind.
type QueuedMessageEditorOperation struct {
	Kind              QueuedMessageEditorOperationKind
	Direction         QueuedMessageEditorDirection
	SelectedMessageID string
	Draft             *QueuedMessageEditorDraft
}

func (operation QueuedMessageEditorOperation) MarshalJSON() ([]byte, error) {
	switch operation.Kind {
	case QueuedMessageEditorStart, QueuedMessageEditorRelease:
		return json.Marshal(struct {
			Kind QueuedMessageEditorOperationKind `json:"kind"`
		}{Kind: operation.Kind})
	case QueuedMessageEditorMove:
		if operation.Direction != QueuedMessageEditorOlder && operation.Direction != QueuedMessageEditorNewer {
			return nil, fmt.Errorf("invalid queued-message editor direction %q: %w", operation.Direction, ErrInvalidFrame)
		}
		if operation.Draft == nil {
			return nil, fmt.Errorf("queued-message editor move draft is nil: %w", ErrInvalidFrame)
		}
		return json.Marshal(struct {
			Kind              QueuedMessageEditorOperationKind `json:"kind"`
			Direction         QueuedMessageEditorDirection     `json:"direction"`
			SelectedMessageID string                           `json:"selected_message_id"`
			Draft             QueuedMessageEditorDraft         `json:"draft"`
		}{operation.Kind, operation.Direction, operation.SelectedMessageID, *operation.Draft})
	case QueuedMessageEditorFinish:
		if operation.Draft == nil {
			return nil, fmt.Errorf("queued-message editor finish draft is nil: %w", ErrInvalidFrame)
		}
		return json.Marshal(struct {
			Kind              QueuedMessageEditorOperationKind `json:"kind"`
			SelectedMessageID string                           `json:"selected_message_id"`
			Draft             QueuedMessageEditorDraft         `json:"draft"`
		}{operation.Kind, operation.SelectedMessageID, *operation.Draft})
	default:
		return nil, fmt.Errorf("invalid queued-message editor operation %q: %w", operation.Kind, ErrInvalidFrame)
	}
}

type QueuedMessageEditorRequest struct {
	SessionID           string                       `json:"session_id"`
	NavigationSessionID string                       `json:"navigation_session_id"`
	OperationID         string                       `json:"operation_id"`
	Operation           QueuedMessageEditorOperation `json:"operation"`
}

type QueuedMessageEditorOutcome string

const (
	QueuedMessageEditorStarted        QueuedMessageEditorOutcome = "started"
	QueuedMessageEditorMoved          QueuedMessageEditorOutcome = "moved"
	QueuedMessageEditorBoundary       QueuedMessageEditorOutcome = "boundary"
	QueuedMessageEditorCommitted      QueuedMessageEditorOutcome = "committed"
	QueuedMessageEditorDeleted        QueuedMessageEditorOutcome = "deleted"
	QueuedMessageEditorReleased       QueuedMessageEditorOutcome = "released"
	QueuedMessageEditorStalePlacement QueuedMessageEditorOutcome = "stale_placement"
	QueuedMessageEditorConflict       QueuedMessageEditorOutcome = "conflict"
	QueuedMessageEditorReplay         QueuedMessageEditorOutcome = "replay"
)

type QueuedMessageEditorPlacement string

const (
	QueuedMessageEditorPlacementExact           QueuedMessageEditorPlacement = "exact"
	QueuedMessageEditorPlacementStaleBestEffort QueuedMessageEditorPlacement = "stale_best_effort"
	QueuedMessageEditorPlacementNotApplied      QueuedMessageEditorPlacement = "not_applied"
)

type QueuedMessageEditorSelection struct {
	MessageID      string      `json:"message_id"`
	Content        string      `json:"content"`
	Images         [][2]string `json:"images,omitempty"`
	OlderAvailable bool        `json:"older_available"`
	NewerAvailable bool        `json:"newer_available"`
}

type QueuedMessageEditorResult struct {
	SessionID           string                        `json:"session_id"`
	NavigationSessionID string                        `json:"navigation_session_id"`
	OperationID         string                        `json:"operation_id"`
	Outcome             QueuedMessageEditorOutcome    `json:"outcome"`
	Selection           *QueuedMessageEditorSelection `json:"selection,omitempty"`
	Placement           QueuedMessageEditorPlacement  `json:"placement"`
	Message             *string                       `json:"message,omitempty"`
}
