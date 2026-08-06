package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"

	jcode "github.com/ariel-frischer/jcode-go"
	"github.com/ariel-frischer/jcode-go/protocol"
)

// Example: a long-lived service with one event consumer per client.
func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	socketPath := os.Getenv("JCODE_API_SOCKET")
	if socketPath == "" {
		runtimeDir := os.Getenv("XDG_RUNTIME_DIR")
		if runtimeDir == "" {
			return errors.New("set JCODE_API_SOCKET or XDG_RUNTIME_DIR")
		}
		socketPath = filepath.Join(runtimeDir, "jcode-api.sock")
	}
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		return err
	}
	client, err := jcode.NewClient(ctx, conn, jcode.Options{
		ClientName:  "go-streaming-service/0.1",
		EventBuffer: 256,
	})
	if err != nil {
		return err
	}
	defer client.Close()

	// In a real service, sessionID comes from durable application state.
	sessionID := os.Getenv("JCODE_SESSION_ID")
	if sessionID == "" {
		return errors.New("set JCODE_SESSION_ID")
	}
	sub := client.Subscribe(sessionID)
	defer sub.Close()
	for {
		event, err := sub.Next(ctx)
		if err != nil {
			return fmt.Errorf("event stream: %w", err)
		}
		switch event.Kind {
		case "text_delta":
			var value struct {
				Text string `json:"text"`
			}
			if err := event.Decode(&value); err != nil {
				return err
			}
			fmt.Print(value.Text)
		case "permission_request":
			// Apply an explicit product policy. Never auto-approve by default.
		case "turn_done":
			fmt.Println()
			return nil
		default:
			// Unknown event kinds are forward-compatible.
			if fields, ok := protocol.FieldsJSON(event.Frame.Event); ok {
				var metadata map[string]any
				_ = json.Unmarshal(fields, &metadata)
				fmt.Fprintf(os.Stderr, "event=%s fields=%v\n", event.Kind, metadata)
			}
		}
	}
}
