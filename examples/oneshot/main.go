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

// Example: connect to the user's bridge, create one session, stream one turn.
// Run `jcode api-bridge` first. This is a compile-checkable example; its
// endpoint is intentionally supplied by the environment for portability.
func main() {
	if err := run(context.Background(), os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
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
		return fmt.Errorf("connect to %s: %w", socketPath, err)
	}
	client, err := jcode.NewClient(ctx, conn, jcode.Options{ClientName: "go-oneshot-example/0.1"})
	if err != nil {
		return err
	}
	defer client.Close()

	create, err := protocol.NewRawRequest("create_session", map[string]any{"working_dir": mustCWD()})
	if err != nil {
		return err
	}
	frame, err := client.Request(ctx, create)
	if err != nil {
		return err
	}
	sessionID, err := sessionID(frame)
	if err != nil {
		return err
	}
	sub := client.Subscribe(sessionID)
	defer sub.Close()

	prompt := "Say hello in five words."
	if len(args) > 0 {
		prompt = args[0]
	}
	message, err := protocol.NewRawRequest("send_message", map[string]any{"session_id": sessionID, "content": prompt})
	if err != nil {
		return err
	}
	if _, err := client.Request(ctx, message); err != nil {
		return err
	}

	for {
		event, err := sub.Next(ctx)
		if err != nil {
			return err
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
		case "turn_done":
			fmt.Println()
			return nil
		case "error":
			return fmt.Errorf("harness error: %s", event.Frame.Event)
		}
	}
}

func sessionID(frame protocol.ServerFrame) (string, error) {
	fields, ok := protocol.FieldsJSON(frame.Event)
	if !ok {
		return "", fmt.Errorf("session creation returned %T", frame.Event)
	}
	var value struct {
		SessionID string `json:"session_id"`
	}
	if err := json.Unmarshal(fields, &value); err != nil {
		return "", err
	}
	if value.SessionID == "" {
		return "", errors.New("session creation reply did not contain session_id")
	}
	return value.SessionID, nil
}

func mustCWD() string {
	cwd, err := os.Getwd()
	if err != nil {
		return "."
	}
	return cwd
}
