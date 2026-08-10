package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"

	jcode "github.com/ariel-frischer/jcode-go"
)

// Example: attach to an existing session and consume its compatible typed Events
// API. This does not own or cancel a turn started elsewhere.
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
	session, err := client.AttachSession(ctx, sessionID)
	if err != nil {
		return err
	}
	stream := session.Events(ctx)
	defer stream.Close()
	for {
		event, err := stream.Next(ctx)
		if err != nil {
			return fmt.Errorf("event stream: %w", err)
		}
		switch value := event.(type) {
		case *jcode.TextDelta:
			_, _ = io.WriteString(os.Stdout, value.Text)
		case *jcode.PermissionRequest:
			// Apply an explicit product policy. Never auto-approve by default.
		case *jcode.TurnDone:
			fmt.Println()
			return nil
		default:
			// Unknown kinds remain forward-compatible. Log only the type or stable
			// kind, never raw fields that may contain model or tool content.
			if unknown, ok := value.(jcode.UnknownEvent); ok {
				fmt.Fprintf(os.Stderr, "event=%s\n", unknown.Kind)
			}
		}
	}
}
