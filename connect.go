package jcode

import (
	"context"
	"os"
	"path/filepath"

	"github.com/ariel-frischer/jcode-go/transport"
)

// ConnectOptions controls attaching to an already-running local harness.
type ConnectOptions struct {
	SocketPath    string
	ClientOptions Options
}

// Connect attaches to a running local Jcode harness. It never starts or stops
// a process, and closing the returned client only closes its socket.
func Connect(ctx context.Context, options ConnectOptions) (*Client, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	socketPath := options.SocketPath
	if socketPath == "" {
		socketPath = apiSocketPath()
	}
	factory := transport.UnixSocket(socketPath)
	tr, err := factory(ctx)
	if err != nil {
		return nil, err
	}
	client, err := NewClient(ctx, tr, options.ClientOptions)
	if err != nil {
		return nil, err
	}
	return client, nil
}

func apiSocketPath() string {
	if value := os.Getenv("JCODE_API_SOCKET"); value != "" {
		return value
	}
	if value := os.Getenv("JCODE_RUNTIME_DIR"); value != "" {
		return filepath.Join(value, "jcode-api.sock")
	}
	if value := os.Getenv("XDG_RUNTIME_DIR"); value != "" {
		return filepath.Join(value, "jcode-api.sock")
	}
	home, err := os.UserHomeDir()
	if err == nil {
		return filepath.Join(home, ".jcode", "run", "jcode-api.sock")
	}
	return filepath.Join(os.TempDir(), "jcode-api.sock")
}
