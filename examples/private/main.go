package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	jcode "github.com/ariel-frischer/jcode-go"
)

// Example: own a private process and isolate its home/socket from the user.
// The exact bridge command is deployment-specific. Verify flags for the jcode
// version you ship before enabling this pattern in production.
func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(parent context.Context) error {
	root, err := os.MkdirTemp("", "jcode-private-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(root)
	home := filepath.Join(root, "home")
	runtimeDir := filepath.Join(root, "run")
	if err := os.MkdirAll(runtimeDir, 0o700); err != nil {
		return err
	}
	socketPath := filepath.Join(runtimeDir, "jcode-api.sock")

	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	cmd := exec.CommandContext(ctx, "jcode", "api-bridge", "--api-socket", socketPath)
	cmd.Env = append([]string{
		"PATH=" + os.Getenv("PATH"),
		"JCODE_HOME=" + home,
		"JCODE_RUNTIME_DIR=" + runtimeDir,
		"JCODE_API_SOCKET=" + socketPath,
		"JCODE_NO_TELEMETRY=1",
	}, "JCODE_CONFIG_DIR=") // Do not inherit the user's config by accident.
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr // Never log this blindly if it may contain secrets.
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start private jcode: %w", err)
	}
	defer func() {
		cancel()
		_ = cmd.Wait()
	}()

	if err := waitForSocket(ctx, socketPath, 10*time.Second); err != nil {
		return err
	}
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		return err
	}
	client, err := jcode.NewClient(ctx, conn, jcode.Options{ClientName: "go-private-example/0.1"})
	if err != nil {
		return err
	}
	defer client.Close()
	fmt.Printf("private client ready; home=%s\n", home)
	return nil
}

func waitForSocket(ctx context.Context, path string, timeout time.Duration) error {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		if _, err := os.Stat(path); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("private socket did not appear: %s", path)
		case <-time.After(50 * time.Millisecond):
		}
	}
}
