package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"time"

	jcode "github.com/ariel-frischer/jcode-go"
)

// Example: connect to the user's bridge, create one session in an explicit
// working directory, and own one complete turn lifecycle.
func main() {
	if err := run(context.Background(), os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(lifecycleCtx context.Context, args []string) error {
	socketPath := os.Getenv("JCODE_API_SOCKET")
	if socketPath == "" {
		runtimeDir := os.Getenv("XDG_RUNTIME_DIR")
		if runtimeDir == "" {
			return errors.New("set JCODE_API_SOCKET or XDG_RUNTIME_DIR")
		}
		socketPath = filepath.Join(runtimeDir, "jcode-api.sock")
	}
	workingDir, err := filepath.Abs(".")
	if err != nil {
		return fmt.Errorf("resolve working directory: %w", err)
	}
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		return fmt.Errorf("connect to %s: %w", socketPath, err)
	}
	client, err := jcode.NewClient(lifecycleCtx, conn, jcode.Options{ClientName: "go-oneshot-example/0.1"})
	if err != nil {
		return err
	}
	defer client.Close()

	session, err := client.CreateSession(lifecycleCtx, jcode.CreateSessionOptions{WorkingDir: workingDir})
	if err != nil {
		return err
	}
	prompt := "Say hello in five words."
	if len(args) > 0 {
		prompt = args[0]
	}
	turn, err := session.StartTurn(lifecycleCtx, prompt, jcode.SendOptions{})
	if err != nil {
		return err
	}

	waitCtx, stopWaiting := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stopWaiting()
	if err := turn.Accepted(waitCtx); err != nil {
		return finishInterruptedTurn(turn, err)
	}
	for {
		event, nextErr := turn.Next(waitCtx)
		if nextErr != nil {
			return finishInterruptedTurn(turn, nextErr)
		}
		switch value := event.(type) {
		case *jcode.TextDelta:
			_, _ = io.WriteString(os.Stdout, value.Text)
		case *jcode.PermissionRequest:
			// Apply an explicit application policy before responding.
		case *jcode.TurnDone:
			fmt.Println()
			return waitForTerminal(turn)
		}
	}
}

func finishInterruptedTurn(turn *jcode.Turn, waitErr error) error {
	// Canceling Accepted/Next/Wait only abandons that local wait. Turn.Cancel is
	// the distinct, at-most-once server-side cancellation request.
	cancelCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	cancelErr := turn.Cancel(cancelCtx)
	cancel()
	return errors.Join(waitErr, cancelErr, waitForTerminal(turn))
}

func waitForTerminal(turn *jcode.Turn) error {
	// Use a fresh bounded context after cancellation. Reusing the interrupted
	// event-wait context would make this wait return immediately.
	waitCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	result, err := turn.Wait(waitCtx)
	if err != nil {
		return err
	}
	return result.Err
}
