package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	jcode "github.com/ariel-frischer/jcode-go"
)

// Example: launch and explicitly own one Linux private runtime. LaunchOptions
// and the session use the same absolute worker directory.
func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(lifecycleCtx context.Context) error {
	workingDir, err := filepath.Abs(".")
	if err != nil {
		return fmt.Errorf("resolve working directory: %w", err)
	}
	client, err := jcode.Launch(lifecycleCtx, jcode.LaunchOptions{
		WorkingDir: workingDir,
		ClientOptions: jcode.Options{
			ClientName: "go-private-example/0.1",
		},
	})
	if err != nil {
		return err
	}

	// Detaching transfers private-runtime ownership exactly once. Client.Close is
	// then transport-only, while ShutdownInstance remains responsible for bounded
	// Linux TERM, KILL escalation, reap, and owned-path cleanup.
	instance, ok := client.DetachInstance()
	if !ok {
		return client.Close()
	}

	runErr := runTurn(lifecycleCtx, client, workingDir)
	closeErr := client.Close()
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 15*time.Second)
	shutdownErr := jcode.ShutdownInstance(shutdownCtx, instance)
	cancelShutdown()
	return errors.Join(runErr, closeErr, shutdownErr)
}

func runTurn(lifecycleCtx context.Context, client *jcode.Client, workingDir string) error {
	session, err := client.CreateSession(lifecycleCtx, jcode.CreateSessionOptions{WorkingDir: workingDir})
	if err != nil {
		return err
	}
	turn, err := session.StartTurn(lifecycleCtx, "List the top-level files.", jcode.SendOptions{})
	if err != nil {
		return err
	}
	waitCtx, cancelWait := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelWait()
	if err := turn.Accepted(waitCtx); err != nil {
		return cancelAndWait(turn, err)
	}
	for {
		event, nextErr := turn.Next(waitCtx)
		if nextErr != nil {
			return cancelAndWait(turn, nextErr)
		}
		switch value := event.(type) {
		case *jcode.TextDelta:
			_, _ = io.WriteString(os.Stdout, value.Text)
		case *jcode.PermissionRequest:
			// Apply an explicit application policy before responding.
		case *jcode.TurnDone:
			fmt.Println()
			result, waitErr := turn.Wait(waitCtx)
			if waitErr != nil {
				return waitErr
			}
			return result.Err
		}
	}
}

func cancelAndWait(turn *jcode.Turn, interrupted error) error {
	cancelCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	cancelErr := turn.Cancel(cancelCtx)
	cancel()
	terminalCtx, cancelTerminal := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelTerminal()
	result, waitErr := turn.Wait(terminalCtx)
	return errors.Join(interrupted, cancelErr, waitErr, result.Err)
}
