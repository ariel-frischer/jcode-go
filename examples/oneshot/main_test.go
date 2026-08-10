package main

import (
	"context"
	"path/filepath"
	"testing"
)

func TestRunRedactsSocketPathOnDialFailure(t *testing.T) {
	privateSocketPath := filepath.Join(t.TempDir(), "private-api-socket-secret.sock")
	t.Setenv("JCODE_API_SOCKET", privateSocketPath)

	err := run(context.Background(), nil)
	if err == nil {
		t.Fatal("run() error = nil, want connection failure")
	}
	if err.Error() != "connect to API socket" {
		t.Fatal("run() returned a non-redacted connection error")
	}
}
