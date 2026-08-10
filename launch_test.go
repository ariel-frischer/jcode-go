package jcode

import (
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestLaunchArgs(t *testing.T) {
	tests := map[string]struct {
		options LaunchOptions
		want    []string
	}{
		"defaults": {
			want: []string{"api-bridge", "--api-socket", "/private/api.sock"},
		},
		"provider and model": {
			options: LaunchOptions{Provider: "openrouter", Model: "openai/gpt-5.6-luna"},
			want:    []string{"--provider", "openrouter", "--model", "openai/gpt-5.6-luna", "api-bridge", "--api-socket", "/private/api.sock"},
		},
		"whitespace is normalized": {
			options: LaunchOptions{Provider: " openai ", Model: " gpt-5.6-luna "},
			want:    []string{"--provider", "openai", "--model", "gpt-5.6-luna", "api-bridge", "--api-socket", "/private/api.sock"},
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			if got := launchArgs(test.options, "/private/api.sock"); !reflect.DeepEqual(got, test.want) {
				t.Fatalf("launchArgs() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestWaitForCommandExitFansOutWithoutConsumingShutdownResult(t *testing.T) {
	cmd := exec.Command("sh", "-c", "exit 0")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	waitDone, bridgeDone := waitForCommandExit(cmd)
	select {
	case <-bridgeDone:
	case <-time.After(time.Second):
		t.Fatal("bridge exit signal was not closed")
	}
	select {
	case err := <-waitDone:
		if err != nil {
			t.Fatalf("process wait error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("bridge signal consumed the shutdown wait result")
	}
}

func TestWaitForCommandExitPreservesNonzeroExitError(t *testing.T) {
	cmd := exec.Command("sh", "-c", "exit 1")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	waitDone, bridgeDone := waitForCommandExit(cmd)
	select {
	case <-bridgeDone:
	case <-time.After(time.Second):
		t.Fatal("bridge exit signal was not closed for nonzero exit")
	}
	select {
	case err := <-waitDone:
		if err == nil {
			t.Fatal("process wait error = nil, want nonzero exit error")
		}
	case <-time.After(time.Second):
		t.Fatal("nonzero shutdown wait result was not delivered")
	}
}

func TestLaunchEnvironmentExplicitAPIKeyReplacesAmbientValue(t *testing.T) {
	t.Setenv("OPENROUTER_API_KEY", "ambient-key")
	env := launchEnvironment("/private/home", "/private/run", "/private/run/api.sock", map[string]string{
		"OPENROUTER_API_KEY": "explicit-key",
	})
	values := make(map[string]string)
	for _, entry := range env {
		key, value, ok := strings.Cut(entry, "=")
		if !ok {
			t.Fatalf("malformed environment entry %q", entry)
		}
		if _, exists := values[key]; exists {
			t.Fatalf("duplicate environment key %q", key)
		}
		values[key] = value
	}
	if got := values["OPENROUTER_API_KEY"]; got != "explicit-key" {
		t.Fatalf("OPENROUTER_API_KEY = %q, want explicit value", got)
	}
	if got := values["JCODE_HOME"]; got != "/private/home" {
		t.Fatalf("JCODE_HOME = %q, want private home", got)
	}
}

func TestInheritLaunchCredentialsCopiesLegacyCodexOAuth(t *testing.T) {
	root := t.TempDir()
	userJcodeHome := filepath.Join(root, ".jcode")
	privateHome := filepath.Join(root, "private")
	legacyAuth := filepath.Join(root, ".codex", "auth.json")
	if err := os.MkdirAll(filepath.Dir(legacyAuth), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(userJcodeHome, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(legacyAuth, []byte(`{"tokens":{"access_token":"test"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("JCODE_HOME", userJcodeHome)

	got, err := inheritLaunchCredentials(LaunchOptions{}, privateHome)
	if err != nil {
		t.Fatal(err)
	}
	if got.Env[allowLegacyCodexAuthEnv] != "1" {
		t.Fatalf("%s = %q, want 1", allowLegacyCodexAuthEnv, got.Env[allowLegacyCodexAuthEnv])
	}
	destination := filepath.Join(privateHome, legacyCodexAuthDestination)
	info, err := os.Lstat(destination)
	if err != nil {
		t.Fatal(err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		t.Fatalf("legacy auth mode = %v, want regular 0600", info.Mode())
	}
	data, err := os.ReadFile(destination)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != `{"tokens":{"access_token":"test"}}` {
		t.Fatalf("legacy auth content = %q", data)
	}
}

func TestInheritCredentialsReportsLegacyCodexOAuth(t *testing.T) {
	root := t.TempDir()
	fromHome := filepath.Join(root, ".jcode")
	toHome := filepath.Join(root, "private")
	legacyAuth := filepath.Join(root, ".codex", "auth.json")
	if err := os.MkdirAll(filepath.Dir(legacyAuth), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(fromHome, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(legacyAuth, []byte("oauth"), 0o600); err != nil {
		t.Fatal(err)
	}

	inherited, err := InheritCredentials(fromHome, toHome)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(inherited, legacyCodexAuthDestination) {
		t.Fatalf("inherited = %q, want %q", inherited, legacyCodexAuthDestination)
	}
}

func TestRedactSecretsOnlyRedactsExplicitKeyValues(t *testing.T) {
	const secret = "explicit-key-value"
	message := "provider failed with key=" + secret
	redacted := redactSecrets(message, map[string]string{"OPENROUTER_API_KEY": secret})
	if strings.Contains(redacted, secret) || !strings.Contains(redacted, "[REDACTED]") {
		t.Fatalf("secret was not redacted: %q", redacted)
	}
	if got := redactSecrets("ordinary diagnostic", nil); got != "ordinary diagnostic" {
		t.Fatalf("unexpected change to ordinary diagnostic: %q", got)
	}
}
