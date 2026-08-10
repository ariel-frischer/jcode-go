package jcode

import (
	"reflect"
	"strings"
	"testing"
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
