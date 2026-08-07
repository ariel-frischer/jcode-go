package jcode

// Private instance startup and supervision.
//
// Launching is intentionally separate from the protocol client. The launcher
// owns only processes and SDK-created temporary homes; it never mutates the
// user's shared runtime.

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const instanceHomePrefix = "jcode-sdk-instance-"

// LaunchErrorCode identifies the phase which failed while starting an
// instance. Callers can use errors.As to inspect a LaunchError without parsing
// its human-readable message.
type LaunchErrorCode string

const (
	LaunchMissingBinary   LaunchErrorCode = "missing_binary"
	LaunchStartupFailed   LaunchErrorCode = "startup_failed"
	LaunchStartupTimeout  LaunchErrorCode = "startup_timeout"
	LaunchHandshakeFailed LaunchErrorCode = "handshake_failed"
	LaunchTransportFailed LaunchErrorCode = "transport_failed"
)

// LaunchError reports a failure in private-instance startup or connection.
type LaunchError struct {
	Code   LaunchErrorCode
	Binary string
	Stderr string
	Err    error
}

func (e *LaunchError) Error() string {
	message := string(e.Code)
	if e.Err != nil {
		message += ": " + e.Err.Error()
	}
	if e.Stderr != "" {
		message += ":\n" + e.Stderr
	}
	return message
}

func (e *LaunchError) Unwrap() error { return e.Err }

// LaunchOptions controls a private jcode instance.
type LaunchOptions struct {
	// JcodeHome is a persistent state directory. Empty creates a temporary
	// SDK-owned directory which is removed by Shutdown.
	JcodeHome  string
	WorkingDir string
	// InheritLogins is tri-state so its nil zero value preserves the SDK
	// behavior shared by the Rust and TypeScript SDKs: inherit by default.
	InheritLogins  *bool
	Binary         string
	Env            map[string]string
	StartupTimeout time.Duration
	CleanupTimeout time.Duration
	InheritStderr  bool
	// ClientOptions controls the protocol client created by Launch.
	ClientOptions Options
	// Observer receives redacted startup lifecycle metadata. If set, it is also
	// used by the protocol client created by Launch unless ClientOptions.Observer
	// is explicitly provided.
	Observer Observer
}

func (o LaunchOptions) withDefaults() LaunchOptions {
	if o.StartupTimeout <= 0 {
		o.StartupTimeout = 30 * time.Second
	}
	if o.CleanupTimeout <= 0 {
		o.CleanupTimeout = 30 * time.Second
	}
	return o
}

func (o LaunchOptions) inheritLogins() bool {
	return o.InheritLogins == nil || *o.InheritLogins
}

// LaunchInstance is the ownership handle for a private runtime.
type Instance interface {
	SocketPath() string
	JcodeHome() string
	Shutdown() error
	Close() error
}

type launchedInstance struct {
	socketPath     string
	jcodeHome      string
	runtimeDir     string
	ephemeral      bool
	cleanupTimeout time.Duration
	cmd            *exec.Cmd
	waitDone       <-chan error
	stopOnce       sync.Once
	stopErr        error
}

func (i *launchedInstance) SocketPath() string { return i.socketPath }
func (i *launchedInstance) JcodeHome() string  { return i.jcodeHome }
func (i *launchedInstance) Close() error       { return i.Shutdown() }

func (i *launchedInstance) Shutdown() error {
	i.stopOnce.Do(func() {
		// The bridge is not the daemon. Stop the daemon first while its
		// instance-scoped registry is still available.
		stopInstanceDaemon(i.jcodeHome, i.runtimeDir)
		if i.cmd != nil && i.cmd.Process != nil {
			terminateProcess(i.cmd, i.waitDone)
		}
		if i.ephemeral {
			removeOwnedInstanceHome(i.jcodeHome, i.cleanupTimeout)
		}
	})
	return i.stopErr
}

// Launch starts a private bridge and connects a Client to it. Any failure
// after the process starts performs best-effort teardown before returning.
func Launch(ctx context.Context, options LaunchOptions) (*Client, error) {
	observer := options.Observer
	if observer == nil {
		observer = options.ClientOptions.Observer
	}
	emitLaunchObservation(observer, Observation{Kind: "launch_start"})
	instance, err := launchInstance(options)
	if err != nil {
		emitLaunchObservation(observer, Observation{Kind: "launch_error", Error: string(launchErrorCode(err))})
		return nil, err
	}
	emitLaunchObservation(observer, Observation{Kind: "launch_ready"})
	tr, err := dialUnix(ctx, instance.SocketPath())
	if err != nil {
		_ = instance.Shutdown()
		emitLaunchObservation(observer, Observation{Kind: "launch_error", Error: string(LaunchTransportFailed)})
		return nil, &LaunchError{Code: LaunchTransportFailed, Err: err}
	}
	clientOptions := options.ClientOptions
	if clientOptions.Observer == nil {
		clientOptions.Observer = observer
	}
	client, err := NewClient(ctx, tr, clientOptions)
	if err != nil {
		_ = instance.Shutdown()
		emitLaunchObservation(observer, Observation{Kind: "launch_error", Error: string(LaunchHandshakeFailed)})
		return nil, &LaunchError{Code: LaunchHandshakeFailed, Err: err}
	}
	client.setInstance(instance)
	return client, nil
}

func launchErrorCode(err error) LaunchErrorCode {
	var launchErr *LaunchError
	if errors.As(err, &launchErr) {
		return launchErr.Code
	}
	return LaunchStartupFailed
}

func emitLaunchObservation(observer Observer, observation Observation) {
	if observer != nil {
		observer.Observe(observation)
	}
}

// LaunchInstance starts an isolated bridge without connecting the protocol
// client. This is useful when the caller needs to control connection setup.
func LaunchInstance(options LaunchOptions) (Instance, error) {
	return launchInstance(options)
}

func launchInstance(options LaunchOptions) (Instance, error) {
	o := options.withDefaults()
	ephemeral := o.JcodeHome == ""
	home := o.JcodeHome
	if ephemeral {
		var err error
		home, err = os.MkdirTemp("", instanceHomePrefix)
		if err != nil {
			return nil, &LaunchError{Code: LaunchStartupFailed, Err: err}
		}
	}
	cleanupOnError := func() {
		if ephemeral {
			removeOwnedInstanceHome(home, 0)
		}
	}
	if err := ensurePrivateDirectory(home); err != nil {
		cleanupOnError()
		return nil, &LaunchError{Code: LaunchStartupFailed, Err: err}
	}
	runtimeDir := filepath.Join(home, "run")
	if err := os.MkdirAll(runtimeDir, 0o700); err != nil {
		cleanupOnError()
		return nil, &LaunchError{Code: LaunchStartupFailed, Err: err}
	}
	_ = os.Chmod(runtimeDir, 0o700)
	for _, name := range []string{"jcode-api.sock", "jcode.sock", "jcode-debug.sock", "jcode.sock.hash"} {
		_ = os.Remove(filepath.Join(runtimeDir, name))
	}
	if o.inheritLogins() {
		if err := inheritCredentials(userJcodeHome(), home); err != nil {
			cleanupOnError()
			return nil, &LaunchError{Code: LaunchStartupFailed, Err: err}
		}
	}

	binary := selectBinary(o.Binary)
	socketPath := filepath.Join(runtimeDir, "jcode-api.sock")
	cmd := exec.Command(binary, "api-bridge", "--api-socket", socketPath)
	cmd.Dir = o.WorkingDir
	if cmd.Dir == "" {
		cmd.Dir = "."
	}
	cmd.Env = launchEnvironment(home, runtimeDir, socketPath, o.Env)
	cmd.Stdin = nil
	var stderr io.ReadCloser
	if o.InheritStderr {
		cmd.Stderr = os.Stderr
	} else {
		var err error
		stderr, err = cmd.StderrPipe()
		if err != nil {
			cleanupOnError()
			return nil, &LaunchError{Code: LaunchStartupFailed, Binary: binary, Err: err}
		}
	}
	if err := startProcess(cmd); err != nil {
		cleanupOnError()
		code := LaunchStartupFailed
		if errors.Is(err, os.ErrNotExist) {
			code = LaunchMissingBinary
		}
		return nil, &LaunchError{Code: code, Binary: binary, Err: err}
	}
	stderrDone := make(chan string, 1)
	if stderr != nil {
		go func() { stderrDone <- readTail(stderr, 4000) }()
	} else {
		stderrDone <- ""
	}
	waitDone := make(chan error, 1)
	go func() { waitDone <- cmd.Wait() }()
	deadline := time.Now().Add(o.StartupTimeout)
	for time.Now().Before(deadline) {
		if socketAccepts(socketPath) {
			return &launchedInstance{socketPath: socketPath, jcodeHome: home, runtimeDir: runtimeDir,
				ephemeral: ephemeral, cleanupTimeout: o.CleanupTimeout, cmd: cmd, waitDone: waitDone}, nil
		}
		select {
		case waitErr := <-waitDone:
			stderrText := <-stderrDone
			cleanupOnError()
			return nil, &LaunchError{Code: LaunchStartupFailed, Binary: binary, Stderr: redactSecrets(stderrText, o.Env), Err: waitErr}
		default:
		}
		time.Sleep(25 * time.Millisecond)
	}
	terminateProcess(cmd, waitDone)
	stderrText := <-stderrDone
	cleanupOnError()
	return nil, &LaunchError{Code: LaunchStartupTimeout, Binary: binary, Stderr: redactSecrets(stderrText, o.Env),
		Err: fmt.Errorf("no API socket at %s within %s", socketPath, o.StartupTimeout)}
}

// launchEnvironment applies explicit SDK credentials as replacements rather
// than appending duplicate keys. This matters for API-key-only providers:
// runtimes and platforms differ in whether the first or last duplicate wins.
func launchEnvironment(home, runtimeDir, socketPath string, overrides map[string]string) []string {
	values := make(map[string]string)
	for _, entry := range os.Environ() {
		key, value, ok := strings.Cut(entry, "=")
		if ok {
			values[key] = value
		}
	}
	values["JCODE_HOME"] = home
	values["JCODE_RUNTIME_DIR"] = runtimeDir
	values["JCODE_API_SOCKET"] = socketPath
	values["JCODE_SOCKET"] = filepath.Join(runtimeDir, "jcode.sock")
	for key, value := range overrides {
		values[key] = value
	}
	env := make([]string, 0, len(values))
	for key, value := range values {
		env = append(env, key+"="+value)
	}
	return env
}

// redactSecrets keeps child diagnostics actionable without allowing an API
// key supplied through LaunchOptions.Env to escape in a returned error.
func redactSecrets(text string, overrides map[string]string) string {
	for key, value := range overrides {
		if value != "" && (strings.Contains(strings.ToLower(key), "key") ||
			strings.Contains(strings.ToLower(key), "token") ||
			strings.Contains(strings.ToLower(key), "secret")) {
			text = strings.ReplaceAll(text, value, "[REDACTED]")
		}
	}
	return text
}

func selectBinary(explicit string) string {
	if explicit != "" {
		return explicit
	}
	if current, err := os.Executable(); err == nil {
		candidate := filepath.Join(filepath.Dir(current), "jcode")
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			return candidate
		}
	}
	return "jcode"
}

func socketAccepts(path string) bool {
	conn, err := net.DialTimeout("unix", path, 100*time.Millisecond)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

func dialUnix(ctx context.Context, path string) (interfaceTransport, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var d net.Dialer
	conn, err := d.DialContext(ctx, "unix", path)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// interfaceTransport keeps launch.go independent of the concrete transport
// package while retaining the same io.ReadWriteCloser contract.
type interfaceTransport interface{ io.ReadWriteCloser }

func readTail(reader io.Reader, limit int) string {
	data, _ := io.ReadAll(bufio.NewReader(reader))
	if len(data) > limit {
		data = data[len(data)-limit:]
	}
	return strings.TrimSpace(string(data))
}

func ensurePrivateDirectory(path string) error {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("instance home must be a real directory: %s", path)
	}
	return os.Chmod(path, 0o700)
}

var credentialFiles = map[string]bool{
	"auth.json": true, "openai-auth.json": true, "antigravity_oauth.json": true,
	"gemini_oauth.json": true, "google_oauth.json": true, "google_credentials.json": true,
	"config.toml": true,
}

// InheritCredentials shares rotating auth files and copies mutable config.
func InheritCredentials(fromHome, toHome string) ([]string, error) {
	if filepath.Clean(fromHome) == filepath.Clean(toHome) {
		return nil, fmt.Errorf("instance home must differ from user's jcode home")
	}
	if err := ensurePrivateDirectory(toHome); err != nil {
		return nil, err
	}
	var inherited []string
	for name := range credentialFiles {
		source := filepath.Join(fromHome, name)
		info, err := os.Stat(source)
		if err != nil || !info.Mode().IsRegular() {
			continue
		}
		destination := filepath.Join(toHome, name)
		_ = os.Remove(destination)
		if name == "config.toml" {
			if err := copyOwnerOnly(source, destination); err != nil {
				return nil, err
			}
		} else {
			if err := os.Symlink(source, destination); err != nil {
				return nil, err
			}
		}
		inherited = append(inherited, name)
	}
	return inherited, nil
}

func inheritCredentials(fromHome, toHome string) error {
	_, err := InheritCredentials(fromHome, toHome)
	return err
}

func copyOwnerOnly(from, to string) error {
	in, err := os.Open(from)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(to, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(out, in)
	closeErr := out.Close()
	if copyErr != nil {
		return copyErr
	}
	return closeErr
}

func userJcodeHome() string {
	if value := os.Getenv("JCODE_HOME"); value != "" {
		return value
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ".jcode"
	}
	return filepath.Join(home, ".jcode")
}

func readDaemonPID(home, runtimeDir string) int {
	data, err := os.ReadFile(filepath.Join(home, "servers.json"))
	if err != nil {
		return 0
	}
	var entries map[string]struct {
		Socket string `json:"socket"`
		PID    int    `json:"pid"`
	}
	if json.Unmarshal(data, &entries) != nil {
		return 0
	}
	wanted := filepath.Join(runtimeDir, "jcode.sock")
	for _, entry := range entries {
		if entry.Socket == wanted && entry.PID > 1 {
			return entry.PID
		}
	}
	return 0
}

func stopInstanceDaemon(home, runtimeDir string) {
	if pid := readDaemonPID(home, runtimeDir); pid > 1 {
		stopProcess(pid)
	}
}

func removeOwnedInstanceHome(home string, timeout time.Duration) {
	clean := filepath.Clean(home)
	parent := filepath.Clean(filepath.Dir(clean))
	if parent != filepath.Clean(os.TempDir()) || !strings.HasPrefix(filepath.Base(clean), instanceHomePrefix) {
		return
	}
	if info, err := os.Lstat(clean); err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return
	}
	deadline := time.Now().Add(timeout)
	for {
		_ = os.RemoveAll(clean)
		if _, err := os.Lstat(clean); os.IsNotExist(err) {
			return
		}
		if timeout <= 0 || time.Now().After(deadline) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}
