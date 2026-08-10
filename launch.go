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
	"slices"
	"strings"
	"sync"
	"time"
)

const instanceHomePrefix = "jcode-sdk-instance-"

const (
	defaultShutdownGracePeriod = 5 * time.Second
	defaultShutdownReapTimeout = 5 * time.Second
	defaultCleanupTimeout      = 30 * time.Second
)

const (
	allowLegacyCodexAuthEnv    = "JCODE_ALLOW_CODEX_LEGACY_AUTH"
	legacyCodexAuthSource      = ".codex/auth.json"
	legacyCodexAuthDestination = "external/.codex/auth.json"
)

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
	InheritLogins *bool
	Binary        string
	// Provider and Model are passed as global jcode CLI selections when starting
	// the private API bridge. Empty values preserve jcode's normal auto/config
	// resolution.
	Provider       string
	Model          string
	Env            map[string]string
	StartupTimeout time.Duration
	CleanupTimeout time.Duration
	// ShutdownGracePeriod bounds cooperative SIGTERM shutdown before Linux
	// private instances escalate to SIGKILL. Non-positive values use 5 seconds.
	ShutdownGracePeriod time.Duration
	// ShutdownReapTimeout bounds waiting for the single bridge Wait result after
	// termination attempts. Non-positive values use 5 seconds.
	ShutdownReapTimeout time.Duration
	InheritStderr       bool
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
		o.CleanupTimeout = defaultCleanupTimeout
	}
	if o.ShutdownGracePeriod <= 0 {
		o.ShutdownGracePeriod = defaultShutdownGracePeriod
	}
	if o.ShutdownReapTimeout <= 0 {
		o.ShutdownReapTimeout = defaultShutdownReapTimeout
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
	socketPath            string
	jcodeHome             string
	runtimeDir            string
	ownedPaths            []string
	ephemeral             bool
	cleanupTimeout        time.Duration
	shutdownGracePeriod   time.Duration
	shutdownReapTimeout   time.Duration
	cmd                   *exec.Cmd
	processGroupID        int
	daemonPID             int
	waitDone              <-chan error
	shutdownMu            sync.Mutex
	shutdownStarted       bool
	shutdownFinished      bool
	shutdownDone          chan struct{}
	shutdownForceKill     chan struct{}
	shutdownForceKillOnce sync.Once
	shutdownContextErrs   []error
	shutdownErr           error
}

func (i *launchedInstance) SocketPath() string { return i.socketPath }
func (i *launchedInstance) JcodeHome() string  { return i.jcodeHome }
func (i *launchedInstance) Close() error       { return i.Shutdown() }

func (i *launchedInstance) Shutdown() error {
	return i.shutdown(context.Background())
}

// ShutdownInstance shuts down an Instance while allowing a caller context to
// shorten the cooperative phase of SDK-owned Linux private instances. The
// Instance interface remains unchanged so external implementations stay source
// compatible. Cancellation never skips best-effort termination and cleanup.
func ShutdownInstance(ctx context.Context, instance Instance) error {
	if instance == nil {
		return errors.New("nil instance")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if owned, ok := instance.(interface{ shutdown(context.Context) error }); ok {
		return owned.shutdown(ctx)
	}
	err := instance.Shutdown()
	if err == nil {
		return nil
	}
	return errors.Join(context.Cause(ctx), err)
}

func (i *launchedInstance) shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	owner, done := i.beginShutdown(ctx)
	if owner {
		i.finishShutdown(i.runShutdown())
	}
	<-done
	i.shutdownMu.Lock()
	err := i.shutdownErr
	i.shutdownMu.Unlock()
	return err
}

func (i *launchedInstance) beginShutdown(ctx context.Context) (bool, <-chan struct{}) {
	i.shutdownMu.Lock()
	if i.shutdownDone == nil {
		i.shutdownDone = make(chan struct{})
		i.shutdownForceKill = make(chan struct{})
	}
	done := i.shutdownDone
	if i.shutdownFinished {
		i.shutdownMu.Unlock()
		return false, done
	}
	owner := !i.shutdownStarted
	i.shutdownStarted = true
	i.shutdownMu.Unlock()

	if cause := context.Cause(ctx); cause != nil {
		i.requestShutdownEscalation(cause)
	} else if ctx.Done() != nil {
		go func() {
			select {
			case <-ctx.Done():
				i.requestShutdownEscalation(context.Cause(ctx))
			case <-done:
			}
		}()
	}
	return owner, done
}

func (i *launchedInstance) requestShutdownEscalation(cause error) {
	if cause == nil {
		return
	}
	i.shutdownMu.Lock()
	if i.shutdownFinished {
		i.shutdownMu.Unlock()
		return
	}
	duplicate := false
	for _, existing := range i.shutdownContextErrs {
		if errors.Is(existing, cause) && errors.Is(cause, existing) {
			duplicate = true
			break
		}
	}
	if !duplicate {
		i.shutdownContextErrs = append(i.shutdownContextErrs, cause)
	}
	i.shutdownMu.Unlock()
	i.shutdownForceKillOnce.Do(func() { close(i.shutdownForceKill) })
}

func (i *launchedInstance) runShutdown() error {
	var errs []error
	// The bridge is not necessarily the daemon. Resolve only the matching
	// instance registry entry while it is still readable, then stop that PID.
	if i.daemonPID <= 1 && i.jcodeHome != "" && i.runtimeDir != "" {
		i.daemonPID = readDaemonPID(i.jcodeHome, i.runtimeDir)
	}
	if i.daemonPID > 1 {
		if err := stopProcess(i.daemonPID); err != nil {
			errs = append(errs, fmt.Errorf("stop private daemon: %w", err))
		}
	}

	if i.cmd != nil || i.waitDone != nil || i.processGroupID != 0 {
		_, err := terminateProcess(i.cmd, i.processGroupID, i.waitDone,
			finiteDuration(i.shutdownGracePeriod, defaultShutdownGracePeriod),
			finiteDuration(i.shutdownReapTimeout, defaultShutdownReapTimeout),
			i.shutdownForceKill)
		if err != nil {
			errs = append(errs, err)
		}
	}
	if err := cleanupOwnedInstancePaths(i.jcodeHome, i.runtimeDir, i.ownedPaths,
		i.ephemeral, i.cleanupTimeout); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func (i *launchedInstance) finishShutdown(phaseErr error) {
	i.shutdownMu.Lock()
	errs := make([]error, 0, len(i.shutdownContextErrs)+1)
	if phaseErr != nil {
		errs = append(errs, phaseErr)
		errs = append(errs, i.shutdownContextErrs...)
	}
	i.shutdownErr = errors.Join(errs...)
	i.shutdownFinished = true
	close(i.shutdownDone)
	i.shutdownMu.Unlock()
}

func finiteDuration(value, fallback time.Duration) time.Duration {
	if value <= 0 {
		return fallback
	}
	return value
}

// Launch starts a private bridge and connects a Client to it. Any failure
// after the process starts performs best-effort teardown before returning.
func Launch(ctx context.Context, options LaunchOptions) (*Client, error) {
	observer := options.Observer
	if observer == nil {
		observer = options.ClientOptions.Observer
	}
	emitLaunchObservation(observer, Observation{Kind: "launch_start"})
	instance, err := launchInstanceWithObserver(options, observer)
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
	return launchInstanceWithObserver(options, nil)
}

func launchInstanceWithObserver(options LaunchOptions, observer Observer) (Instance, error) {
	o := options.withDefaults()
	emitLaunchObservation(observer, Observation{Kind: "launch_prepare"})
	ephemeral := o.JcodeHome == ""
	home := o.JcodeHome
	if ephemeral {
		var err error
		home, err = os.MkdirTemp("", instanceHomePrefix)
		if err != nil {
			return nil, &LaunchError{Code: LaunchStartupFailed, Err: err}
		}
	}
	absoluteHome, err := filepath.Abs(home)
	if err != nil {
		if ephemeral {
			_ = removeOwnedInstanceHome(home, defaultCleanupTimeout)
		}
		return nil, &LaunchError{Code: LaunchStartupFailed, Err: fmt.Errorf("resolve instance home: %w", err)}
	}
	home = absoluteHome
	cleanupOnError := func() {
		if ephemeral {
			_ = removeOwnedInstanceHome(home, defaultCleanupTimeout)
		}
	}
	if err := ensurePrivateDirectory(home); err != nil {
		cleanupOnError()
		return nil, &LaunchError{Code: LaunchStartupFailed, Err: err}
	}
	runtimeDir := filepath.Join(home, "run")
	if err := ensurePrivateDirectory(runtimeDir); err != nil {
		cleanupOnError()
		return nil, &LaunchError{Code: LaunchStartupFailed, Err: fmt.Errorf("prepare instance runtime directory: %w", err)}
	}
	ownedPaths := instanceOwnedRuntimePaths(runtimeDir)
	cleanupOnError = func() {
		_ = cleanupOwnedInstancePaths(home, runtimeDir, ownedPaths, ephemeral, o.CleanupTimeout)
	}
	for _, path := range ownedPaths {
		_ = os.Remove(path)
	}
	if o.inheritLogins() {
		var err error
		o, err = inheritLaunchCredentials(o, home)
		if err != nil {
			cleanupOnError()
			return nil, &LaunchError{Code: LaunchStartupFailed, Err: err}
		}
	}

	binary := selectBinary(o.Binary)
	socketPath := filepath.Join(runtimeDir, "jcode-api.sock")
	cmd := exec.Command(binary, launchArgs(o, socketPath)...)
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
	processGroupID, err := startProcess(cmd)
	if err != nil {
		cleanupOnError()
		code := LaunchStartupFailed
		if errors.Is(err, os.ErrNotExist) {
			code = LaunchMissingBinary
		}
		return nil, &LaunchError{Code: code, Binary: binary, Err: err}
	}
	emitLaunchObservation(observer, Observation{Kind: "launch_process_started"})
	stderrDone := make(chan string, 1)
	if stderr != nil {
		go func() { stderrDone <- readTail(stderr, 4000) }()
	} else {
		stderrDone <- ""
	}
	waitDone := make(chan error, 1)
	go func() { waitDone <- cmd.Wait() }()
	emitLaunchObservation(observer, Observation{Kind: "launch_wait_socket"})
	deadline := time.Now().Add(o.StartupTimeout)
	for time.Now().Before(deadline) {
		if socketAccepts(socketPath) {
			emitLaunchObservation(observer, Observation{Kind: "launch_socket_ready"})
			return &launchedInstance{socketPath: socketPath, jcodeHome: home, runtimeDir: runtimeDir,
				ownedPaths: ownedPaths, ephemeral: ephemeral, cleanupTimeout: o.CleanupTimeout,
				shutdownGracePeriod: o.ShutdownGracePeriod, shutdownReapTimeout: o.ShutdownReapTimeout,
				cmd: cmd, processGroupID: processGroupID, daemonPID: readDaemonPID(home, runtimeDir),
				waitDone: waitDone}, nil
		}
		select {
		case waitErr := <-waitDone:
			stderrText := collectStderr(stderr, stderrDone, o.ShutdownReapTimeout)
			cleanupOnError()
			emitLaunchObservation(observer, Observation{Kind: "launch_process_error", Error: string(LaunchStartupFailed)})
			return nil, &LaunchError{Code: LaunchStartupFailed, Binary: binary, Stderr: redactSecrets(stderrText, o.Env), Err: waitErr}
		default:
		}
		time.Sleep(25 * time.Millisecond)
	}
	_, terminationErr := terminateProcess(cmd, processGroupID, waitDone,
		o.ShutdownGracePeriod, o.ShutdownReapTimeout, nil)
	stderrText := collectStderr(stderr, stderrDone, o.ShutdownReapTimeout)
	cleanupOnError()
	emitLaunchObservation(observer, Observation{Kind: "launch_socket_timeout", Error: string(LaunchStartupTimeout)})
	return nil, &LaunchError{Code: LaunchStartupTimeout, Binary: binary, Stderr: redactSecrets(stderrText, o.Env),
		Err: errors.Join(fmt.Errorf("no API socket at %s within %s", socketPath, o.StartupTimeout), terminationErr)}
}

func launchArgs(options LaunchOptions, socketPath string) []string {
	args := make([]string, 0, 7)
	if provider := strings.TrimSpace(options.Provider); provider != "" {
		args = append(args, "--provider", provider)
	}
	if model := strings.TrimSpace(options.Model); model != "" {
		args = append(args, "--model", model)
	}
	return append(args, "api-bridge", "--api-socket", socketPath)
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

func collectStderr(stderr io.Closer, stderrDone <-chan string, timeout time.Duration) string {
	timer := time.NewTimer(finiteDuration(timeout, defaultShutdownReapTimeout))
	defer timer.Stop()
	select {
	case text := <-stderrDone:
		return text
	case <-timer.C:
		if stderr != nil {
			_ = stderr.Close()
		}
		select {
		case text := <-stderrDone:
			return text
		default:
			return ""
		}
	}
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

func inheritLaunchCredentials(options LaunchOptions, privateHome string) (LaunchOptions, error) {
	inherited, err := InheritCredentials(userJcodeHome(), privateHome)
	if err != nil {
		return options, err
	}
	if !slices.Contains(inherited, legacyCodexAuthDestination) {
		return options, nil
	}
	options.Env = cloneEnvironment(options.Env)
	if _, explicit := options.Env[allowLegacyCodexAuthEnv]; !explicit {
		options.Env[allowLegacyCodexAuthEnv] = "1"
	}
	return options, nil
}

func cloneEnvironment(source map[string]string) map[string]string {
	cloned := make(map[string]string, len(source)+1)
	for key, value := range source {
		cloned[key] = value
	}
	return cloned
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
	legacyInherited, err := inheritLegacyCodexAuth(fromHome, toHome)
	if err != nil {
		return nil, err
	}
	if legacyInherited {
		inherited = append(inherited, legacyCodexAuthDestination)
	}
	return inherited, nil
}

func inheritLegacyCodexAuth(fromHome, toHome string) (bool, error) {
	source := filepath.Join(filepath.Dir(fromHome), legacyCodexAuthSource)
	info, err := os.Lstat(source)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("inspecting legacy Codex auth: %w", err)
	}
	if !info.Mode().IsRegular() {
		return false, fmt.Errorf("legacy Codex auth must be a regular file: %s", source)
	}
	destination := filepath.Join(toHome, legacyCodexAuthDestination)
	if err := ensurePrivateDirectory(filepath.Dir(destination)); err != nil {
		return false, fmt.Errorf("creating private legacy Codex auth directory: %w", err)
	}
	if err := copyOwnerOnly(source, destination); err != nil {
		return false, fmt.Errorf("copying legacy Codex auth: %w", err)
	}
	return true, nil
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
	if home == "" || runtimeDir == "" {
		return 0
	}
	home = filepath.Clean(home)
	runtimeDir = filepath.Clean(runtimeDir)
	if runtimeDir != filepath.Join(home, "run") {
		return 0
	}
	for _, directory := range []string{home, runtimeDir} {
		info, err := os.Lstat(directory)
		if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return 0
		}
	}
	registryPath := filepath.Join(home, "servers.json")
	info, err := os.Lstat(registryPath)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return 0
	}
	data, err := os.ReadFile(registryPath)
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
	wanted := filepath.Clean(filepath.Join(runtimeDir, "jcode.sock"))
	for _, entry := range entries {
		if filepath.Clean(entry.Socket) == wanted && entry.PID > 1 {
			return entry.PID
		}
	}
	return 0
}

func instanceOwnedRuntimePaths(runtimeDir string) []string {
	return []string{
		filepath.Join(runtimeDir, "jcode-api.sock"),
		filepath.Join(runtimeDir, "jcode.sock"),
		filepath.Join(runtimeDir, "jcode-debug.sock"),
		filepath.Join(runtimeDir, "jcode.sock.hash"),
	}
}

func cleanupOwnedInstancePaths(home, runtimeDir string, ownedPaths []string, ephemeral bool, timeout time.Duration) error {
	timeout = finiteDuration(timeout, defaultCleanupTimeout)
	deadline := time.Now().Add(timeout)
	var errs []error
	if err := removeOwnedRuntimePaths(home, runtimeDir, ownedPaths, remainingCleanupDuration(deadline)); err != nil {
		errs = append(errs, err)
	}
	if ephemeral {
		if err := removeOwnedInstanceHome(home, remainingCleanupDuration(deadline)); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func remainingCleanupDuration(deadline time.Time) time.Duration {
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return time.Nanosecond
	}
	return remaining
}

func removeOwnedRuntimePaths(home, runtimeDir string, ownedPaths []string, timeout time.Duration) error {
	if home == "" && runtimeDir == "" && len(ownedPaths) == 0 {
		return nil
	}
	home = filepath.Clean(home)
	runtimeDir = filepath.Clean(runtimeDir)
	if runtimeDir != filepath.Join(home, "run") {
		return fmt.Errorf("owned runtime directory is outside the recorded home")
	}
	allowed := make(map[string]struct{}, 4)
	for _, path := range instanceOwnedRuntimePaths(runtimeDir) {
		allowed[filepath.Clean(path)] = struct{}{}
	}
	for _, path := range ownedPaths {
		clean := filepath.Clean(path)
		if filepath.Dir(clean) != runtimeDir {
			return fmt.Errorf("owned runtime path is outside the recorded runtime directory")
		}
		if _, ok := allowed[clean]; !ok {
			return fmt.Errorf("owned runtime path is not an expected private runtime file")
		}
	}
	if info, err := os.Lstat(home); os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("inspect owned home: %w", err)
	} else if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("owned home is not a real directory")
	}
	if info, err := os.Lstat(runtimeDir); os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("inspect owned runtime directory: %w", err)
	} else if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("owned runtime directory is not a real directory")
	}

	timeout = finiteDuration(timeout, defaultCleanupTimeout)
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		var permanent bool
		lastErr, permanent = removeOwnedRuntimePathsOnce(runtimeDir, ownedPaths)
		if lastErr == nil {
			return nil
		}
		if permanent {
			return fmt.Errorf("clean owned runtime paths: %w", lastErr)
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("clean owned runtime paths: %w", lastErr)
		}
		delay := min(50*time.Millisecond, time.Until(deadline))
		if delay > 0 {
			time.Sleep(delay)
		}
	}
}

func removeOwnedRuntimePathsOnce(runtimeDir string, ownedPaths []string) (error, bool) {
	var errs []error
	permanent := false
	for _, path := range ownedPaths {
		info, err := os.Lstat(path)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("inspect owned runtime file: %w", err))
			permanent = true
			continue
		}
		if info.Mode()&os.ModeSymlink != 0 || info.IsDir() ||
			(!info.Mode().IsRegular() && info.Mode()&os.ModeSocket == 0) {
			errs = append(errs, errors.New("owned runtime file has an unsafe type"))
			permanent = true
			continue
		}
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			errs = append(errs, fmt.Errorf("remove owned runtime file: %w", err))
		}
	}
	if len(errs) != 0 {
		return errors.Join(errs...), permanent
	}
	entries, err := os.ReadDir(runtimeDir)
	if os.IsNotExist(err) {
		return nil, false
	}
	if err != nil {
		return fmt.Errorf("inspect owned runtime directory contents: %w", err), false
	}
	if len(entries) != 0 {
		return nil, false
	}
	if err := os.Remove(runtimeDir); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove empty owned runtime directory: %w", err), false
	}
	return nil, false
}

func removeOwnedInstanceHome(home string, timeout time.Duration) error {
	clean := filepath.Clean(home)
	parent := filepath.Clean(filepath.Dir(clean))
	if parent != filepath.Clean(os.TempDir()) || !strings.HasPrefix(filepath.Base(clean), instanceHomePrefix) {
		return errors.New("ephemeral instance home is outside the SDK ownership boundary")
	}
	if info, err := os.Lstat(clean); os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("inspect ephemeral instance home: %w", err)
	} else if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return errors.New("ephemeral instance home is not a real directory")
	}
	timeout = finiteDuration(timeout, defaultCleanupTimeout)
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		lastErr = os.RemoveAll(clean)
		_, statErr := os.Lstat(clean)
		if os.IsNotExist(statErr) {
			return nil
		}
		if statErr != nil {
			return errors.Join(lastErr, fmt.Errorf("inspect ephemeral instance home after removal: %w", statErr))
		}
		if !time.Now().Before(deadline) {
			if lastErr == nil {
				lastErr = errors.New("ephemeral instance home still exists")
			}
			return fmt.Errorf("remove ephemeral instance home: %w", lastErr)
		}
		delay := min(50*time.Millisecond, time.Until(deadline))
		if delay > 0 {
			time.Sleep(delay)
		}
	}
}
