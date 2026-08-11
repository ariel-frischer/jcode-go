# Go SDK

The Go SDK is the Go client for the jcode harness API. It speaks protocol v1 over an `io.ReadWriteCloser`, completes the `hello` handshake, correlates request replies, and delivers asynchronous events through bounded subscriptions.

> **Unofficial community fork:** This is an independent, vibe-coded fork maintained by Ariel Frischer. It is not an official Jcode release, is not endorsed or supported by the upstream Jcode maintainers, and may be incomplete or incompatible with future Jcode versions. Use it experimentally and review the code before relying on it.

> **API status:** The Go package provides a transport-level client, typed session helpers (`CreateSession`, `AttachSession`, `Session.Send`, and `Session.StartTurn`), typed event streams, and private-instance helpers (`Launch`, `LaunchInstance`, and `LaunchOptions`). Raw `Request` and `Subscribe` remain available for forward-compatible protocol additions.

## Install

This fork is published as a Go module. Install the tagged release with:

```bash
go get github.com/ariel-frischer/jcode-go@v0.1.0
```

For a source checkout:

```bash
git clone https://github.com/ariel-frischer/jcode-go.git
cd jcode-go
go test ./...
go vet ./...
```

The module requires Go 1.23 or newer. The SDK has no third-party dependencies.

## Choose a connection mode

There are two deployment patterns:

1. **Connect to a shared instance.** Start `jcode api-bridge` once, then dial its owner-only Unix socket and pass the connection to `NewClient`. This is suitable for editor plugins, dashboards, and tools that intentionally operate on the user's live sessions.
2. **Own a private instance.** `jcode.Launch` starts a separately configured bridge/daemon, gives it a separate home and socket, and returns a client that owns shutdown. `LaunchInstance` is available when the caller needs to control dialing. Typed session helpers and raw protocol requests are both available.

### Safe-run ownership

`Connect` is always non-owning. It attaches to an existing runtime, and
`Client.Close` closes only that client's transport. It never stops the shared
daemon. `Launch` is different: the returned client owns the private process,
daemon, instance home, and cleanup through its internal `Instance` handle.

When a worker connection must be allowed to close while the private session
continues, transfer the private ownership explicitly:

```go
client, err := jcode.Launch(ctx, jcode.LaunchOptions{
    JcodeHome:  home,
    WorkingDir: workingDir,
})
if err != nil { return err }
owner, ok := client.DetachInstance()
if !ok { return errors.New("launch did not return an owned instance") }
_ = client.Close() // transport only; the safe-run owner remains alive
defer func() {
    shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 15*time.Second)
    defer cancelShutdown()
    _ = jcode.ShutdownInstance(shutdownCtx, owner) // explicit owner performs bounded cleanup
}()
```

This is intentionally opt-in. It does not make arbitrary clients immortal or
change shared-runtime shutdown behavior. The owner is responsible for keeping
the `Instance` handle alive and calling `Shutdown`; shutdown is idempotent.
The SDK does not run an implicit infinite supervisor. Reconnect remains
explicit and bounded by `ReconnectPolicy.MaxAttempts`, with
`reconnect_failed`, `resume_failed`, and `reconnected` observations exposing
attempts and outcomes. In-flight requests are never replayed.

A shared connection sees the user's sessions and actions are visible in their terminal. A private connection must use a distinct state directory and socket. Never point a private process at the user's live jcode home.

## Shared connect: one-shot CLI-like flow

Start the bridge first:

```bash
jcode api-bridge
```

Then run [`examples/oneshot`](examples/oneshot):

```bash
go run ./examples/oneshot "List the top-level files"
```

The example dials `$JCODE_API_SOCKET` when set, otherwise `$XDG_RUNTIME_DIR/jcode-api.sock`, creates a session, sends one message, prints text deltas, and exits at `turn_done`.

The important shared-connection lifecycle is still explicitly closed, while the
turn owns acceptance, ordered events, server cancellation, and terminal outcome:

```go
ctx := context.Background()
conn, err := net.Dial("unix", socketPath)
if err != nil { return err }
client, err := jcode.NewClient(ctx, conn, jcode.Options{ClientName: "my-tool/1.0"})
if err != nil { return err } // NewClient closes conn after handshake failure
defer client.Close()
session, err := client.CreateSession(ctx, jcode.CreateSessionOptions{WorkingDir: workingDir})
if err != nil { return err }
turn, err := session.StartTurn(lifecycleCtx, prompt, jcode.SendOptions{})
if err != nil { return err }
```

`NewClient` starts its reader goroutine and performs a protocol v1 handshake. `Client.Close` is idempotent and wakes pending requests and subscriptions.

## Typed session lifecycle

The existing `Session.Send` and `Session.Events` APIs remain compatible for
callers that separately manage acceptance and event consumption:

```go
session, err := client.CreateSession(ctx, jcode.CreateSessionOptions{WorkingDir: workingDir})
if err != nil { return err }
stream := session.Events(ctx)
defer stream.Close()
if err := session.Send(ctx, prompt, jcode.SendOptions{}); err != nil { return err }
for {
    event, err := stream.Next(ctx)
    if err != nil { return err }
    switch value := event.(type) {
    case *jcode.TextDelta:
        io.WriteString(out, value.Text)
    case *jcode.PermissionRequest:
        // Apply an explicit application policy before responding.
    case *jcode.TurnDone:
        return nil
    }
}
```

`AttachSession` creates the same lightweight typed view for an existing ID. `Session.Send` subscribes before notifying the server and waits for the asynchronous `message_accepted` event. `SendOptions.NoReply` selects fire-and-forget notification semantics. `Session.Send` does not retry because a timeout can leave a mutation with an unknown server-side outcome.

Use `StartTurn` when the caller must own acceptance, ordered events,
cancellation, and completion as one lifecycle:

```go
turn, err := session.StartTurn(lifecycleCtx, prompt, jcode.SendOptions{})
if err != nil { return err }
if err := turn.Accepted(waitCtx); err != nil { return err }

var cancelErr error
for {
    event, err := turn.Next(waitCtx)
    if err != nil {
        if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
            cancelCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
            cancelErr = turn.Cancel(cancelCtx)
            cancel()
        }
        break
    }
    if text, ok := event.(*jcode.TextDelta); ok {
        io.WriteString(out, text.Text)
    }
}

terminalWaitCtx, cancelTerminalWait := context.WithTimeout(context.Background(), 30*time.Second)
defer cancelTerminalWait()
result, err := turn.Wait(terminalWaitCtx)
if err != nil { return errors.Join(cancelErr, err) } // only this fresh Wait was interrupted
switch result.Kind {
case jcode.TurnResultCompleted:
    return nil
case jcode.TurnResultCanceled:
    return result.Err
default:
    return result.Err
}
```

When the event wait is interrupted, the example explicitly requests server-side
cancellation and then waits for the terminal outcome with a fresh bounded context.
The lifecycle context passed to `StartTurn` owns the turn. Canceling it records
`TurnResultLifecycleCanceled` or `TurnResultLifecycleDeadlineExceeded` and does
not send protocol `cancel`. Contexts passed to `Accepted`, `Next`, `Cancel`, and
`Wait` bound only those calls. `Turn.Cancel` sends at most one shared protocol
request, and a successful acknowledgement remains non-terminal until the server
emits its terminal event. The first terminal signal is stored immutably, so all
later `Wait` calls return the same `TurnResult`.

In particular, canceling a `Wait` context is local. It does not stop server work.
Call `Turn.Cancel` for server-side cancellation, then use a fresh bounded context
for `Turn.Wait` so the interrupted context does not immediately abort the
terminal wait.


```go
create, err := protocol.NewRawRequest("create_session", map[string]any{
    "working_dir": workingDir,
})
if err != nil { return err }
reply, err := client.Request(ctx, create)
if err != nil { return err }

var fields json.RawMessage
if raw, ok := protocol.FieldsJSON(reply.Event); ok {
    fields = raw
} else {
    return errors.New("session creation returned no fields")
}
var sessions struct {
    SessionID string `json:"session_id"`
}
if err := json.Unmarshal(fields, &sessions); err != nil {
    return err
}
```

The raw request path remains available when an application needs a request or event added by a newer server:


`Request` waits for the correlated reply or `ctx.Done()`. Each request also has a 30-second SDK deadline by default, preventing a bridge or daemon that accepts a connection but never replies from hanging a caller indefinitely. Set `Options.RequestTimeout` to a positive duration to override that bound. Cancellation removes the pending request locally, but it does not necessarily cancel work already accepted by the server. For an owned turn, use `Turn.Cancel`; raw protocol callers may still send `cancel` directly.

[`examples/streaming`](examples/streaming) demonstrates a long-lived service. For a typed stream, use `session.Events(ctx)` and switch on `*TextDelta`, `*ToolStart`, `*ToolExec`, `*TokenUsage`, `*PermissionRequest`, and `*TurnDone`. The lower-level `Subscription` API remains useful when a service wants raw event fields:


```go
for {
    event, err := sub.Next(ctx)
    if err != nil {
        if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
            return nil
        }
        return err
    }
    switch event.Kind {
    case "text_delta":
        var value struct { Text string `json:"text"` }
        if err := event.Decode(&value); err != nil { return err }
        io.WriteString(out, value.Text)
    case "permission_request":
        // Apply your product's policy. Never blindly allow in an untrusted app.
    case "turn_done":
        return nil
    default:
        // Unknown event kinds are intentionally forward-compatible.
        log.Printf("jcode event kind=%s", event.Kind)
    }
}
```

Events are delivered to every subscription. A subscription buffer is bounded (`Options.EventBuffer`, default 128). If a consumer falls behind, that subscription terminates with `ErrSubscriberOverflow` rather than blocking all request traffic or silently dropping events. Consume promptly, increase the buffer deliberately, or fan out after a single reader. Do not call `Next` concurrently on one subscription.

## Cancellation, shutdown, and reconnect/resume

Use contexts for local deadlines and cancellation:

```go
ctx, cancel := context.WithTimeout(parent, 30*time.Second)
defer cancel()
reply, err := client.Request(ctx, request)
```

A per-method canceled context only abandons that method's local wait. A
`StartTurn` lifecycle context instead terminates the owned turn locally without
sending protocol cancellation. For server-side cancellation, call
`Turn.Cancel` and continue observing the turn until its terminal result.

The SDK does not automatically reconnect or replay events. Use `Reconnect` when you configure a `transport.Factory`; it retries connection setup according to `ReconnectPolicy`, and with `Resume: true` it sends the remembered session ID after the new handshake. In-flight requests are never retried. Persist session IDs in your application and resubscribe after reconnect:

```go
client, err := jcode.NewClient(ctx, conn, jcode.Options{
    SessionID: sessionID,
    Reconnect: jcode.ReconnectPolicy{
        Factory: func(ctx context.Context) (transport.Transport, error) {
            return transport.UnixSocket(socketPath)(ctx)
        },
        MaxAttempts: 5,
        Backoff: 500 * time.Millisecond,
        Resume: true,
    },
})
// After ErrDisconnected:
if err := client.Reconnect(ctx); err != nil { return err }
sub := client.Subscribe(sessionID)
```

A fresh subscription only receives events from attach onward. Protocol v1 cannot replay events emitted before the new subscription attaches, so use `get_history`/`peek_session` after reconnect if your application needs a consistent transcript. Never blindly repeat a mutating request after a timeout or disconnect: its server-side outcome may be unknown.

## Errors

Branch on sentinel errors where available, and preserve protocol error code/message fields for diagnostics:

- `ErrClosed`: client or transport has closed.
- `ErrDisconnected`: the client transport disconnected.
- `ErrSubscriberOverflow`: one subscription exceeded its bounded queue.
- `ErrTurnCanceled`: the server terminal event completed an explicit turn cancellation.
- `ErrProtocolFailure`: an owned turn ended on invalid framing, protocol data, or typed event decoding.
- `ErrBridgeExited`: the attached SDK-owned private bridge exited.
- `context.Canceled` and `context.DeadlineExceeded`: local caller cancellation/deadline.
- `protocol.ErrMalformedFrame`, `ErrInvalidFrame`, and `ErrFrameTooLarge`: invalid or unsafe wire data.
- A `protocol.Error` event: the harness rejected a request. Its `Code` is stable; its `Message` is diagnostic.

`TurnResult.Kind` is one of `TurnResultCompleted`, `TurnResultCanceled`,
`TurnResultLifecycleCanceled`, `TurnResultLifecycleDeadlineExceeded`,
`TurnResultProviderError`, `TurnResultProtocolError`,
`TurnResultSubscriberOverflow`, `TurnResultBridgeExited`,
`TurnResultTransportDisconnected`, or `TurnResultClientClosed`. Inspect
`TurnResult.Err` with `errors.Is` and `errors.As`. Owned-turn errors preserve
safe sentinels and provider codes, but omit provider messages, raw frames,
transport diagnostics, prompts, response content, credentials, private paths,
and session identifiers.

Ordinary disconnect closes active turn-owned subscriptions so a `Turn` never
appears as an unexplained empty stream. Raw subscriptions retain their existing
explicit-reconnect behavior. Only positive evidence from an attached
SDK-launched Linux `Instance` produces `TurnResultBridgeExited`; EOF on a
`Connect` client is `TurnResultTransportDisconnected`. Neither path retries,
replays, reconnects, or revives the ended turn automatically.

Treat `timeout`, disconnect, and transport failures as unknown-outcome for mutations. Retry idempotent reads only, and refresh session state after reconnect. Keep a default branch for future protocol error codes and event kinds.

## Private instance pattern

The Go SDK's `Launch` owns a private daemon, temporary home, credential policy, and cleanup. Use it when embedding jcode as an agent engine:

```go
inherit := false
client, err := jcode.Launch(ctx, jcode.LaunchOptions{
    WorkingDir:    workingDir,
    InheritLogins: &inherit,
    Provider:      "openrouter",
    Model:         "openai/gpt-5.6-luna",
    Env: map[string]string{
        "OPENROUTER_API_KEY": os.Getenv("OPENROUTER_API_KEY"),
    },
    ClientOptions: jcode.Options{ClientName: "my-service/1.0"},
})
if err != nil { return err }
defer client.Close()

session, err := client.CreateSession(ctx, jcode.CreateSessionOptions{
    WorkingDir: workingDir, // same explicit absolute worktree as LaunchOptions
})
if err != nil { return err }
```

`Launch` defaults to a temporary owner-only home and removes it on shutdown. Set `JcodeHome` to persist sessions. `LaunchInstance` starts the isolated process without dialing it, and its `SocketPath()` can be passed to `net.Dial("unix", ...)` and `NewClient`. `Provider` and `Model` become explicit global selections for the private jcode process; leave either empty to preserve jcode's normal auto/config resolution. Set `InheritLogins` to a bool pointer whose value is false to avoid copying/linking the user's recognized login files.

When `InheritLogins` is false, provide provider credentials explicitly through `LaunchOptions.Env`, for example `OPENROUTER_API_KEY`. Explicit environment entries replace same-named ambient variables, so API-key-only authentication is deterministic rather than dependent on duplicate environment-key behavior. Startup diagnostics redact explicit values for keys, tokens, and secrets. Never print or persist the credential value yourself, and use per-request context deadlines for `Session.Send`.


[`examples/private`](examples/private) uses `jcode.Launch`, the same explicit
absolute cwd for launch and session creation, `DetachInstance`, `Client.Close`,
and context-bounded `ShutdownInstance`. `Instance.Shutdown` and `Instance.Close`
use finite defaults; the additive helper lets a caller shorten the cooperative
grace period without skipping forced termination, bounded reap, or owned-path
cleanup. If a private process must use credentials, provision a dedicated
service identity instead of inheriting a developer's login files.

## Redacted lifecycle observations

Set `Options.Observer` or `LaunchOptions.Observer` to receive bounded lifecycle
metadata. The existing `Observer` contract remains synchronous, concurrency-safe
for callers to implement, and backend-neutral. The SDK does not create a
telemetry backend or generic event framework.

Lifecycle observations include:

- `launch_start`, launch preparation/process/socket phases, `launch_ready`, and a
  classified `launch_error`.
- `connect_start`, `connect_ready`, and classified `connect_error` events in
  addition to the existing connection `state` observations.
- `turn_start`, `turn_prompt_accepted`, the single `turn_first_event`, one shared
  cancellation-request start and result, and exactly one `turn_terminal` whose
  `Observation.Outcome` is the immutable `TurnResultKind`.
- `shutdown_start`, TERM grace start/completion, optional
  `shutdown_force_kill`, bounded reap completion, cleanup completion, and the
  final shutdown result for SDK-owned Linux private instances.

Observations contain classifications only. They never contain prompts,
credentials or tokens, response or tool content, raw protocol frames, raw
session IDs, secret-bearing environment values, or private runtime paths. Keep
observer implementations equally strict and fast because lifecycle code calls
them synchronously and may call them concurrently.

Linux maintainers can run the deterministic private-runtime acceptance and the explicitly gated real OpenAI OAuth smoke described in [`docs/private-runtime-acceptance.md`](docs/private-runtime-acceptance.md).

## Security guidance

- Unix sockets and runtime directories should be owner-only. Do not change permissions to make a shared socket world-readable.
- `connect` operates on the user's live sessions. Treat prompts, file contents, tool inputs, permission requests, and transcripts as sensitive.
- Do not log raw protocol frames, authorization headers, environment variables, credential paths, prompts, tool arguments, or model output by default. Redact secrets before structured logging.
- Credential inheritance is powerful and dangerous. A process using the user's bridge or login files can spend their quota and access their sessions. Disable inheritance for untrusted code and use a dedicated account for services.
- Validate permission requests in application policy. Do not auto-approve tools merely to make an example convenient.
- Bound frame sizes (`Options.MaxFrameSize`) and event buffers. Apply request deadlines and avoid unbounded transcript or output retention.
- Treat the private home as sensitive state. Use a real, dedicated directory, restrict permissions, and remove temporary homes only after the child process has stopped.

## Platforms and protocol compatibility

The protocol client is pure Go and compiles on platforms supported by Go. The
bounded private-process supervision contract in this stability program is
Linux-only. Windows supervision parity is intentionally out of scope. The
transport support matrix is:

| Platform | `transport.UnixSocket` | Notes |
| --- | --- | --- |
| Linux, macOS, and other Unix-like targets | Supported and tested by build | Uses the OS Unix-domain socket transport. |
| Windows | Explicitly unsupported | Supply a named-pipe/TCP `Transport` until a named-pipe adapter is added. |

The examples are Unix examples and are not expected to run on Windows unchanged. Cross-platform package compilation is covered by the release checks; live transport interoperability remains platform-specific.

The client negotiates **protocol major version 1** (`protocol.APIVersionMajor == 1`). Minor, additive event fields should be decoded permissively. Unknown event kinds are represented as `protocol.UnknownEvent`; preserve or ignore them rather than failing the whole connection. A major-version mismatch requires upgrading the bridge and SDK together. Go SDK releases are independent package releases, so pin a compatible jcode version in deployment and test the pair.

## Troubleshooting

| Symptom | Likely cause | Action |
| --- | --- | --- |
| `dial unix ...: no such file` | Bridge is not running or socket path is wrong | Run `jcode api-bridge`; check `JCODE_API_SOCKET`, `XDG_RUNTIME_DIR`, and permissions. |
| Hello handshake fails | Wrong socket, incompatible protocol, or non-jcode service | Confirm the endpoint is the harness API socket and upgrade both sides. |
| `ErrClosed` during a request | Bridge/daemon exited or transport was closed | Reconnect, then refresh sessions. Do not blindly repeat mutations. |
| `ErrSubscriberOverflow` | Consumer is slower than event production | Consume faster, increase `EventBuffer`, or fan out from one reader. |
| No text appears | Session is not attached or events are consumed after sending | Subscribe/attach before `send_message`; inspect all event kinds and `turn_done`. |
| Permission request blocks | Application has not answered the request | Apply an explicit allow/deny policy and send the corresponding response request. |
| Private process cannot start | Invalid binary, home, socket, or credentials | Capture child stderr without logging secrets; verify paths and use a dedicated home. |

## Migration from exec-based integrations

An exec integration commonly starts `jcode`, writes prompts to stdin, parses terminal text, and treats process exit as completion. Migrate in stages:

| Exec integration | Go SDK replacement |
| --- | --- |
| `exec.Command` plus shell/terminal parsing | Start `jcode api-bridge` or a private process, dial its API socket, call `NewClient`. |
| Prompt text on stdin | `Session.StartTurn`; compatible callers may retain `Session.Send` or raw `Notify`. |
| Scraping stdout for tokens | `Turn.Next`; compatible callers may retain `Session.Events` or raw `Subscribe`. |
| Killing the child for cancellation | `Turn.Cancel`, a fresh bounded `Turn.Wait`, then owned instance shutdown. |
| Assuming process exit means success | Inspect the immutable typed `TurnResult`. |
| Re-running the whole command after a timeout | Reconnect, refresh history, and retry only operations known to be safe. |

Keep the old exec path as a fallback while validating parity. Do not run both paths against the same live session unless you intentionally want concurrent actors. Once the SDK path is stable, remove shell quoting and terminal scraping, add explicit deadlines and permission policy, and redact protocol diagnostics before logging.

## Examples and compile checks

The three examples are ordinary Go packages and are checked by:

```bash
gofmt -d .
go test ./...
go vet ./...
go build ./examples/oneshot ./examples/streaming ./examples/private
```

They require a live jcode bridge only at runtime. `go build` and `go test` do not contact a daemon.

## Canonical validation and publication

The canonical source for this module is `sdk/go/` in Ariel Frischer's Jcode repository. `github.com/ariel-frischer/jcode-go` is the public publication destination. The public repository also owns repository-specific governance, specifications, worktree/task state, automation, and maintainer documentation; those paths are not SDK payload and must never be removed by publication.

From the Jcode repository root, run the complete non-mutating validation contract:

```bash
scripts/validate_go_sdk.sh
```

It reports all seven boundaries even if one fails: formatting, module consistency (`go mod tidy -diff` and `go mod verify`), vet, build, tests, race tests, and Windows amd64 build. CI runs the same command with Go 1.23.x and 1.24.x. Results from a newer local Go toolchain are supplementary rather than matrix evidence.

Publication begins with a deterministic, read-only preview:

```bash
scripts/sync-jcode-go.sh preview \
  --source sdk/go \
  --destination /absolute/path/to/jcode-go > /tmp/jcode-go.manifest
```

Review the timestamp-free manifest and verify its source/destination fingerprints, named include/protect rules, exact operations, and retained exclusions. Preview is also the default mode and changes neither tree. A future explicitly authorized publication may apply that exact reviewed manifest only while both inputs remain unchanged:

```bash
scripts/sync-jcode-go.sh apply \
  --source sdk/go \
  --destination /absolute/path/to/jcode-go \
  --manifest /tmp/jcode-go.manifest
```

Apply rejects stale, malformed, unsafe, dirty, wrong-branch, or wrong-repository inputs before writing. Preview and validation alone do not authorize live publication. Applying the reviewed manifest, committing the public repository, and pushing `main` each require explicit maintainer approval.
