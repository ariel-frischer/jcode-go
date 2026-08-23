# Linux SDK lifecycle architecture

Status: architecture contract for `jcode-go-70h.1`

## Scope

This document defines the minimum lifecycle boundary required for the Linux private-runtime work in `jcode-go-70h.2` through `jcode-go-70h.6`. It preserves the current protocol v1 client and public APIs while adding one turn handle and bounded Linux supervision. It does not introduce a generic lifecycle framework.

Windows supervision, Locus workflow policy, retries, provider selection, automatic fallback, swarm behavior, and hosted execution are out of scope.

## Repository and wire ownership

This repository's `dev` branch is the sole development source for the Go SDK
implementation, exported APIs, lifecycle behavior, tests, examples, and Go CI.
Reviewed version tags are release boundaries. The Jcode repository does not
carry a second Go implementation.

The serialized protocol-v1 Rust wire contract remains owned by
`crates/jcode-harness-api` in the separate Jcode repository. Compatibility is
checked through one read-only boundary: Jcode invokes this repository's
protocol tests with `JCODE_REPO_ROOT` set to its checkout. The Go tests read Go
source from this module and Rust wire source from that explicit root. No copy,
projection, mirror, generated payload, submodule, or reverse-sync path exists.

## Current contract that must remain true

The existing implementation establishes these compatibility constraints:

- `Client` performs the protocol v1 handshake, correlates replies by request ID, serializes writes, and publishes each asynchronous event to bounded subscriptions.
- `Session.Send` subscribes before `send_message`, waits for `message_accepted`, does not retry, and treats `SendOptions.NoReply` as a notification-only operation.
- `Session.Events` returns a typed view over a subscription. `Next(ctx)` controls each wait, unknown event kinds remain forward-compatible, and a slow consumer fails only its own subscription with `ErrSubscriberOverflow`.
- `Connect` is non-owning. `Client.Close` closes only its transport and never stops a shared runtime.
- `Launch` attaches an `Instance` to its returned `Client`. Unless ownership is transferred by `DetachInstance`, `Client.Close` also shuts down that private instance.
- A successful `DetachInstance` transfers private-runtime ownership exactly once. Closing the client afterward is transport-only, and the caller must call `Instance.Shutdown` or `Instance.Close`.
- `Client.Close`, `Instance.Shutdown`, and `Instance.Close` are idempotent.
- Requests and mutations are never automatically replayed after timeout or disconnect. Their server-side result can be unknown.

The current source also exposes gaps that the downstream work addresses. `Session.Send` does not represent the whole turn, local context cancellation does not send protocol `cancel`, ordinary EOF can leave an event subscription waiting for an explicit reconnect, and Unix termination currently signals only the bridge PID before an unbounded reap.

## Typed session metadata validation boundary

The typed session helpers validate their additive request metadata before crossing
the protocol or lifecycle boundary. `Client.CreateSession` validates `Profile`
before emitting observations or allocating, encoding, or writing a request.
`Session.Send` and `Session.StartTurn` validate `MaxTurns`, `TokenBudget`, and
`Deadline` before allocating or writing a request, creating a subscription, or
constructing an owned `Turn`. Zero values omit the additive fields and retain the
legacy request shape. This validation applies only to the typed helpers: raw
`Request`, `Notify`, and `Subscribe` remain caller-owned, forward-compatible
protocol access and are neither rewritten nor subjected to typed-option
validation.

After typed validation succeeds, the existing ownership and lifecycle contracts
remain authoritative:

- `Session.Send` retains its acceptance-wait and `NoReply` notification semantics.
- `Turn.Cancel` remains the sole explicit server-cancellation path, and the first
  serialized terminal signal still records the immutable `TurnResult`.
- Mutating operations are not retried or replayed after timeout or disconnect,
  when their server-side outcome may be unknown.
- `Instance` ownership, attachment, one-time detachment, and bounded cleanup are
  unchanged.
- Private-runtime supervision and cleanup remain Linux-only; this metadata
  validation adds no Windows behavior.

## Sole ownership

Each responsibility has exactly one owner.

| Responsibility | Sole owner | Boundary |
| --- | --- | --- |
| Request correlation and subscriptions | `Client` | Allocates request IDs, matches replies, owns the reader, bounds subscription queues, and wakes pending operations. |
| Turn acceptance, event consumption, explicit server cancellation, and terminal outcome | `Turn`, created by the session-level API | Owns one underlying session subscription from before `send_message` until one terminal result. It is the only component that constructs the public turn cancellation flow. |
| Transport disconnect and protocol failure | `Client` | Detects read, write, decode, framing, and socket failures, updates connection state, and broadcasts a typed internal failure signal. |
| Private bridge, daemon, process group, socket, runtime directory, and ephemeral-home cleanup | `Instance` | Owns only the processes and paths created or explicitly claimed by `LaunchInstance`. |

`Session` remains a lightweight typed session view. It creates a `Turn` but does not independently supervise transport or processes. `Turn` maps a failure signal received from `Client` into its own immutable terminal result, but it does not detect or repair the connection. `Client` may invoke shutdown for an attached private `Instance`, but `Instance` remains the sole implementation owner of process and path cleanup.

## Working directory contract

The caller-selected `WorkingDir` is the Jcode agent working directory, not merely SDK metadata.

For a private worker, the caller must pass the same explicit target worktree as both:

- `LaunchOptions.WorkingDir`, which becomes `exec.Cmd.Dir` for the private Jcode bridge and its agent processes.
- `CreateSessionOptions.WorkingDir`, which becomes the session working directory sent to the harness.

Empty `LaunchOptions.WorkingDir` continues to mean the current directory for source compatibility, but orchestrators and acceptance tests must use an explicit absolute path. SDK implementation workers execute from their assigned `jcode-go` worktree. They do not execute from the Locus repository.

## Legal lifecycle states

These state machines are local to their owners. No shared framework or cross-object state enum is required.

### Client

The existing exported states remain the legal states:

- `connecting`
- `connected`
- `disconnected`
- `reconnecting`
- `closing`
- `closed`

Legal transitions are:

```text
connecting -> connected
connecting -> closing -> closed
connected -> disconnected
connected -> closing -> closed
disconnected -> reconnecting
reconnecting -> connecting -> connected
reconnecting -> disconnected
reconnecting -> closing -> closed
disconnected -> closing -> closed
```

`closed` is terminal. Reconnect remains explicit. A fatal frame or protocol decode failure terminates the current connection and its subscriptions, but a caller may still create a fresh connection through the existing explicit reconnect contract. In-flight requests are never replayed.

### Turn

A `Turn` has these internal states:

- `starting`: its subscription is installed and `send_message` has not completed its write.
- `awaiting_acceptance`: the write completed and `message_accepted` has not been observed.
- `accepted`: the server accepted the prompt and turn events may be consumed.
- `cancel_requested`: the SDK sent the one allowed protocol cancellation request and awaits a server terminal signal.
- `terminal`: one immutable `TurnResult` has won.

Legal transitions are:

```text
starting -> awaiting_acceptance
awaiting_acceptance -> accepted
awaiting_acceptance -> cancel_requested
accepted -> cancel_requested
starting | awaiting_acceptance | accepted | cancel_requested -> terminal
```

A successful terminal event implies acceptance if it races ahead of the acceptance waiter. A provider or protocol failure before acceptance ends the turn without reporting successful acceptance. `cancel_requested` is not terminal by itself.

### Instance

An SDK-owned private instance has these internal states:

- `starting`: paths are prepared and the bridge is starting.
- `running`: the API socket accepted a connection and an `Instance` handle was returned.
- `shutting_down`: one caller owns the shutdown sequence while concurrent callers wait for the same result.
- `closed`: shutdown attempts and owned-path cleanup finished.

Legal transitions are:

```text
starting -> running -> shutting_down -> closed
starting -> startup failure with best-effort cleanup and no returned Instance
```

`closed` is terminal. An `Instance` is not restartable.

## One terminal outcome per turn

Every `Turn` records exactly one `TurnResult`. The first terminal signal wins through one synchronization point such as `sync.Once` or an equivalent locked compare-and-set. The stored result is immutable and every later wait returns the same result. Late or conflicting terminal signals may be observed for redacted diagnostics but cannot replace the result.

The required semantic terminal classes are:

- successful `turn_done`
- explicit server cancellation
- local lifecycle context cancellation
- local lifecycle deadline expiry
- provider failure
- protocol or framing failure
- subscriber overflow
- owned bridge exit
- transport disconnect
- local client closure through the existing `ErrClosed` contract

The exported terminal kinds and causes are fixed as follows:

| Semantic class | `TurnResult.Kind` | Stable cause inspection |
| --- | --- | --- |
| successful completion | `TurnResultCompleted` (`"completed"`) | `Err == nil` |
| explicit server cancellation | `TurnResultCanceled` (`"canceled"`) | `errors.Is(Err, ErrTurnCanceled)` |
| lifecycle cancellation | `TurnResultLifecycleCanceled` (`"lifecycle_canceled"`) | lifecycle `context.Cause`, normally `context.Canceled` |
| lifecycle deadline | `TurnResultLifecycleDeadlineExceeded` (`"lifecycle_deadline_exceeded"`) | lifecycle `context.Cause`, normally `context.DeadlineExceeded` |
| provider failure | `TurnResultProviderError` (`"provider_error"`) | `errors.As` into `EventError` with the safe provider code |
| protocol or framing failure | `TurnResultProtocolError` (`"protocol_error"`) | `errors.Is(Err, ErrProtocolFailure)` and an applicable framing sentinel |
| subscriber overflow | `TurnResultSubscriberOverflow` (`"subscriber_overflow"`) | `errors.Is(Err, ErrSubscriberOverflow)` |
| attached owned bridge exit | `TurnResultBridgeExited` (`"bridge_exited"`) | `errors.Is(Err, ErrBridgeExited)` |
| transport disconnect | `TurnResultTransportDisconnected` (`"transport_disconnected"`) | `errors.Is(Err, ErrDisconnected)` |
| local client close | `TurnResultClientClosed` (`"client_closed"`) | `errors.Is(Err, ErrClosed)` |

Every producer commits through the same Turn mutex. The first locked terminal
commit sets acceptance when necessary, stores the result, closes the terminal
notification once, and makes every later signal a no-op. Error values may wrap
the stable causes above, but an owned Turn never retains unsafe provider text,
transport diagnostics, prompts, response content, raw frames, credentials,
private paths, or session identifiers.

The following are not terminal by themselves:

- successful `message_accepted`
- a successful cancellation request before the server terminates the turn
- cancellation of a context used only to wait on `Accepted` or `Wait`
- a reconnect attempt

A turn lifecycle context and a wait context have different meanings. Cancellation of the lifecycle context supplied when starting the turn records a local cancellation or deadline result and never sends protocol `cancel`. Cancellation of a context supplied only to `Accepted`, `Next`, `Wait`, or `Cancel` stops that method call without changing an already-live turn, except when the method's underlying transport failure independently terminates the turn.

## Minimal additive public API direction

The existing APIs remain supported. Add one session-level `Turn` handle rather than changing `Session` into a stateful supervisor. The intended surface is:

```go
type Turn struct { /* unexported ownership state */ }

type TurnResult struct {
    Kind TurnResultKind
    Err  error
}

func (s Session) StartTurn(lifecycleCtx context.Context, content string, options SendOptions) (*Turn, error)
func (t *Turn) Accepted(ctx context.Context) error
func (t *Turn) Next(ctx context.Context) (TypedEvent, error)
func (t *Turn) Cancel(ctx context.Context) error
func (t *Turn) Wait(ctx context.Context) (TurnResult, error)
```

The exact exported `TurnResultKind` constants and stable values are the ones in
the terminal table above. `ErrTurnCanceled`, `ErrProtocolFailure`, and
`ErrBridgeExited` are the only new sentinels; existing `EventError`, context
causes, `ErrSubscriberOverflow`, `ErrDisconnected`, `ErrClosed`, and protocol
framing sentinels retain their established inspection contracts.

`StartTurn` must install its single underlying subscription before writing `send_message`. It returns after a successful write, not after acceptance. `Accepted` exposes acceptance separately from terminal completion. `Next` is the turn's ordered event consumer. `Wait` returns the immutable terminal result. Its second return value is only for interruption of that particular wait before a terminal result exists. Once terminal, `Wait` returns the stored result with a nil wait error.

Only one goroutine may call `Turn.Next` at a time. `Accepted`, `Cancel`, and `Wait` may run concurrently with event consumption. An internal dispatcher is the sole reader of the underlying `Subscription`, preventing acceptance waiting, terminal detection, and user event consumption from stealing events from one another.

`StartTurn` rejects `SendOptions.NoReply` before writing because a fire-and-forget notification has no owned turn lifecycle. Existing `Session.Send(..., SendOptions{NoReply: true})` remains the notification API.

### Explicit cancellation

`Turn.Cancel` constructs the protocol `cancel` request inside the SDK and sends it at most once. Concurrent and repeated calls share the first cancellation attempt and result. It does not retry after timeout or disconnect because the server may already have accepted the cancellation.

A successful cancellation request moves the turn to `cancel_requested`. The terminal result is recorded only when the server subsequently terminates the turn. If a terminal result wins before cancellation is sent, `Cancel` performs no write. If natural completion wins concurrently with cancellation, the first serialized terminal signal remains authoritative.

### Compatibility behavior

- `Session.Send` keeps its signature and current acceptance semantics. It still subscribes before sending, returns on `message_accepted`, does not wait for `turn_done`, and does not retry. Its context remains a local acceptance wait and does not imply server cancellation.
- `Session.Send` with `NoReply` keeps notification-only behavior.
- `Session.Events` keeps its signature and remains valid for existing callers. Construction does not send, cancel, or own a turn. `Next(ctx)` continues to bound each read, and callers must still call `Close`.
- `Client.Close` keeps its signature, idempotence, wake-up behavior, and existing `ErrClosed` compatibility. A shared client remains transport-only. A launched client still shuts down its attached private instance using finite default bounds.
- `Instance.Shutdown` and `Instance.Close` keep their signatures and become internally bounded and orphan-free on Linux. `Close` remains an alias for `Shutdown`.
- `DetachInstance` keeps its signature and one-time transfer behavior. After transfer, `Client.Close` cannot shut down the detached instance.
- Raw `Request`, `Notify`, and `Subscribe` remain available. No new turn API removes forward-compatible raw protocol access.

A context-aware instance shutdown entry point must be additive because adding a method to the exported `Instance` interface would break external implementations. The direction is an additive helper such as:

```go
func ShutdownInstance(ctx context.Context, instance Instance) error
```

The finite Linux guarantee applies to SDK-owned instances returned by `LaunchInstance` or detached from a launched client. `Instance.Shutdown` uses configured finite defaults. A caller deadline may shorten the graceful phase, but it cannot disable best-effort SIGKILL, bounded reap, or owned-path cleanup.

## Failure semantics

- Local lifecycle cancellation never constructs server cancellation. Callers that need the server turn stopped must call `Turn.Cancel` with a usable context before canceling local supervision.
- Acceptance timeout, cancellation timeout, request timeout, and transport disconnect leave a mutating server operation with a potentially unknown outcome. The SDK does not retry it.
- A provider error is preserved as a typed cause with its stable code. Human-readable server text remains diagnostic and must not be used as a branch condition.
- Fatal framing or protocol decode errors terminate the connection and every active turn using that connection.
- An ordinary transport disconnect terminates an active `Turn` even though lower-level raw subscriptions may retain their existing explicit-reconnect behavior. Protocol v1 cannot replay the exact missed turn events, so an owned turn cannot safely resume as if uninterrupted.
- Only the private turn-owned subscription installed by `StartTurn` is closed on an ordinary disconnect. Existing raw subscriptions remain caller-owned and available for explicit reconnect; the ended Turn is never revived.
- For a launched client, the `launchedInstance` process wait fans out a private bridge-exit signal without consuming the shutdown wait result. `Client` watches it only while that exact SDK-owned instance remains attached. The shared Turn terminal commit decides bridge exit versus EOF or close races. For `Connect`, detached instances, and arbitrary transport errors, no positive process ownership evidence exists, so EOF is a transport disconnect.
- Subscription overflow terminates only the affected raw subscription or turn. It must not block request correlation, the client reader, or other subscribers.
- Shutdown continues through all safe cleanup phases after an error. The context-aware path returns joined or wrapped causes after cleanup attempts. Existing `Client.Close` retains its legacy return contract, so cleanup detail is exposed through direct instance shutdown and redacted observations rather than a breaking return change.
- No failure value or observation may include prompts, credentials, response content, raw protocol frames, or private session identifiers.

## Concurrency and race invariants

1. Request IDs and subscription IDs are unique per `Client`. Reply correlation is independent of reply order.
2. The client reader never blocks on a slow subscriber. Overflow closes only that subscriber.
3. A turn subscription exists before `send_message` can produce `message_accepted` or any later event.
4. Exactly one internal turn dispatcher reads the underlying subscription. Public waits and event consumption cannot compete for frames.
5. Exactly one protocol cancellation write is possible per turn. Cancellation success is not itself terminal.
6. Exactly one terminal result wins. Acceptance, completion, cancellation, context expiry, overflow, protocol failure, disconnect, bridge exit, and client close may race without replacing the winner or panicking on closed channels.
7. `Client.Close` and `DetachInstance` serialize ownership transfer. Detach succeeds only before closing begins. Close atomically claims and clears an attached instance before shutdown. The same instance can never be both returned to the caller and shut down by the client.
8. Concurrent `Instance.Shutdown`, `Instance.Close`, `ShutdownInstance`, and client-owned shutdown calls wait for and return the same completed shutdown result.
9. Shutdown never sends a signal to PID 0, PID 1, an unrecorded process group, or a path outside the instance's recorded ownership set.
10. All public methods remain safe to race with `Client.Close`. They return a typed terminal or existing closure error rather than block forever.

## Linux private-instance shutdown

Linux supervision is implemented only in `launch.go` and `process_unix.go`. Windows parity is explicitly excluded from this program.

### Process ownership

Before `Start`, the bridge command is placed in a new process group with the bridge PID as the recorded process-group ID. The `Instance` records the command, process-group ID, one `Wait` result channel, instance-scoped daemon identity, socket path, runtime directory, Jcode home, whether the home is ephemeral, and the exact owned paths.

The SDK does not scan the system for Jcode processes. The daemon PID is considered owned only when read from the private instance registry and matched to the instance's private runtime socket. Missing processes and `ESRCH` are successful cleanup conditions.

### Bounded sequence

The first shutdown caller performs this sequence while concurrent callers wait:

1. Enter `shutting_down` and prevent any later ownership transfer.
2. Ask the instance-scoped daemon to stop while its private registry is still readable.
3. Send `SIGTERM` to the recorded owned process group using the negative process-group ID.
4. Wait for bridge exit for a configurable graceful interval. The default is 5 seconds. A shorter caller deadline shortens this wait.
5. If the bridge leader has not been reaped and the group remains alive or the grace interval expires, send `SIGKILL` to the same recorded process group.
6. Await the single existing `cmd.Wait` result channel for a bounded reap interval. The default is 5 seconds. Never call `Wait` twice and never wait indefinitely.
7. Remove owned socket files and other instance runtime files, then remove the owned runtime directory when safe. Bound repeated path cleanup by the existing `CleanupTimeout`, whose current default is 30 seconds.
8. If the SDK created the ephemeral `JCODE_HOME`, remove that owned home after process termination attempts. If the caller supplied a persistent home, retain the home and durable state while removing only this instance's owned runtime paths.
9. Record `closed`, store the combined result, and release all concurrent shutdown callers.

The default maximum after shutdown begins is therefore the 5-second TERM grace, the 5-second reap bound, and at most the configured 30-second owned-path cleanup bound. Signaling and daemon-stop requests must not add an unbounded wait. Configuration may reduce these values. Non-positive configuration selects finite defaults rather than disabling a bound.

If the caller context is already done or expires during grace, shutdown escalates immediately to `SIGKILL`. Final kill, bounded reap, and safe path cleanup still run under the instance's finite internal caps so context cancellation cannot intentionally leave an orphan. The returned error retains the caller context cause and any cleanup failure.

The recorded numeric process-group ID is actionable only while the bridge leader's identity remains unambiguous. Once the single `cmd.Wait` result reports that the leader was reaped, the SDK performs no further liveness check or signal against that process-group ID because Linux may reuse it for an unrelated group. Supported private bridges therefore keep the leader alive until descendants have exited or group-wide TERM/KILL escalation completes. A bridge-exit fixture must arrange for any transport-holding descendant to terminate through its own supervision rather than require unsafe post-reap group signaling.

### Owned-path safety

Cleanup operates from the instance's recorded paths, not from ambient environment variables at shutdown time. It validates that paths are real, non-symlink directories or the exact expected socket files beneath the recorded private home. It never removes a caller's shared Jcode home, unrelated runtime directory, or unrelated socket. Process termination attempts happen before ephemeral-home removal.

## Downstream responsibility allocation

Every implementation responsibility resulting from this document belongs to one approved downstream Bead.

| Bead | Assigned responsibility |
| --- | --- |
| `jcode-go-70h.2` | Add `Turn`, `Session.StartTurn`, acceptance waiting, the single internal event dispatcher, ordered `Next`, SDK-owned protocol `cancel`, idempotent cancellation, lifecycle-context handling, `Wait`, and compatibility tests for existing `Session.Send` and `Session.Events`. Do not add retry, reconnect, process supervision, or Locus policy. |
| `jcode-go-70h.3` | Put the Linux bridge in an owned process group. Add finite grace and reap configuration, the additive context-aware instance shutdown entry point, SIGTERM and SIGKILL group signaling, single bounded reap, daemon stop, owned socket/runtime/ephemeral-home cleanup, shutdown result sharing, and atomic `Client.Close` versus `DetachInstance` ownership. Do not implement Windows behavior or system-wide process discovery. |
| `jcode-go-70h.4` | Finalize `TurnResultKind` and typed/wrapped causes for every required terminal class. Connect `Client` transport, protocol, and owned bridge-exit signals to active turns. Enforce first-terminal-wins under conflicting signals and ensure EOF or bridge death cannot become an unexplained empty turn stream. Do not decide Locus retry policy or provider fallback. |
| `jcode-go-70h.5` | Add Linux public-path acceptance coverage for explicit cwd, prompt acceptance, event streaming, lifecycle-context cancellation, explicit server cancellation, timeout, bridge failure, TERM cooperation, TERM-ignoring descendants, KILL escalation, bounded reap, repeated shutdown, and removal of every owned process and path. Run focused race tests and document the separately gated real OAuth smoke. Do not add paid CI, Windows, or Locus integration. |
| `jcode-go-70h.6` | Add redacted lifecycle observations for launch, connect, acceptance, first event, cancellation request, terminal result, TERM grace, forced kill, reap, and cleanup. Update README and examples to use explicit cwd, the final turn API, correct ownership transfer, and Linux-only supervision language. Do not add a telemetry backend or emit sensitive content or identifiers. |

No downstream Bead owns a broad `Client` rewrite. Existing request correlation, raw subscriptions, handshake, reconnect, and forward-compatible event decoding are reused unless a focused change is required by the allocation above.

## Validation boundaries

- Architecture handoff: `go test ./... -count=1` and `git diff --check`. This documentation task does not run a live provider or change Go files.
- `jcode-go-70h.2`: deterministic unit and race tests for fast acceptance, event ordering, local lifecycle cancellation, explicit cancellation, duplicate cancellation, and existing API compatibility.
- `jcode-go-70h.3`: Linux integration tests with controlled child processes for process groups, TERM grace, KILL escalation, bounded reap, ownership races, and path cleanup. No live provider is required.
- `jcode-go-70h.4`: deterministic unit and race tests for every terminal class and conflicting terminal signals, including EOF, malformed frames, overflow, bridge exit, close, and cancellation races.
- `jcode-go-70h.5`: integrated public SDK fixtures, race-enabled lifecycle packages, full `go test ./... -count=1`, and an optional explicitly approved real OAuth smoke. The smoke must not print credentials or private identifiers.
- `jcode-go-70h.6`: example builds, documentation checks, observation redaction tests, and the normal repository suite.

Unit fixtures are sufficient for deterministic failure injection but are not the sole program acceptance evidence. The final real smoke must launch the public SDK in the caller-selected `jcode-go` worktree. Windows behavior, live Locus execution, provider mutation, deployment, push, and merge remain outside this repository architecture task.
