# Linux private-runtime lifecycle acceptance

The Linux lifecycle acceptance suite exercises the public `Launch`, `CreateSession`, `StartTurn`, `Turn.Cancel`, `Turn.Wait`, `Client.Close`, `DetachInstance`, and `ShutdownInstance` APIs.

## Deterministic acceptance

Run the process-backed fixture without provider access:

```bash
go test -run '^TestPrivateRuntimeLifecycleAcceptance$' -count=1 -v .
```

The fixture launches an SDK-owned private bridge in the explicit absolute test worktree, creates a session with that same working directory, reproduces an accepted but stalled turn, cancels it through `Turn.Cancel`, and performs a fresh bounded `Turn.Wait`. The bridge leader and descendant then ignore TERM while the leader remains unreaped, proving bounded KILL escalation through the public shutdown path. A second path forces the owned bridge leader to exit while a descendant briefly holds the transport, which must produce `TurnResultBridgeExited` with `errors.Is(result.Err, ErrBridgeExited)`. That descendant self-terminates after preserving the bridge-exit ordering; shutdown must not inspect or signal the ambiguous numeric process group after leader reap.

Both paths record only synthetic process ownership evidence. Shutdown assertions verify that the recorded bridge and descendant identities, API socket, runtime directory, and ephemeral home are gone. A caller-owned marker outside the private home must remain. Diagnostics use stable phase and resource labels rather than raw process IDs, paths, prompts, frames, or session identifiers.

## Optional real OpenAI OAuth smoke

The real smoke is skipped unless explicitly gated. Run it from the exact absolute worktree that the private agent and session must use:

```bash
JCODE_GO_REAL_OAUTH_SMOKE=1 \
JCODE_GO_REAL_OAUTH_WORKDIR="$PWD" \
go test -run '^TestPrivateRuntimeRealOAuthSmoke$' -count=1 -timeout=4m -v .
```

The test requires `JCODE_GO_REAL_OAUTH_WORKDIR` to be absolute. It launches provider `openai` with model `gpt-5.6-luna`, creates the session in the same directory, completes one real turn through the public SDK, detaches the public `Instance` ownership handle, closes the client transport, shuts down the private runtime, and verifies removal of its ephemeral home and socket.

The smoke uses existing login inheritance. It does not mutate provider configuration or credentials. Its output is intentionally limited to redacted phase names, provider/model selection, stable terminal kind, bounded elapsed milliseconds, cleanup booleans, and stable error classifications. It never logs credentials, prompts, model output, raw frames, or raw session identifiers.
