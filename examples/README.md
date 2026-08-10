# Go SDK examples

These examples are compile-checkable packages:

- `oneshot`: connect to a shared `jcode api-bridge`, create a session in an explicit absolute cwd, and use `StartTurn`, `Accepted`, `Next`, `Cancel`, and `Wait`. Interrupted event waits request server-side cancellation and use a fresh bounded context for the terminal wait.
- `streaming`: attach to an existing session through the compatible `Session.Events` API, with bounded buffering and forward-compatible event handling that does not log raw fields.
- `private`: launch a Linux private runtime with an explicit `LaunchOptions.WorkingDir`, use the same cwd for session creation, transfer ownership with `DetachInstance`, close the client transport, and clean up with context-bounded `ShutdownInstance`.

From the repository root:

```bash
go build ./examples/oneshot ./examples/streaming ./examples/private
```

Runtime examples use Unix sockets and require a local jcode installation. Linux is the supported private-process supervision target. Existing `Session.Send`, `Session.Events`, and raw protocol APIs remain available for compatibility, but `StartTurn` is the API for owning acceptance, cancellation, and an immutable terminal result.

Do not log raw prompts, credentials, response or tool content, environment values, protocol frames, private session IDs, or private runtime paths.
