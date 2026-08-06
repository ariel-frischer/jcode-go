# Go SDK examples

These examples are compile-checkable packages:

- `oneshot`: connect to the shared `jcode api-bridge`, create a session, send one prompt, and stream text until `turn_done`.
- `streaming`: run a long-lived consumer for an existing `JCODE_SESSION_ID`, with bounded buffering and forward-compatible event handling.
- `private`: supervise an isolated bridge process with a separate `JCODE_HOME` and API socket. Prefer `jcode.Launch` in applications that do not need custom process supervision.

From `sdk/go`:

```bash
go build ./examples/oneshot ./examples/streaming ./examples/private
```

Runtime examples using Unix sockets require a local jcode installation. Do not use the examples' permissive output handling as a production permission policy, and do not log raw prompts, credentials, tool arguments, or protocol frames.
