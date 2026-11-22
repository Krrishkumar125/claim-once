# Changelog

## 0.1.0

- First release: Redis Lua claim state machine (pending, done, failed, in_doubt, free) with leases, heartbeats and fencing tokens.
- `runOnce`, manual `reserve` / `begin` / `complete`, `inspect`, `resolve` and a `reconcile` hook.
- `onceProcessor` for BullMQ and `onceTool` for tool calls.
- `claim-once-lab` crash lab with real child processes and SIGKILL.
