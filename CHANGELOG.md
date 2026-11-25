# Changelog

## 0.2.0 (2026-11-26)

- `claim-once-lab --check` exits 1 if claim-once ran any effect twice or lost one, so the lab works as a CI gate. `labProblems(rows)` is exported from `claim-once/lab` for the same check in code.
- CI runs the full lab (20 operations, 8 runners) with `--check` on Node 20, 22 and 24 and writes the table to the job summary.
- README: how to pass the fencing token to your own database so a paused worker cannot overwrite a newer result, and a comparison with idemfence, Redis locks and BullMQ `jobId`. A Postgres store is not part of this release.

## 0.1.0

- First release: Redis Lua claim state machine (pending, done, failed, in_doubt, free) with leases, heartbeats and fencing tokens.
- `runOnce`, manual `reserve` / `begin` / `complete`, `inspect`, `resolve` and a `reconcile` hook.
- `onceProcessor` for BullMQ and `onceTool` for tool calls.
- `claim-once-lab` crash lab with real child processes and SIGKILL.
