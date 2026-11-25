# claim-once

[![npm](https://img.shields.io/npm/v/claim-once)](https://www.npmjs.com/package/claim-once)
[![CI](https://github.com/Krrishkumar125/claim-once/actions/workflows/ci.yml/badge.svg)](https://github.com/Krrishkumar125/claim-once/actions/workflows/ci.yml)

**Run a side effect at most once per operation key, across every worker, with Redis. When it cannot know, it says so.**

Queues and agents retry. BullMQ hands a stalled job to another worker. A tool call gets replayed after a timeout. A process dies between sending the email and writing down that it sent it. Each of those is a chance to charge a card twice, or to quietly never charge it at all.

`claim-once` gives each operation a key and a small state machine in Redis. One caller wins the claim. A repeat call gets the stored result back. If a worker crashed after the effect may have started, the key goes to `in_doubt` and stays there until you check the real system, instead of the library guessing.

```bash
npm install claim-once ioredis
```

```ts
import { Redis } from 'ioredis';
import { createClaimOnce } from 'claim-once';

const once = createClaimOnce({ redis: new Redis(process.env.REDIS_URL), namespace: 'billing' });

const receipt = await once.runOnce(`refund:${orderId}`, async ({ idempotencyKey }) => {
  return stripe.refunds.create({ payment_intent: pi }, { idempotencyKey });
});
```

Call it again with the same key, from any process, and you get the same `receipt` back without a second refund.

## What it guarantees, and what it does not

This is **at most once effect execution with an explicit in_doubt state**. It is not "exactly once". Nothing that talks to an outside system over a network can promise that on its own.

- Under any number of concurrent callers on any number of machines, at most one holds a live claim on a key. Every state change is one Lua script, so Redis runs it atomically.
- If a worker dies **after claiming but before the effect began**, the lease runs out and the next caller takes over with a higher fencing token. Nothing is lost.
- If a worker dies **after the effect began but before it reported back**, nobody can tell from Redis whether the charge went through. The key becomes `in_doubt` and every later call throws `InDoubtError` until you reconcile it. You get no duplicate, and no silent loss either, but you do get work to do.
- A slow worker that was only stalled (not dead) can still finish with its token and settle the key as done.

The limits, plainly:

- **Redis is the source of truth.** If Redis loses writes (failover without persistence, a flushed instance), claims are lost with it. Use a Redis with AOF or replicas you trust for this.
- **The effect itself can still run twice in one case:** your reconcile hook says `not_happened` when it actually did happen. Reconcile against the real system, using the idempotency key.
- **Leases use the Redis server clock,** so worker clock skew does not matter, but a worker blocked longer than its lease will be treated as gone. Pick a lease longer than your worst pause, and let heartbeats extend it.
- **Downstream idempotency is still the best protection.** `ctx.idempotencyKey` is stable for a key across every attempt. Pass it to Stripe, your email provider, or your own API so a retry after `in_doubt` is harmless there too. `ctx.token` strictly increases per key if your downstream can reject stale writers.

## How it works

Each key is one Redis hash with a state:

| State | Meaning | Next call gets |
|---|---|---|
| `pending` | Claimed, lease live | `InProgressError` (or waits, with `onBusy: 'wait'`) |
| `pending`, lease ran out, effect not begun | Owner died before doing anything | A takeover with a new token |
| `in_doubt` | Lease ran out after the effect began, or the effect threw an ambiguous error | `InDoubtError`, or your `reconcile` hook runs |
| `done` | Result stored | The stored result |
| `failed` | Marked failed for good | `FailedError` |
| `free` | Released after a failure that did not run the effect | A new claim |

`runOnce` does this for you: `reserve`, then `begin` right before calling your function, a heartbeat every `leaseMs / 3`, then `complete` with the result. You can call those steps yourself for multi-step work:

```ts
const r = await once.reserve('payout:42');
if (r.status === 'reserved') {
  const { claim } = r;
  const quote = await loadQuote();      // safe: a crash here frees the key after the lease
  await claim.begin();                  // from here on, a lost lease means in_doubt
  const res = await bank.payout(quote, { idempotencyKey: claim.idempotencyKey });
  await claim.complete(res);
}
```

## Fencing your own writes

The fencing token protects Redis, and you can pass it on so a downstream store protects itself too. Every claim gets a token higher than any earlier claim on the same key, and `runOnce` hands it to your function as `ctx.token`. Store it with the write and refuse writes that carry a lower one:

```ts
await once.runOnce(`ledger:${orderId}`, async ({ token }) => {
  // Postgres: the row only changes if no newer claim has written it.
  const res = await sql`
    UPDATE orders SET status = 'refunded', fence = ${token}
    WHERE id = ${orderId} AND fence < ${token}`;
  if (res.count === 0) throw new Error('a newer claim already wrote this order');
});
```

A worker that was paused (a GC pause, a frozen VM) and comes back after a takeover then cannot overwrite the newer result, even if its own Redis calls are slow to fail.

## Errors from your function

By default a thrown error **releases** the key, which means "the effect did not happen, a retry is fine". That is right for a declined card or a 400. It is wrong for a timeout after the request was sent. Tell it which is which:

```ts
await once.runOnce(key, sendInvoice, {
  onError: (err) => (isTimeout(err) ? 'in_doubt' : isBadRequest(err) ? 'fail' : 'release'),
});
```

`maxAttempts` on the client turns the Nth release into `failed`.

## Reconciling in_doubt keys

Pass a `reconcile` hook and `runOnce` calls it once when it finds an in_doubt key:

```ts
const once = createClaimOnce({
  redis,
  namespace: 'billing',
  reconcile: async ({ idempotencyKey }) => {
    const found = await stripe.refunds.list({ payment_intent: pi }); // or look up your own ledger
    const hit = found.data.find((r) => r.metadata.key === idempotencyKey);
    if (hit) return { status: 'done', result: hit };
    return { status: 'not_happened' }; // only if you are sure; this allows a new attempt
    // return undefined to leave it in_doubt for a human
  },
});
```

Or settle keys from a script with `once.inspect(key)` and `once.resolve(key, token, outcome)`. `resolve` needs the token from the in_doubt record, so two reconcilers cannot both settle the same key.

## BullMQ

```ts
import { Worker } from 'bullmq';
import { onceProcessor } from 'claim-once';

new Worker('billing', onceProcessor(once, async (job, { idempotencyKey }) => {
  return chargeCustomer(job.data, { idempotencyKey });
}), { connection });
```

The key defaults to `<queueName>:<job.id>`. If the same work can arrive as different jobs (a user double clicks and two jobs get added), key it by the business id instead: `onceProcessor(once, fn, { key: (job) => 'charge:' + job.data.orderId })`.

When a stalled job is redelivered while the first worker still holds the claim, the second gets `InProgressError`, so BullMQ retries it later and it then receives the stored result. Give the queue `attempts` and a `backoff`. An `InDoubtError` means a person or a reconcile hook needs to look; you may want to move those jobs aside with BullMQ's `UnrecoverableError`.

## Tool calls

```ts
import { onceTool } from 'claim-once';

const sendEmail = onceTool(once, 'send_email', async (args: { to: string; body: string }) => {
  return mailer.send(args);
});

// Many tool frameworks call execute(args, { toolCallId }). The helper reads that id.
await sendEmail({ to, body }, { toolCallId: 'call_abc' });
```

A replayed call with the same tool name and call id returns the first result. Pass `callId: (args, options) => ...` if your framework names it differently.

## The crash lab

`claim-once` ships a lab that starts real Node processes against a real Redis, kills them with `SIGKILL` at chosen points, and counts how many times the side effect ran. The effect appends a line to a file, so a kill cannot undo an effect that already happened.

```bash
npx claim-once-lab --redis redis://127.0.0.1:6379
```

It compares four strategies:

- **no guard**: just run it.
- **check, run, then set done**: `GET done`, run, `SET done`. The common first attempt.
- **SET NX first, then run**: mark the key before the effect, so a crash after marking forgets the work.
- **claim-once**: `reserve`, `begin`, run, `complete`.

And four scenarios. In the crash scenarios each operation's first worker is killed at the crash point, the lease runs out, and a second worker gets every job again, the way a queue redelivers.

This is the output of a real run (20 operations per scenario, 8 concurrent runners, 400 ms lease, Redis 8.10 on Linux, Node 24):

| Scenario | Strategy | Ops | Effects run | Duplicates | Lost | in_doubt |
|---|---|---:|---:|---:|---:|---:|
| SIGKILL after the effect, before it is recorded | no guard | 20 | 40 | 20 | 0 | 0 |
| SIGKILL after the effect, before it is recorded | check, run, then set done | 20 | 40 | 20 | 0 | 0 |
| SIGKILL after the effect, before it is recorded | SET NX first, then run | 20 | 20 | 0 | 0 | 0 |
| SIGKILL after the effect, before it is recorded | claim-once | 20 | 20 | 0 | 0 | 20 |
| SIGKILL after the claim, before the effect | no guard | 20 | 20 | 0 | 0 | 0 |
| SIGKILL after the claim, before the effect | check, run, then set done | 20 | 20 | 0 | 0 | 0 |
| SIGKILL after the claim, before the effect | SET NX first, then run | 20 | 0 | 0 | 20 | 0 |
| SIGKILL after the claim, before the effect | claim-once | 20 | 20 | 0 | 0 | 0 |
| event loop blocked past the lease, job redelivered | no guard | 20 | 40 | 20 | 0 | 0 |
| event loop blocked past the lease, job redelivered | check, run, then set done | 20 | 40 | 20 | 0 | 0 |
| event loop blocked past the lease, job redelivered | SET NX first, then run | 20 | 20 | 0 | 0 | 0 |
| event loop blocked past the lease, job redelivered | claim-once | 20 | 20 | 0 | 0 | 0 |
| concurrent runners on the same keys | no guard | 20 | 160 | 140 | 0 | 0 |
| concurrent runners on the same keys | check, run, then set done | 20 | 160 | 140 | 0 | 0 |
| concurrent runners on the same keys | SET NX first, then run | 20 | 20 | 0 | 0 | 0 |
| concurrent runners on the same keys | claim-once | 20 | 20 | 0 | 0 | 0 |

160 worker processes were SIGKILLed in that run.

How to read it:

- Killed **after the effect**: claim-once ran each effect once and left all 20 keys `in_doubt`, which is the honest answer. The marking strategy also ran each once, but only because it never retries anything, which the next row shows.
- Killed **before the effect**: marking first lost all 20 operations with nothing flagging them. claim-once saw that the effect never began and took over.
- **Stalled past the lease**: the stalled worker could not heartbeat, a second worker started, saw a lease that ran out after the effect began, and backed off with in_doubt. The first worker then woke up, ran the effect and settled the key as done with its own token.
- **Concurrent**: one claim per key won, the rest got `InProgressError`.

The same lab runs in CI on every push, on Node 20, 22 and 24, with `--check`: the build fails if claim-once runs any effect twice or loses one, and the table for that run is in the job summary of the [CI workflow](https://github.com/Krrishkumar125/claim-once/actions/workflows/ci.yml). Run it yourself with `npx claim-once-lab --check`.

The lab uses keys under `claim-once-lab:*` and deletes them when it finishes. Point it at a scratch Redis anyway. You can also run it from code: `import { runLab, formatTable } from 'claim-once/lab'`.

## Compared with

Checked on 26 Nov 2026.

| | **claim-once** | [idemfence](https://www.npmjs.com/package/idemfence) 0.1.0 | Redis locks (Redlock and similar) | BullMQ `jobId` |
| --- | --- | --- | --- | --- |
| Scope | Any side effect: jobs, tool calls, scripts | HTTP requests (Idempotency-Key header) | Mutual exclusion | Enqueueing |
| Fencing tokens | Yes | Yes | No | No |
| Stores and replays the result | Yes | Yes, the HTTP response | No | No |
| Says "unknown" after a crash mid-effect (`in_doubt` plus reconcile hook) | Yes | No (none in its 0.1.0 code or docs) | No | No |
| Store | Redis | PostgreSQL | Redis | Redis |
| Crash lab with real SIGKILL | Yes, in CI | No | No | No |

If your idempotency boundary is an HTTP API and your data lives in Postgres, idemfence is built for that. claim-once is for work that happens inside workers and agents, where the question after a crash is whether the effect already happened.

A Postgres store for claim-once is not in this release. The state machine is small, but every transition has to stay one atomic step with the same lease and token rules, and that needs its own crash lab run before it ships.

## API

`createClaimOnce(options)` returns a `ClaimOnce`.

| Option | Default | |
|---|---|---|
| `redis` | required | An ioredis client or Cluster |
| `namespace` | `claim-once` | Key prefix |
| `leaseMs` | `30000` | How long a claim stays exclusive without a heartbeat |
| `retentionMs` | 7 days | How long records, including results, are kept |
| `maxAttempts` | `0` (no limit) | Releases after which the key becomes failed |
| `owner` | host, pid, random | Written to records, for debugging |
| `serialize` / `deserialize` | JSON | How results are stored |
| `reconcile` | none | Default hook for in_doubt keys |

Methods:

- `runOnce(key, fn, { leaseMs, heartbeatMs, onBusy, waitTimeoutMs, onError, reconcile })`. `fn` receives `{ key, idempotencyKey, token, signal }`. `signal` aborts if the heartbeat finds the claim was taken away.
- `reserve(key)` returns `reserved` (with a `Claim`), `done`, `busy`, `in_doubt` or `failed`.
- `Claim`: `begin()`, `heartbeat()`, `complete(result)`, `release(error?)`, `fail(error)`, `markInDoubt(error?)`. Each throws `StaleClaimError` if the token is no longer current.
- `inspect(key)`, `resolve(key, token, outcome)`, `forget(key)`.

Errors: `InProgressError`, `InDoubtError`, `FailedError`, `StaleClaimError`, all extending `ClaimOnceError` and carrying `key`.

Results must survive your `serialize` function. With the default JSON, a `Date` comes back as a string.

## Requirements

Node 18 or newer. Redis 6 or newer with Lua scripting (tested on Redis 7 and 8). `ioredis` 5 is a peer dependency. `bullmq` is optional and only needed for your own workers.

## License

MIT, Krrish Kumar
