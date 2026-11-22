import { createHash, randomBytes } from 'node:crypto';
import { hostname } from 'node:os';
import { FailedError, InDoubtError, InProgressError, StaleClaimError } from './errors.js';
import * as S from './scripts.js';
import type {
  ClaimOnceOptions,
  ClaimRecord,
  ClaimState,
  EffectContext,
  ErrorDecision,
  Reconcile,
  RedisLike,
  RunOnceOptions,
} from './types.js';

const DAY = 86_400_000;

/** Result of `reserve()`. Exactly one caller at a time gets `reserved`. */
export type ReserveResult =
  | { status: 'reserved'; claim: Claim; takeover: boolean }
  | { status: 'done'; result: unknown; token: number }
  | { status: 'busy'; owner: string; retryAfterMs: number; token: number }
  | { status: 'in_doubt'; token: number; owner: string }
  | { status: 'failed'; reason: string; token: number };

class Scripts {
  private readonly shas = new Map<string, string>();
  constructor(private readonly redis: RedisLike) {}
  async run(script: string, key: string, args: (string | number)[]): Promise<unknown> {
    let sha = this.shas.get(script);
    if (!sha) {
      sha = createHash('sha1').update(script).digest('hex');
      this.shas.set(script, sha);
    }
    try {
      return await this.redis.evalsha(sha, 1, key, ...args);
    } catch (err) {
      if (err instanceof Error && /NOSCRIPT/.test(err.message)) return this.redis.eval(script, 1, key, ...args);
      throw err;
    }
  }
}

function errorText(err: unknown): string {
  if (err instanceof Error) return `${err.name}: ${err.message}`.slice(0, 500);
  return String(err).slice(0, 500);
}

/** A live, exclusive claim on one key. Use it directly for multi-step work, or let `runOnce` drive it. */
export class Claim {
  constructor(
    private readonly core: ClaimOnce,
    readonly key: string,
    readonly token: number,
    readonly attempt: number,
  ) {}

  get idempotencyKey(): string {
    return this.core.idempotencyKey(this.key);
  }

  /**
   * Call right before the side effect. From here on, a lost lease means in_doubt instead of a
   * silent retry. Throws StaleClaimError if someone else took the key over in the meantime.
   */
  async begin(leaseMs?: number): Promise<void> {
    await this.owned(S.BEGIN, [this.token, leaseMs ?? this.core.leaseMs]);
  }

  /** Extend the lease. Throws StaleClaimError if the claim is no longer ours. */
  async heartbeat(leaseMs?: number): Promise<void> {
    await this.owned(S.HEARTBEAT, [this.token, leaseMs ?? this.core.leaseMs]);
  }

  /** Store the result. Later calls for this key get it back without running anything. */
  async complete(result: unknown): Promise<void> {
    await this.owned(S.COMPLETE, [this.token, this.core.serialize(result), this.core.retentionMs]);
  }

  /** The effect did not happen. Free the key for a retry (or mark failed after maxAttempts). */
  async release(error?: unknown): Promise<'released' | 'failed'> {
    const r = String(await this.core.scripts.run(S.RELEASE, this.core.redisKey(this.key), [this.token, error === undefined ? '' : errorText(error), this.core.maxAttempts]));
    if (r === 'released' || r === 'failed') return r;
    throw new StaleClaimError(this.key, this.token, r);
  }

  /** Mark the key failed for good. Later calls throw FailedError. */
  async fail(error: unknown): Promise<void> {
    await this.owned(S.FAIL, [this.token, errorText(error)]);
  }

  /** We cannot tell whether the effect happened. Park the key as in_doubt for reconciliation. */
  async markInDoubt(error?: unknown): Promise<void> {
    await this.owned(S.MARK_IN_DOUBT, [this.token, error === undefined ? '' : errorText(error)]);
  }

  private async owned(script: string, args: (string | number)[]): Promise<void> {
    const r = String(await this.core.scripts.run(script, this.core.redisKey(this.key), args));
    if (r !== 'ok') throw new StaleClaimError(this.key, this.token, r);
  }
}

export class ClaimOnce {
  readonly redis: RedisLike;
  readonly namespace: string;
  readonly leaseMs: number;
  readonly retentionMs: number;
  readonly owner: string;
  readonly maxAttempts: number;
  readonly serialize: (value: unknown) => string;
  readonly deserialize: (raw: string) => unknown;
  /** @internal */
  readonly scripts: Scripts;
  private readonly defaultReconcile?: Reconcile;

  constructor(opts: ClaimOnceOptions) {
    if (!opts?.redis) throw new TypeError('claim-once: `redis` (an ioredis client) is required');
    this.redis = opts.redis;
    this.namespace = opts.namespace ?? 'claim-once';
    this.leaseMs = opts.leaseMs ?? 30_000;
    this.retentionMs = opts.retentionMs ?? 7 * DAY;
    this.owner = opts.owner ?? `${hostname()}:${process.pid}:${randomBytes(3).toString('hex')}`;
    this.maxAttempts = opts.maxAttempts ?? 0;
    this.serialize = opts.serialize ?? ((v) => (v === undefined ? '' : JSON.stringify(v)));
    this.deserialize = opts.deserialize ?? ((raw) => (raw === '' ? undefined : JSON.parse(raw)));
    this.defaultReconcile = opts.reconcile;
    if (!(this.leaseMs > 0)) throw new RangeError('claim-once: leaseMs must be > 0');
    if (!(this.retentionMs > this.leaseMs)) throw new RangeError('claim-once: retentionMs must be greater than leaseMs');
    this.scripts = new Scripts(this.redis);
  }

  /** @internal */
  redisKey(key: string): string {
    if (typeof key !== 'string' || key.length === 0) throw new TypeError('claim-once: key must be a non-empty string');
    return `${this.namespace}:{${key}}`;
  }

  idempotencyKey(key: string): string {
    return `${this.namespace}:${key}`;
  }

  /** Try to take the key. Atomic: under any number of concurrent callers, at most one gets `reserved`. */
  async reserve(key: string, opts: { leaseMs?: number } = {}): Promise<ReserveResult> {
    const raw = (await this.scripts.run(S.RESERVE, this.redisKey(key), [this.owner, opts.leaseMs ?? this.leaseMs, this.retentionMs])) as string[];
    const [status, a = '', b = '', c = ''] = raw.map(String);
    switch (status) {
      case 'reserved':
        return { status, claim: new Claim(this, key, Number(a), Number(c)), takeover: b === '1' };
      case 'done':
        return { status, result: this.deserialize(a), token: Number(b) };
      case 'failed':
        return { status, reason: a, token: Number(b) };
      case 'in_doubt':
        return { status, token: Number(a), owner: b };
      case 'busy':
        return { status, token: Number(a), owner: b, retryAfterMs: Number(c) };
      default:
        throw new Error(`claim-once: unexpected script reply ${JSON.stringify(raw)}`);
    }
  }

  /** Read the record for a key, or null if none exists. */
  async inspect(key: string): Promise<ClaimRecord | null> {
    const rk = this.redisKey(key);
    const [h, ttl] = await Promise.all([this.redis.hgetall(rk), this.redis.pttl(rk)]);
    if (!h || !h.s) return null;
    const num = (v: string | undefined) => (v === undefined ? null : Number(v));
    return {
      key,
      state: h.s as ClaimState,
      token: Number(h.t ?? 0),
      owner: h.o ?? '',
      attempts: Number(h.a ?? 0),
      began: h.b === '1',
      leaseExpiresAt: num(h.l),
      createdAt: num(h.c),
      updatedAt: num(h.u),
      ...(h.r !== undefined ? { result: h.r } : {}),
      ...(h.e ? { error: h.e } : {}),
      ttlMs: ttl,
    };
  }

  /**
   * Settle an in_doubt key after checking the downstream system. `token` must be the one
   * reported by InDoubtError / inspect(), so two reconcilers cannot both settle it.
   */
  async resolve(
    key: string,
    token: number,
    outcome: { status: 'done'; result?: unknown } | { status: 'not_happened' } | { status: 'failed'; reason: string },
  ): Promise<void> {
    const payload = outcome.status === 'done' ? this.serialize(outcome.result) : outcome.status === 'failed' ? outcome.reason : '';
    const r = String(await this.scripts.run(S.RESOLVE, this.redisKey(key), [token, outcome.status, payload, this.retentionMs]));
    if (r !== 'ok') throw new StaleClaimError(key, token, r);
  }

  /** Delete a record. An operator tool; normal code should not need it. */
  async forget(key: string): Promise<boolean> {
    return (await this.redis.del(this.redisKey(key))) === 1;
  }

  /**
   * Run `fn` at most once for `key`. A repeat call returns the stored result.
   * Throws InProgressError, InDoubtError or FailedError instead of guessing.
   */
  async runOnce<T>(key: string, fn: (ctx: EffectContext) => Promise<T> | T, opts: RunOnceOptions<T> = {}): Promise<T> {
    const leaseMs = opts.leaseMs ?? this.leaseMs;
    const reconcile = (opts.reconcile ?? this.defaultReconcile) as Reconcile<T> | undefined;
    const waitUntil = Date.now() + (opts.waitTimeoutMs ?? leaseMs * 2);
    let reconciled = false;
    let delay = 25;

    for (;;) {
      const r = await this.reserve(key, { leaseMs });
      if (r.status === 'done') return r.result as T;
      if (r.status === 'failed') throw new FailedError(key, r.reason);
      if (r.status === 'busy') {
        if (opts.onBusy !== 'wait' || Date.now() >= waitUntil) throw new InProgressError(key, r.owner, r.retryAfterMs);
        await sleep(Math.min(delay, r.retryAfterMs + 5, Math.max(1, waitUntil - Date.now())));
        delay = Math.min(delay * 2, 500);
        continue;
      }
      if (r.status === 'in_doubt') {
        if (!reconcile || reconciled) throw new InDoubtError(key, r.token, await this.inspect(key));
        reconciled = true;
        const record = await this.inspect(key);
        const outcome = await reconcile({ key, idempotencyKey: this.idempotencyKey(key), token: r.token, record });
        if (!outcome) throw new InDoubtError(key, r.token, record);
        await this.resolve(key, r.token, outcome.status === 'done' ? { status: 'done', result: outcome.result } : outcome);
        continue;
      }
      return this.execute(r.claim, fn, opts, leaseMs);
    }
  }

  private async execute<T>(claim: Claim, fn: (ctx: EffectContext) => Promise<T> | T, opts: RunOnceOptions<T>, leaseMs: number): Promise<T> {
    const ac = new AbortController();
    await claim.begin(leaseMs);
    const every = opts.heartbeatMs ?? Math.floor(leaseMs / 3);
    let timer: ReturnType<typeof setInterval> | undefined;
    if (every > 0) {
      timer = setInterval(() => {
        claim.heartbeat(leaseMs).catch((err) => {
          if (err instanceof StaleClaimError) {
            clearInterval(timer);
            ac.abort(err);
          }
        });
      }, every);
      timer.unref?.();
    }
    let result: T;
    try {
      result = await fn({ key: claim.key, idempotencyKey: claim.idempotencyKey, token: claim.token, signal: ac.signal });
    } catch (err) {
      clearInterval(timer);
      const decision: ErrorDecision = opts.onError ? opts.onError(err) : 'release';
      try {
        if (decision === 'fail') await claim.fail(err);
        else if (decision === 'in_doubt') await claim.markInDoubt(err);
        else await claim.release(err);
      } catch (settleErr) {
        if (!(settleErr instanceof StaleClaimError)) throw settleErr;
      }
      throw err;
    }
    clearInterval(timer);
    await claim.complete(result);
    return result;
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

export function createClaimOnce(opts: ClaimOnceOptions): ClaimOnce {
  return new ClaimOnce(opts);
}
