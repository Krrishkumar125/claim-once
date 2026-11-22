/** The slice of an ioredis client this package uses. A Cluster works too (one key per script). */
export interface RedisLike {
  evalsha(sha: string, numKeys: number, ...args: (string | number)[]): Promise<unknown>;
  eval(script: string, numKeys: number, ...args: (string | number)[]): Promise<unknown>;
  hgetall(key: string): Promise<Record<string, string>>;
  pttl(key: string): Promise<number>;
  del(...keys: string[]): Promise<number>;
}

export type ClaimState = 'pending' | 'done' | 'failed' | 'in_doubt' | 'free';

export interface ClaimRecord {
  key: string;
  state: ClaimState;
  token: number;
  owner: string;
  attempts: number;
  /** True once `begin()` ran, meaning the effect may have started. */
  began: boolean;
  leaseExpiresAt: number | null;
  createdAt: number | null;
  updatedAt: number | null;
  /** Raw stored result (serialized), when state is done. */
  result?: string;
  error?: string;
  ttlMs: number;
}

/** What `onError` decides when the effect function throws. */
export type ErrorDecision = 'release' | 'fail' | 'in_doubt';

export type ReconcileOutcome<T> =
  | { status: 'done'; result: T }
  | { status: 'not_happened' }
  | { status: 'failed'; reason: string }
  | undefined;

export interface ReconcileInput {
  key: string;
  /** The idempotency key that was passed to the effect. Look it up downstream. */
  idempotencyKey: string;
  token: number;
  record: ClaimRecord | null;
}

export type Reconcile<T = unknown> = (input: ReconcileInput) => Promise<ReconcileOutcome<T>> | ReconcileOutcome<T>;

export interface ClaimOnceOptions {
  redis: RedisLike;
  /** Prefix for every Redis key. Default `claim-once`. */
  namespace?: string;
  /** How long a claim stays exclusive without a heartbeat. Default 30 000 ms. */
  leaseMs?: number;
  /** How long records (including done results) are kept. Default 7 days. */
  retentionMs?: number;
  /** Identifies this process in records. Default `<hostname>:<pid>:<random>`. */
  owner?: string;
  /** After this many attempts, a release turns into failed. 0 means no limit. Default 0. */
  maxAttempts?: number;
  serialize?: (value: unknown) => string;
  deserialize?: (raw: string) => unknown;
  /** Default reconcile hook for in_doubt keys, used by `runOnce` when the call does not pass one. */
  reconcile?: Reconcile;
}

export interface EffectContext {
  key: string;
  /**
   * Stable for the key across every attempt. Pass it to providers that accept an idempotency
   * key (Stripe `idempotencyKey`, an email provider's dedupe id) so a retry after in_doubt is safe downstream.
   */
  idempotencyKey: string;
  /** Fencing token of this claim. Strictly increases per key. Downstream systems can reject older tokens. */
  token: number;
  /** Aborted when the heartbeat finds the claim is no longer ours. */
  signal: AbortSignal;
}

export interface RunOnceOptions<T> {
  leaseMs?: number;
  /** Heartbeat interval. Default leaseMs / 3. Set 0 to disable. */
  heartbeatMs?: number;
  /** What to do when another caller holds a live claim. Default `throw` (InProgressError). */
  onBusy?: 'throw' | 'wait';
  /** Max time to wait when `onBusy` is `wait`. Default leaseMs * 2. */
  waitTimeoutMs?: number;
  /**
   * Decide what a thrown error means. Default `release` (the effect did not happen, allow a retry).
   * Return `in_doubt` for errors that can happen after the effect landed, such as a timeout after the request was sent.
   */
  onError?: (error: unknown) => ErrorDecision;
  reconcile?: Reconcile<T>;
}
