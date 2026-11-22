import type { ClaimRecord } from './types.js';

/** Base class for every error this package throws. */
export class ClaimOnceError extends Error {
  readonly key: string;
  constructor(message: string, key: string) {
    super(message);
    this.name = new.target.name;
    this.key = key;
  }
}

/** Another caller holds a live claim on this key. Retry later, or pass `onBusy: 'wait'`. */
export class InProgressError extends ClaimOnceError {
  readonly owner: string;
  readonly retryAfterMs: number;
  constructor(key: string, owner: string, retryAfterMs: number) {
    super(`claim-once: "${key}" is already claimed by ${owner || 'another caller'} (lease ends in ${retryAfterMs} ms)`, key);
    this.owner = owner;
    this.retryAfterMs = retryAfterMs;
  }
}

/**
 * The effect may or may not have happened: a claim began its effect and then lost its lease
 * (crash, stall, network split) without reporting a result. claim-once will not guess.
 * Check the downstream system, then call `resolve()`, or pass a `reconcile` hook.
 */
export class InDoubtError extends ClaimOnceError {
  readonly token: number;
  readonly record: ClaimRecord | null;
  constructor(key: string, token: number, record: ClaimRecord | null) {
    super(`claim-once: "${key}" is in_doubt (token ${token}). The effect may have run. Reconcile before retrying.`, key);
    this.token = token;
    this.record = record;
  }
}

/** The key was marked failed (by `fail()`, or after `maxAttempts` releases). */
export class FailedError extends ClaimOnceError {
  readonly reason: string;
  constructor(key: string, reason: string) {
    super(`claim-once: "${key}" is marked failed: ${reason}`, key);
    this.reason = reason;
  }
}

/** This claim's fencing token is no longer current, so the call was refused. */
export class StaleClaimError extends ClaimOnceError {
  readonly token: number;
  readonly state: string;
  constructor(key: string, token: number, state: string) {
    super(`claim-once: claim on "${key}" with token ${token} is no longer current (record is ${state})`, key);
    this.token = token;
    this.state = state;
  }
}
