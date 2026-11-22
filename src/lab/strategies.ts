import type { Redis } from 'ioredis';
import { ClaimOnce } from '../claim-once.js';

export const STRATEGIES = ['none', 'check-then-set', 'mark-first', 'claim-once'] as const;
export type Strategy = (typeof STRATEGIES)[number];

export const STRATEGY_LABELS: Record<Strategy, string> = {
  none: 'no guard',
  'check-then-set': 'check, run, then set done',
  'mark-first': 'SET NX first, then run',
  'claim-once': 'claim-once',
};

export interface Hooks {
  /** After the key is claimed or checked, before the effect starts. */
  gap(): Promise<void>;
  /** Right before the effect call (after claim-once `begin`). */
  beforeEffect(): Promise<void>;
  /** After the effect landed, before it is recorded as done. */
  afterEffect(): Promise<void>;
}

export type Outcome = 'ran' | 'skipped';

/** One operation under one strategy. `effect` is the side effect (a charge, an email). */
export async function runStrategy(
  strategy: Strategy,
  redis: Redis,
  ns: string,
  key: string,
  effect: () => Promise<void>,
  hooks: Hooks,
  leaseMs: number,
): Promise<Outcome> {
  const k = `${ns}:${strategy}:${key}`;
  switch (strategy) {
    case 'none':
      await hooks.gap();
      await hooks.beforeEffect();
      await effect();
      await hooks.afterEffect();
      return 'ran';
    case 'check-then-set':
      if (await redis.get(k)) return 'skipped';
      await hooks.gap();
      await hooks.beforeEffect();
      await effect();
      await hooks.afterEffect();
      await redis.set(k, 'done');
      return 'ran';
    case 'mark-first':
      if ((await redis.set(k, 'taken', 'NX')) !== 'OK') return 'skipped';
      await hooks.gap();
      await hooks.beforeEffect();
      await effect();
      await hooks.afterEffect();
      return 'ran';
    case 'claim-once': {
      const once = new ClaimOnce({ redis, namespace: `${ns}:co`, leaseMs });
      const r = await once.reserve(key);
      if (r.status !== 'reserved') return 'skipped';
      await hooks.gap();
      try {
        await r.claim.begin();
      } catch {
        return 'skipped';
      }
      await hooks.beforeEffect();
      await effect();
      await hooks.afterEffect();
      await r.claim.complete({ ok: true }).catch(() => undefined);
      return 'ran';
    }
  }
}
