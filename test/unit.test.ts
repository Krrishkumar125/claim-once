import { describe, expect, it } from 'vitest';
import { ClaimOnce, InDoubtError, InProgressError, onceProcessor, onceTool, StaleClaimError } from '../src/index.js';
import type { RedisLike } from '../src/index.js';

const fake: RedisLike = {
  evalsha: async () => { throw new Error('not used'); },
  eval: async () => { throw new Error('not used'); },
  hgetall: async () => ({}),
  pttl: async () => -2,
  del: async () => 0,
};

describe('options and keys', () => {
  it('requires a redis client', () => {
    expect(() => new ClaimOnce({} as never)).toThrow(/redis/);
  });
  it('rejects bad lease and retention', () => {
    expect(() => new ClaimOnce({ redis: fake, leaseMs: 0 })).toThrow(RangeError);
    expect(() => new ClaimOnce({ redis: fake, leaseMs: 1000, retentionMs: 500 })).toThrow(RangeError);
  });
  it('builds hash tagged redis keys and a stable idempotency key', () => {
    const once = new ClaimOnce({ redis: fake, namespace: 'pay' });
    expect(once.redisKey('order-1')).toBe('pay:{order-1}');
    expect(once.idempotencyKey('order-1')).toBe('pay:order-1');
    expect(() => once.redisKey('')).toThrow(TypeError);
  });
  it('falls back to EVAL when the script is not cached', async () => {
    const calls: string[] = [];
    const redis: RedisLike = {
      ...fake,
      evalsha: async () => { calls.push('evalsha'); throw new Error('NOSCRIPT No matching script'); },
      eval: async () => { calls.push('eval'); return ['busy', '3', 'other', '100']; },
    };
    const r = await new ClaimOnce({ redis }).reserve('k');
    expect(r).toEqual({ status: 'busy', token: 3, owner: 'other', retryAfterMs: 100 });
    expect(calls).toEqual(['evalsha', 'eval']);
  });
});

describe('errors', () => {
  it('carry the key and details', () => {
    const e = new InProgressError('k', 'w1', 50);
    expect(e).toBeInstanceOf(Error);
    expect(e.name).toBe('InProgressError');
    expect(e.retryAfterMs).toBe(50);
    expect(new InDoubtError('k', 4, null).token).toBe(4);
    expect(new StaleClaimError('k', 2, 'done').state).toBe('done');
  });
});

describe('helper keys', () => {
  const recorder = () => {
    const keys: string[] = [];
    const once = { runOnce: async (key: string, fn: (ctx: never) => unknown) => { keys.push(key); return fn({} as never); } } as unknown as ClaimOnce;
    return { keys, once };
  };
  it('onceProcessor keys by queue and job id, or a custom key', async () => {
    const { keys, once } = recorder();
    await onceProcessor(once, async () => 1)({ id: '7', name: 'charge', queueName: 'billing' });
    await onceProcessor(once, async () => 1, { key: (j: { data: { orderId: string }; name: string; queueName: string }) => `order:${j.data.orderId}` })({ name: 'x', queueName: 'q', data: { orderId: 'A1' } });
    expect(keys).toEqual(['billing:7', 'order:A1']);
    expect(() => onceProcessor(once, async () => 1)({ name: 'x', queueName: 'q' })).toThrow(/no id/);
  });
  it('onceTool keys by tool name and call id', async () => {
    const { keys, once } = recorder();
    await onceTool(once, 'send_email', async () => 'ok')({ to: 'a' }, { toolCallId: 'call_1' });
    await onceTool(once, 'refund', async () => 'ok', { callId: (a: { id: string }) => a.id })({ id: 'r9' }, undefined);
    expect(keys).toEqual(['tool:send_email:call_1', 'tool:refund:r9']);
    expect(() => onceTool(once, 'x', async () => 1)({}, {})).toThrow(/no tool call id/);
  });
});

describe('labProblems', () => {
  const row = (strategy: 'claim-once' | 'none', duplicates: number, lost: number) => ({
    scenario: 'kill-in-gap' as const, strategy, ops: 5, effects: 5, duplicates, lost, inDoubt: 0, kills: 5,
  });
  it('flags only claim-once rows that duplicated or lost an effect', async () => {
    const { labProblems } = await import('../src/lab/index.js');
    expect(labProblems([row('none', 5, 0), row('claim-once', 0, 0)])).toEqual([]);
    expect(labProblems([row('claim-once', 1, 0)])).toEqual(['SIGKILL after the claim, before the effect: 1 duplicate(s), 0 lost']);
    expect(labProblems([row('claim-once', 0, 2)])).toHaveLength(1);
  });
});
