import { afterAll, describe, expect, it } from 'vitest';
import { ClaimOnce, FailedError, InDoubtError, InProgressError, StaleClaimError } from '../src/index.js';
import { closeAll, ns, redis, sleep } from './helpers.js';

afterAll(closeAll);

const make = (opts: Partial<ConstructorParameters<typeof ClaimOnce>[0]> = {}) =>
  new ClaimOnce({ redis: redis(), namespace: ns(), leaseMs: 300, ...opts });

describe('runOnce', () => {
  it('runs the effect once and returns the stored result on repeat calls', async () => {
    const once = make();
    let runs = 0;
    const fn = async () => ({ charged: ++runs });
    expect(await once.runOnce('order-1', fn)).toEqual({ charged: 1 });
    expect(await once.runOnce('order-1', fn)).toEqual({ charged: 1 });
    expect(runs).toBe(1);
    const rec = await once.inspect('order-1');
    expect(rec?.state).toBe('done');
    expect(rec?.ttlMs).toBeGreaterThan(6 * 86_400_000);
  });

  it('keeps undefined results as undefined', async () => {
    const once = make();
    expect(await once.runOnce('u', async () => undefined)).toBeUndefined();
    expect(await once.runOnce('u', async () => 'never')).toBeUndefined();
  });

  it('lets exactly one of many concurrent callers on separate connections run the effect', async () => {
    const namespace = ns();
    const instances = Array.from({ length: 10 }, () => new ClaimOnce({ redis: redis(), namespace, leaseMs: 2000 }));
    let runs = 0;
    const results = await Promise.allSettled(
      Array.from({ length: 100 }, (_, i) =>
        instances[i % 10]!.runOnce('race', async () => {
          runs++;
          await sleep(50);
          return 'paid';
        }),
      ),
    );
    expect(runs).toBe(1);
    const ok = results.filter((r) => r.status === 'fulfilled');
    const busy = results.filter((r) => r.status === 'rejected' && r.reason instanceof InProgressError);
    expect(ok.length + busy.length).toBe(100);
    expect(ok.length).toBeGreaterThanOrEqual(1);
  });

  it('with onBusy wait, waiting callers get the winner result', async () => {
    const namespace = ns();
    let runs = 0;
    const results = await Promise.all(
      Array.from({ length: 20 }, () =>
        new ClaimOnce({ redis: redis(), namespace, leaseMs: 1000 }).runOnce('w', async () => { runs++; await sleep(100); return 42; }, { onBusy: 'wait' }),
      ),
    );
    expect(runs).toBe(1);
    expect(results.every((r) => r === 42)).toBe(true);
  });

  it('heartbeats keep a long effect exclusive past its first lease', async () => {
    const once = make({ leaseMs: 200 });
    const other = new ClaimOnce({ redis: redis(), namespace: once.namespace, leaseMs: 200 });
    const slow = once.runOnce('long', async () => { await sleep(700); return 'done'; });
    await sleep(450);
    await expect(other.runOnce('long', async () => 'second')).rejects.toBeInstanceOf(InProgressError);
    expect(await slow).toBe('done');
  });

  it('aborts the signal when the claim is lost', async () => {
    const once = make({ leaseMs: 150 });
    let aborted = false;
    await once.runOnce('lost', async (ctx) => {
      await once.forget('lost');
      await sleep(250);
      aborted = ctx.signal.aborted;
      return 1;
    }).catch((e) => expect(e).toBeInstanceOf(StaleClaimError));
    expect(aborted).toBe(true);
  });

  it('releases on error by default, so a retry runs', async () => {
    const once = make();
    let n = 0;
    await expect(once.runOnce('e', async () => { n++; throw new Error('card declined'); })).rejects.toThrow('card declined');
    expect((await once.inspect('e'))?.state).toBe('free');
    expect(await once.runOnce('e', async () => ++n)).toBe(2);
  });

  it('onError fail marks the key failed', async () => {
    const once = make();
    await expect(once.runOnce('f', async () => { throw new Error('bad request'); }, { onError: () => 'fail' })).rejects.toThrow();
    await expect(once.runOnce('f', async () => 1)).rejects.toBeInstanceOf(FailedError);
  });

  it('onError in_doubt parks the key', async () => {
    const once = make();
    await expect(once.runOnce('d', async () => { throw new Error('timeout'); }, { onError: () => 'in_doubt' })).rejects.toThrow('timeout');
    await expect(once.runOnce('d', async () => 1)).rejects.toBeInstanceOf(InDoubtError);
  });

  it('maxAttempts turns the last release into failed', async () => {
    const once = make({ maxAttempts: 2 });
    const boom = async () => { throw new Error('x'); };
    await expect(once.runOnce('m', boom)).rejects.toThrow('x');
    await expect(once.runOnce('m', boom)).rejects.toThrow('x');
    await expect(once.runOnce('m', boom)).rejects.toBeInstanceOf(FailedError);
  });
});

describe('leases, fencing and in_doubt', () => {
  it('takes over a claim whose lease ran out before the effect began, with a higher token', async () => {
    const once = make({ leaseMs: 100 });
    const a = await once.reserve('gap');
    expect(a.status).toBe('reserved');
    await sleep(150);
    const b = await once.reserve('gap');
    expect(b.status).toBe('reserved');
    if (a.status !== 'reserved' || b.status !== 'reserved') return;
    expect(b.takeover).toBe(true);
    expect(b.claim.token).toBeGreaterThan(a.claim.token);
    expect(b.claim.idempotencyKey).toBe(a.claim.idempotencyKey);
    await expect(a.claim.begin()).rejects.toBeInstanceOf(StaleClaimError);
    await b.claim.begin();
    await b.claim.complete('ok');
    await expect(a.claim.complete('late')).rejects.toBeInstanceOf(StaleClaimError);
    expect(await once.runOnce('gap', async () => 'again')).toBe('ok');
  });

  it('reports busy while the lease is live', async () => {
    const once = make({ leaseMs: 1000 });
    await once.reserve('b');
    const r = await once.reserve('b');
    expect(r.status).toBe('busy');
  });

  it('marks in_doubt when the lease runs out after the effect began', async () => {
    const once = make({ leaseMs: 100 });
    const a = await once.reserve('doubt');
    if (a.status !== 'reserved') throw new Error('expected reserved');
    await a.claim.begin();
    await sleep(150);
    const err = await once.runOnce('doubt', async () => 'dup').catch((e) => e);
    expect(err).toBeInstanceOf(InDoubtError);
    expect(err.token).toBe(a.claim.token);
    // The original owner was only slow, not dead: it can still finish with its token.
    await a.claim.complete('first');
    expect(await once.runOnce('doubt', async () => 'dup')).toBe('first');
  });

  it('reconcile can settle in_doubt as done without running the effect', async () => {
    const once = make({ leaseMs: 80 });
    const a = await once.reserve('r1');
    if (a.status !== 'reserved') throw new Error('expected reserved');
    await a.claim.begin();
    await sleep(120);
    const seen: string[] = [];
    const r = await once.runOnce('r1', async () => 'dup', {
      reconcile: ({ idempotencyKey }) => { seen.push(idempotencyKey); return { status: 'done', result: 'from provider' }; },
    });
    expect(r).toBe('from provider');
    expect(seen).toEqual([`${once.namespace}:r1`]);
  });

  it('reconcile not_happened frees the key and the effect runs', async () => {
    const once = make({ leaseMs: 80 });
    const a = await once.reserve('r2');
    if (a.status !== 'reserved') throw new Error('expected reserved');
    await a.claim.begin();
    await sleep(120);
    expect(await once.runOnce('r2', async () => 'ran', { reconcile: () => ({ status: 'not_happened' }) })).toBe('ran');
  });

  it('reconcile returning undefined leaves it in_doubt', async () => {
    const once = make({ leaseMs: 80 });
    const a = await once.reserve('r3');
    if (a.status !== 'reserved') throw new Error('expected reserved');
    await a.claim.begin();
    await sleep(120);
    await expect(once.runOnce('r3', async () => 1, { reconcile: () => undefined })).rejects.toBeInstanceOf(InDoubtError);
  });

  it('resolve refuses a wrong token, so two reconcilers cannot both settle', async () => {
    const once = make({ leaseMs: 80 });
    const a = await once.reserve('r4');
    if (a.status !== 'reserved') throw new Error('expected reserved');
    await a.claim.begin();
    await sleep(120);
    await once.reserve('r4');
    await expect(once.resolve('r4', a.claim.token + 1, { status: 'not_happened' })).rejects.toBeInstanceOf(StaleClaimError);
    await once.resolve('r4', a.claim.token, { status: 'failed', reason: 'refunded by hand' });
    await expect(once.runOnce('r4', async () => 1)).rejects.toThrow(/refunded by hand/);
  });

  it('passes a stable idempotency key and increasing tokens to the effect', async () => {
    const once = make();
    const seen: { idem: string; token: number }[] = [];
    await once.runOnce('s', async (ctx) => { seen.push({ idem: ctx.idempotencyKey, token: ctx.token }); throw new Error('retry me'); }).catch(() => undefined);
    await once.runOnce('s', async (ctx) => { seen.push({ idem: ctx.idempotencyKey, token: ctx.token }); });
    expect(seen[0]!.idem).toBe(seen[1]!.idem);
    expect(seen[1]!.token).toBeGreaterThan(seen[0]!.token);
  });
});
