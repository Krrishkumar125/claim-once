// One lab worker process. The parent SIGKILLs it at a crash point, or lets it finish.
import { appendFileSync } from 'node:fs';
import { Redis } from 'ioredis';
import { runStrategy, type Strategy } from './strategies.js';

export interface WorkerConfig {
  redisUrl: string;
  strategy: Strategy;
  ns: string;
  keys: string[];
  effectsFile: string;
  leaseMs: number;
  /** Where to ask the parent for a SIGKILL. */
  crash?: 'gap' | 'after-effect';
  /** Block the event loop this long right before the effect, after telling the parent. */
  stallMs?: number;
  /** Simulated network time of the effect. */
  effectDelayMs?: number;
  /** Wall clock time (ms) to start at, so concurrent runners collide. */
  startAt?: number;
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

function send(msg: unknown): Promise<void> {
  return new Promise((resolve) => {
    if (!process.send) return resolve();
    process.send(msg, () => resolve());
  });
}

async function main(): Promise<void> {
  const cfg = JSON.parse(process.argv[2] ?? '{}') as WorkerConfig;
  const redis = new Redis(cfg.redisUrl, { maxRetriesPerRequest: 2 });
  const crashAt = async (point: string) => {
    if (cfg.crash !== point) return;
    await send({ type: 'crash' });
    await new Promise(() => undefined); // wait here for the SIGKILL
  };
  const effect = (key: string) => async () => {
    if (cfg.effectDelayMs) await sleep(cfg.effectDelayMs);
    appendFileSync(cfg.effectsFile, `${key}\t${process.pid}\n`);
  };
  if (cfg.startAt) await sleep(Math.max(0, cfg.startAt - Date.now()));

  if (cfg.stallMs) {
    // Claim every key, then block the event loop past the lease, then run the effects.
    const gates: (() => void)[] = [];
    const ready: Promise<void>[] = [];
    const runs = cfg.keys.map((key) => {
      let arrived!: () => void;
      ready.push(new Promise<void>((r) => (arrived = r)));
      return runStrategy(cfg.strategy, redis, cfg.ns, key, effect(key), {
        gap: async () => undefined,
        beforeEffect: () => new Promise<void>((go) => { gates.push(go); arrived(); }),
        afterEffect: async () => undefined,
      }, cfg.leaseMs).finally(() => arrived());
    });
    await Promise.all(ready);
    if (gates.length) {
      await send({ type: 'stalling' });
      const until = Date.now() + cfg.stallMs;
      while (Date.now() < until) { /* busy: no timers, no heartbeats */ }
      for (const go of gates) go();
    }
    await Promise.all(runs);
  } else {
    await Promise.all(cfg.keys.map((key) => runStrategy(cfg.strategy, redis, cfg.ns, key, effect(key), {
      gap: () => crashAt('gap'),
      beforeEffect: async () => undefined,
      afterEffect: () => crashAt('after-effect'),
    }, cfg.leaseMs)));
  }
  await redis.quit();
}

main().then(
  () => process.exit(0),
  (err) => {
    console.error(err);
    process.exit(1);
  },
);
