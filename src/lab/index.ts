import { spawn } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { existsSync, mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { Redis } from 'ioredis';
import { ClaimOnce } from '../claim-once.js';
import { STRATEGIES, STRATEGY_LABELS, type Strategy } from './strategies.js';
import type { WorkerConfig } from './worker.js';

export { STRATEGIES, STRATEGY_LABELS };
export type { Strategy };

export const SCENARIOS = ['kill-after-effect', 'kill-in-gap', 'stall-past-lease', 'concurrent'] as const;
export type Scenario = (typeof SCENARIOS)[number];

export const SCENARIO_LABELS: Record<Scenario, string> = {
  'kill-after-effect': 'SIGKILL after the effect, before it is recorded',
  'kill-in-gap': 'SIGKILL after the claim, before the effect',
  'stall-past-lease': 'event loop blocked past the lease, job redelivered',
  concurrent: 'concurrent runners on the same keys',
};

export interface LabOptions {
  redisUrl?: string;
  /** Operations (keys) per scenario. Default 20. */
  ops?: number;
  /** Processes in the concurrent scenario. Default 8. */
  runners?: number;
  /** Lease used by claim-once in the lab. Default 400 ms. */
  leaseMs?: number;
  scenarios?: readonly Scenario[];
  strategies?: readonly Strategy[];
}

export interface LabRow {
  scenario: Scenario;
  strategy: Strategy;
  ops: number;
  /** Times the effect actually ran. */
  effects: number;
  /** Extra runs beyond the first, summed over keys. */
  duplicates: number;
  /** Keys whose effect never ran and that nothing flags. */
  lost: number;
  /** Keys claim-once left as in_doubt for reconciliation. */
  inDoubt: number;
  /** Real SIGKILLs sent to worker processes. */
  kills: number;
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

function workerCommand(): { file: string; execArgv: string[] } {
  const here = dirname(fileURLToPath(import.meta.url));
  const js = join(here, 'worker.js');
  if (existsSync(js)) return { file: js, execArgv: [] };
  return { file: join(here, 'worker.ts'), execArgv: ['--import', 'tsx'] };
}

interface Spawned {
  done: Promise<{ code: number | null; signal: NodeJS.Signals | null }>;
  onMessage(type: string, cb: () => void): void;
}

function spawnWorker(cfg: WorkerConfig, counter: { kills: number }): Spawned {
  const { file, execArgv } = workerCommand();
  const child = spawn(process.execPath, [...execArgv, file, JSON.stringify(cfg)], { stdio: ['ignore', 'inherit', 'inherit', 'ipc'] });
  const listeners = new Map<string, () => void>();
  child.on('message', (m: { type?: string }) => {
    if (m?.type === 'crash') {
      counter.kills++;
      child.kill('SIGKILL');
    }
    const cb = m?.type ? listeners.get(m.type) : undefined;
    cb?.();
  });
  const done = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve, reject) => {
    child.on('error', reject);
    child.on('exit', (code, signal) => {
      if (code !== 0 && signal !== 'SIGKILL') reject(new Error(`lab worker exited with code ${code} signal ${signal}`));
      else resolve({ code, signal });
    });
  });
  return { done, onMessage: (type, cb) => listeners.set(type, cb) };
}

async function runCase(redis: Redis, redisUrl: string, scenario: Scenario, strategy: Strategy, o: Required<Omit<LabOptions, 'scenarios' | 'strategies' | 'redisUrl'>>): Promise<LabRow> {
  const id = randomBytes(4).toString('hex');
  const ns = `claim-once-lab:${id}`;
  const dir = mkdtempSync(join(tmpdir(), 'claim-once-lab-'));
  const effectsFile = join(dir, 'effects.log');
  const keys = Array.from({ length: o.ops }, (_, i) => `op-${i}`);
  const base = { redisUrl, strategy, ns, effectsFile, leaseMs: o.leaseMs };
  const counter = { kills: 0 };
  const redeliver = () => spawnWorker({ ...base, keys }, counter).done;

  try {
    if (scenario === 'kill-after-effect' || scenario === 'kill-in-gap') {
      const crash = scenario === 'kill-after-effect' ? 'after-effect' : 'gap';
      await Promise.all(keys.map((key) => spawnWorker({ ...base, keys: [key], crash }, counter).done));
      await sleep(o.leaseMs + 100); // the lease runs out, then the queue redelivers every job
      await redeliver();
    } else if (scenario === 'stall-past-lease') {
      const first = spawnWorker({ ...base, keys, stallMs: o.leaseMs * 4 }, counter);
      const stalling = new Promise<void>((r) => first.onMessage('stalling', r));
      await Promise.race([stalling, first.done]);
      await sleep(o.leaseMs + 100);
      await redeliver(); // a second worker picks up the "stalled" jobs while the first is still blocked
      await first.done;
    } else {
      const startAt = Date.now() + 1500;
      await Promise.all(Array.from({ length: o.runners }, () => spawnWorker({ ...base, keys, startAt, effectDelayMs: 20 }, counter).done));
    }

    const counts = new Map<string, number>(keys.map((k) => [k, 0]));
    const log = existsSync(effectsFile) ? readFileSync(effectsFile, 'utf8') : '';
    for (const line of log.split('\n')) {
      const key = line.split('\t')[0];
      if (key && counts.has(key)) counts.set(key, (counts.get(key) ?? 0) + 1);
    }
    const doubt = new Set<string>();
    if (strategy === 'claim-once') {
      const once = new ClaimOnce({ redis, namespace: `${ns}:co`, leaseMs: o.leaseMs });
      for (const key of keys) {
        const r = await once.reserve(key);
        if (r.status === 'in_doubt') doubt.add(key);
        if (r.status === 'reserved') await r.claim.release(); // do not leave a fresh claim behind
      }
    }
    let effects = 0;
    let duplicates = 0;
    let lost = 0;
    for (const [key, n] of counts) {
      effects += n;
      duplicates += Math.max(0, n - 1);
      if (n === 0 && !doubt.has(key)) lost++;
    }
    return { scenario, strategy, ops: o.ops, effects, duplicates, lost, inDoubt: doubt.size, kills: counter.kills };
  } finally {
    rmSync(dir, { recursive: true, force: true });
    const stream = redis.scanStream({ match: `${ns}:*`, count: 500 });
    for await (const batch of stream as AsyncIterable<string[]>) if (batch.length) await redis.del(...batch);
  }
}

/** Run every scenario against every strategy with real child processes and real Redis. */
export async function runLab(opts: LabOptions = {}): Promise<LabRow[]> {
  const redisUrl = opts.redisUrl ?? process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
  const o = { ops: opts.ops ?? 20, runners: opts.runners ?? 8, leaseMs: opts.leaseMs ?? 400 };
  const redis = new Redis(redisUrl, { maxRetriesPerRequest: 2 });
  const rows: LabRow[] = [];
  try {
    for (const scenario of opts.scenarios ?? SCENARIOS) {
      for (const strategy of opts.strategies ?? STRATEGIES) rows.push(await runCase(redis, redisUrl, scenario, strategy, o));
    }
  } finally {
    await redis.quit();
  }
  return rows;
}

/** Render rows as a Markdown table. */
export function formatTable(rows: LabRow[]): string {
  const head = '| Scenario | Strategy | Ops | Effects run | Duplicates | Lost | in_doubt |';
  const sep = '|---|---|---:|---:|---:|---:|---:|';
  const body = rows.map((r) => `| ${SCENARIO_LABELS[r.scenario]} | ${STRATEGY_LABELS[r.strategy]} | ${r.ops} | ${r.effects} | ${r.duplicates} | ${r.lost} | ${r.inDoubt} |`);
  return [head, sep, ...body].join('\n');
}
