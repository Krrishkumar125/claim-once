// Runs the built package on the oldest supported Node. Needs REDIS_URL.
import { createRequire } from 'node:module';
import assert from 'node:assert/strict';
import { Redis } from 'ioredis';
import { createClaimOnce, InDoubtError } from 'claim-once';
import { runLab } from 'claim-once/lab';

const require = createRequire(import.meta.url);
const cjs = require('claim-once');
assert.equal(typeof cjs.createClaimOnce, 'function');
assert.equal(typeof require('claim-once/lab').runLab, 'function');

const redis = new Redis(process.env.REDIS_URL ?? 'redis://127.0.0.1:6379');
const once = createClaimOnce({ redis, namespace: `smoke:${process.pid}`, leaseMs: 200 });
let runs = 0;
assert.equal(await once.runOnce('a', async () => ++runs), 1);
assert.equal(await once.runOnce('a', async () => ++runs), 1);
const r = await once.reserve('b');
await r.claim.begin();
await new Promise((res) => setTimeout(res, 250));
await assert.rejects(once.runOnce('b', async () => 1), InDoubtError);
await redis.quit();

const rows = await runLab({ redisUrl: process.env.REDIS_URL, ops: 3, runners: 3 });
for (const row of rows.filter((x) => x.strategy === 'claim-once')) {
  assert.equal(row.duplicates, 0, row.scenario);
  assert.equal(row.lost, 0, row.scenario);
}
console.log(`smoke ok on Node ${process.version}`);
