#!/usr/bin/env node
import { parseArgs } from 'node:util';
import { formatTable, runLab, SCENARIOS, type Scenario } from './index.js';

const { values } = parseArgs({
  options: {
    redis: { type: 'string' },
    ops: { type: 'string' },
    runners: { type: 'string' },
    lease: { type: 'string' },
    scenario: { type: 'string', multiple: true },
    json: { type: 'boolean' },
    help: { type: 'boolean', short: 'h' },
  },
});

if (values.help) {
  console.log(`claim-once-lab: crash real worker processes and count side effects.

Usage: claim-once-lab [--redis redis://127.0.0.1:6379] [--ops 20] [--runners 8] [--lease 400] [--scenario NAME]... [--json]

Scenarios: ${SCENARIOS.join(', ')}
Uses keys under claim-once-lab:* and deletes them afterwards. Point it at a scratch Redis.`);
  process.exit(0);
}

const num = (v: string | undefined) => (v === undefined ? undefined : Number(v));
const scenarios = values.scenario as Scenario[] | undefined;
for (const s of scenarios ?? []) {
  if (!(SCENARIOS as readonly string[]).includes(s)) {
    console.error(`unknown scenario ${s}; expected one of ${SCENARIOS.join(', ')}`);
    process.exit(2);
  }
}

const started = Date.now();
runLab({ redisUrl: values.redis, ops: num(values.ops), runners: num(values.runners), leaseMs: num(values.lease), scenarios }).then(
  (rows) => {
    if (values.json) console.log(JSON.stringify(rows, null, 2));
    else {
      console.log(formatTable(rows));
      console.log(`\n${rows.reduce((n, r) => n + r.kills, 0)} worker processes were SIGKILLed. Took ${((Date.now() - started) / 1000).toFixed(1)} s.`);
    }
  },
  (err) => {
    console.error(err);
    process.exit(1);
  },
);
