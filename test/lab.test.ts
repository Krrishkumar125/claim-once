import { describe, expect, it } from 'vitest';
import { runLab } from '../src/lab/index.js';
import { REDIS_URL } from './helpers.js';

describe('crash lab (real child processes, real SIGKILL, real Redis)', () => {
  it('claim-once never duplicates or silently loses an effect; the naive strategies do', async () => {
    const rows = await runLab({ redisUrl: REDIS_URL, ops: 6, runners: 4 });
    const row = (scenario: string, strategy: string) => rows.find((r) => r.scenario === scenario && r.strategy === strategy)!;
    for (const r of rows.filter((r) => r.strategy === 'claim-once')) {
      expect(r.duplicates, r.scenario).toBe(0);
      expect(r.lost, r.scenario).toBe(0);
    }
    expect(row('kill-after-effect', 'claim-once').inDoubt).toBe(6);
    expect(row('kill-after-effect', 'claim-once').effects).toBe(6);
    expect(row('kill-in-gap', 'claim-once').effects).toBe(6);
    expect(row('stall-past-lease', 'claim-once').effects).toBe(6);
    expect(row('concurrent', 'claim-once').effects).toBe(6);
    expect(row('kill-after-effect', 'check-then-set').duplicates).toBe(6);
    expect(row('kill-in-gap', 'mark-first').lost).toBe(6);
    expect(row('concurrent', 'none').duplicates).toBe(18);
    expect(rows.filter((r) => r.scenario.startsWith('kill')).every((r) => r.kills === 6)).toBe(true);
  }, 120_000);
});
