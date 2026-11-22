import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';

describe('README badges', () => {
  it('shows the npm version and CI status for the published name', () => {
    const readme = readFileSync(new URL('../README.md', import.meta.url), 'utf8');
    expect(readme).toContain('img.shields.io/npm/v/claim-once)');
    expect(readme).toContain('Krrishkumar125/claim-once/actions/workflows/ci.yml/badge.svg');
  });
});
