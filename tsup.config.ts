import { defineConfig } from 'tsup';

export default defineConfig([
  {
    entry: { index: 'src/index.ts', 'lab/index': 'src/lab/index.ts' },
    format: ['esm', 'cjs'],
    dts: true,
    sourcemap: true,
    shims: true,
    target: 'node18',
    external: ['ioredis', 'bullmq'],
  },
  {
    entry: { 'lab/worker': 'src/lab/worker.ts', 'lab/cli': 'src/lab/cli.ts' },
    format: ['esm'],
    sourcemap: true,
    target: 'node18',
    external: ['ioredis', 'bullmq'],
  },
]);
