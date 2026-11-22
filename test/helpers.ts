import { Redis } from 'ioredis';
import { randomBytes } from 'node:crypto';

export const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
export const clients: Redis[] = [];
export function redis(): Redis {
  const r = new Redis(REDIS_URL, { maxRetriesPerRequest: 2 });
  clients.push(r);
  return r;
}
export function ns(): string {
  return `claim-once-test:${randomBytes(4).toString('hex')}`;
}
export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
export async function closeAll(): Promise<void> {
  await Promise.all(clients.splice(0).map((c) => c.quit()));
}
