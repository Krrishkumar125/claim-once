import { Queue, QueueEvents, Worker, type Job } from 'bullmq';
import { afterAll, describe, expect, it } from 'vitest';
import { ClaimOnce, onceProcessor, onceTool } from '../src/index.js';
import { closeAll, ns, redis, REDIS_URL } from './helpers.js';

afterAll(closeAll);

describe('onceProcessor with a real BullMQ worker', () => {
  it('runs the effect once even when the same job is processed again', async () => {
    const connection = { url: REDIS_URL, maxRetriesPerRequest: null };
    const queueName = ns().replace(/:/g, '-');
    const once = new ClaimOnce({ redis: redis(), namespace: ns(), leaseMs: 2000 });
    const charges: string[] = [];
    const processor = onceProcessor(once, async (job: Job<{ orderId: string }>) => {
      charges.push(job.data.orderId);
      return { receipt: `r-${job.data.orderId}` };
    });
    const queue = new Queue(queueName, { connection });
    const events = new QueueEvents(queueName, { connection });
    const worker = new Worker(queueName, processor, { connection });
    try {
      await events.waitUntilReady();
      const job = await queue.add('charge', { orderId: 'A1' });
      const result = await job.waitUntilFinished(events, 10_000);
      expect(result).toEqual({ receipt: 'r-A1' });
      // A redelivery of the same job (for example after a stall) gets the stored result.
      const again = await processor((await queue.getJob(job.id!))!);
      expect(again).toEqual({ receipt: 'r-A1' });
      expect(charges).toEqual(['A1']);
    } finally {
      await worker.close();
      await events.close();
      await queue.obliterate({ force: true });
      await queue.close();
    }
  });
});

describe('onceTool', () => {
  it('a retried tool call with the same id does not run twice', async () => {
    const once = new ClaimOnce({ redis: redis(), namespace: ns() });
    let sent = 0;
    const sendEmail = onceTool(once, 'send_email', async (args: { to: string }) => { sent++; return `queued for ${args.to}`; });
    expect(await sendEmail({ to: 'a@example.com' }, { toolCallId: 'call_1' })).toBe('queued for a@example.com');
    expect(await sendEmail({ to: 'a@example.com' }, { toolCallId: 'call_1' })).toBe('queued for a@example.com');
    await sendEmail({ to: 'a@example.com' }, { toolCallId: 'call_2' });
    expect(sent).toBe(2);
  });
});
