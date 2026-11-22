import type { ClaimOnce } from './claim-once.js';
import type { EffectContext, RunOnceOptions } from './types.js';

/** The parts of a BullMQ Job the helper reads. A real `Job` fits. */
export interface JobLike {
  id?: string;
  name: string;
  queueName: string;
}

export interface OnceProcessorOptions<J extends JobLike, R> extends RunOnceOptions<R> {
  /** Operation key for a job. Default `<queueName>:<job.id>`. Use a business key (order id) when the same work can arrive as different jobs. */
  key?: (job: J) => string;
}

/**
 * Wrap a BullMQ processor so its side effect runs at most once per job, even when BullMQ
 * hands a stalled job to a second worker. A redelivered job that already finished returns the stored result.
 */
export function onceProcessor<J extends JobLike, R>(
  once: ClaimOnce,
  processor: (job: J, ctx: EffectContext, token?: string) => Promise<R> | R,
  opts: OnceProcessorOptions<J, R> = {},
): (job: J, token?: string) => Promise<R> {
  const { key: keyOf, ...runOpts } = opts;
  return (job, token) => {
    const key = keyOf ? keyOf(job) : defaultJobKey(job);
    return once.runOnce(key, (ctx) => processor(job, ctx, token), runOpts);
  };
}

function defaultJobKey(job: JobLike): string {
  if (!job.id) throw new TypeError('claim-once: job has no id; pass `key` to onceProcessor');
  return `${job.queueName}:${job.id}`;
}

export interface OnceToolOptions<A, R> extends RunOnceOptions<R> {
  /** Read the tool call id. Default: `options.toolCallId` from the second argument (the AI SDK shape). */
  callId?: (args: A, options: unknown) => string | undefined;
}

/**
 * Wrap a tool's execute function so a retried tool call (same tool name and call id)
 * returns the first result instead of running the effect again.
 */
export function onceTool<A, R, O = unknown>(
  once: ClaimOnce,
  toolName: string,
  execute: (args: A, options: O, ctx: EffectContext) => Promise<R> | R,
  opts: OnceToolOptions<A, R> = {},
): (args: A, options: O) => Promise<R> {
  const { callId, ...runOpts } = opts;
  return (args, options) => {
    const id = callId ? callId(args, options) : (options as { toolCallId?: unknown } | undefined)?.toolCallId;
    if (typeof id !== 'string' || id.length === 0) {
      throw new TypeError(`claim-once: no tool call id for "${toolName}"; pass \`callId\` to onceTool`);
    }
    return once.runOnce(`tool:${toolName}:${id}`, (ctx) => execute(args, options, ctx), runOpts);
  };
}
