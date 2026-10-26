import { Refused, WhitewaterError } from "./errors.js";
import type { Feed, FeedMessage } from "./feed.js";
import type { ReaderStart } from "./models.js";
import type { Subscription } from "./clients.js";

const POLL_MS = 25;
const END = Symbol("whitewater.consumer.end");

type QueueItem<K, V> = FeedMessage<K, V> | Error | typeof END;

/**
 * A permanent, supervised consumer bound to a Feed's named Reader —
 * the durable-Reader equivalent of Kafka's `while (true) poll()` with
 * the plumbing removed:
 *
 * ```ts
 * for await (const msg of feed.consumer("enricher")) {
 *   await process(msg);
 * }
 *
 * // or a handler, resolving only on close():
 * await feed.consumer("enricher").onRecord(process).run();
 * ```
 *
 * Semantics:
 *
 * - The named Reader's acked cursor is durable server-side — a restart
 *   (app or session) resumes where it stopped; no offset management.
 * - Each fetched page is delivered record-by-record; its delivered
 *   cursor acks only after every handler/yield completes — a thrown
 *   handler (or an abandoned iteration) leaves the page unacked so it
 *   redelivers (at-least-once). Poison pills are the app's job — route
 *   them to a DLQ feed.
 * - Transient failures retry with exponential backoff inside the
 *   supervisor; a fenced session re-opens and resumes. `run()`/the
 *   iterator throw only for startup refusals (misconfiguration, denied
 *   auth) or a silent subscription death — everything after a
 *   successful open is supervised forever until {@link close}.
 * - {@link close} finishes the in-flight page, releases the session,
 *   and lets `run()`/iteration return — the SIGTERM path.
 */
export class FeedConsumer<K, V> implements AsyncIterable<FeedMessage<K, V>> {
  private start: ReaderStart | undefined;
  private maxPageSizeValue: number | undefined;
  private initialBackoffMs = 250;
  private maxBackoffMs = 30_000;
  private onRecordHandler?: (message: FeedMessage<K, V>) => void | Promise<void>;
  private onErrorHandler: (error: unknown) => void = () => {};
  private stopped = false;
  private running = false;
  private inHandler = false;
  private current?: Subscription;
  private _lastError?: unknown;

  constructor(
    private readonly feed: Feed<K, V>,
    private readonly readerName: string,
  ) {}

  /** The declared Reader's name — its durable identity. */
  get reader(): string {
    return this.readerName;
  }

  /** Where the Reader starts on first declaration (default BEGINNING);
   * ignored once the Reader exists — it resumes from durable progress. */
  startAt(start: ReaderStart): this {
    this.start = start;
    return this;
  }

  /** Max records delivered per page (default 128). */
  maxPageSize(maxPageSize: number): this {
    this.maxPageSizeValue = maxPageSize;
    return this;
  }

  /** The per-record handler — required for {@link run}. A thrown error
   * leaves the page unacked so it redelivers; poison pills are the
   * app's job (route to a DLQ feed). */
  onRecord(handler: (message: FeedMessage<K, V>) => void | Promise<void>): this {
    this.onRecordHandler = handler;
    return this;
  }

  /** Error sink for visibility — plumbing failures and handler errors
   * alike. The loop continues regardless; stopping is only via
   * {@link close}. */
  onError(handler: (error: unknown) => void): this {
    this.onErrorHandler = handler;
    return this;
  }

  /** Restart backoff bounds in milliseconds (default 250 → 30_000). */
  backoff(initialMs: number, maxMs: number): this {
    this.initialBackoffMs = initialMs;
    this.maxBackoffMs = maxMs;
    return this;
  }

  /** The last supervised failure (plumbing or handler). */
  get lastError(): unknown {
    return this._lastError;
  }

  /** True while the supervise loop is active. */
  get isRunning(): boolean {
    return this.running;
  }

  /** Run the consumer until {@link close}. Throws only for startup
   * refusals or a subscription death with nothing to supervise —
   * Kafka-style fail-fast. */
  async run(
    handler?: (message: FeedMessage<K, V>) => void | Promise<void>,
  ): Promise<void> {
    const handle = handler ?? this.onRecordHandler;
    if (handle === undefined) {
      throw new Error("onRecord handler is required");
    }
    this.stopped = false;
    this.running = true;
    let backoffMs = this.initialBackoffMs;
    try {
      while (!this.stopped) {
        let iterError: unknown;
        const sub = this.subscribeDecoded(
          this.readerName,
          this.start,
          this.maxPageSizeValue,
          async (messages) => {
            for (const message of messages) {
              try {
                this.inHandler = true;
                await handle(message);
              } catch (error) {
                this._lastError = error;
                iterError = error;
                throw error;
              } finally {
                this.inHandler = false;
              }
            }
          },
        );
        this.current = sub;
        try {
          while (sub.running && !this.stopped) {
            await sleep(POLL_MS);
          }
        } finally {
          await sub.close();
          this.current = undefined;
        }
        if (this.stopped) break;
        const failure = sub.failure;
        if (failure instanceof Refused) {
          throw failure; // refused = fix the request — fail fast
        }
        const error = failure ?? iterError;
        if (error === undefined) {
          // Died with nothing to supervise — a local (validation/
          // programming) error swallowed by the loop. Restarting that
          // silently loops forever; surface it instead.
          throw new WhitewaterError(
            0,
            null,
            `subscription '${this.readerName}' terminated without ` +
              "an error to supervise",
          );
        }
        try {
          this.onErrorHandler(error);
        } catch {
          // Error sinks never break supervision.
        }
        await this.sleepOrStop(backoffMs);
        backoffMs = Math.min(backoffMs * 2, this.maxBackoffMs);
      }
    } finally {
      this.running = false;
      const current = this.current;
      this.current = undefined;
      if (current !== undefined) await current.close();
    }
  }

  /** The same supervised consumer detached — returns a promise that
   * resolves on close and rejects on startup refusal. */
  runAsync(
    handler?: (message: FeedMessage<K, V>) => void | Promise<void>,
  ): Promise<void> {
    return this.run(handler);
  }

  /** Graceful stop: the in-flight page finishes and acks, the session
   * releases, `run()`/iteration return. Safe inside a handler —
   * signal-only there since the supervise loop releases the session. */
  async close(): Promise<void> {
    this.stopped = true;
    const current = this.current;
    if (current !== undefined && !this.inHandler) {
      await current.close();
    }
  }

  /** Iterate decoded records forever — supervised like {@link run}.
   * Breaking out (or a loop-body throw) closes the consumer; a
   * startup {@link Refused} propagates. */
  async *[Symbol.asyncIterator](): AsyncGenerator<FeedMessage<K, V>> {
    const queue: QueueItem<K, V>[] = [];
    let notify: (() => void) | undefined;
    const push = (item: QueueItem<K, V>) => {
      queue.push(item);
      notify?.();
    };
    const supervised = this.run((message) => push(message));
    void supervised.then(
      () => push(END),
      (error: unknown) =>
        push(error instanceof Error ? error : new Error(String(error))),
    );
    try {
      for (;;) {
        while (queue.length === 0) {
          await new Promise<void>((resolve) => {
            notify = resolve;
          });
        }
        const item = queue.shift()!;
        if (item === END) return;
        if (item instanceof Error) throw item;
        yield item;
      }
    } finally {
      await this.close();
    }
  }

  private subscribeDecoded(
    reader: string,
    start: ReaderStart | undefined,
    capacity: number | undefined,
    onRecords: (messages: FeedMessage<K, V>[]) => Promise<void>,
  ): Subscription {
    return this.feed.subscribeDecoded(
      reader, start, capacity, onRecords, undefined);
  }

  private async sleepOrStop(ms: number): Promise<void> {
    const deadline = Date.now() + ms;
    while (!this.stopped && Date.now() < deadline) {
      await sleep(Math.min(POLL_MS, deadline - Date.now()));
    }
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
