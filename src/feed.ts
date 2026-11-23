import { randomUUID } from "node:crypto";
import { Codec } from "./codecs.js";
import {
  AmbiguousResult,
  StaleSession,
  Unavailable,
  WhitewaterError,
} from "./errors.js";
import type {
  AppendBatchResult,
  AppendResult,
  Cursor,
  FeedRecord,
  ReaderPage,
  ReaderSession,
  TemporaryFetchOptions,
} from "./models.js";
import { ReaderStart } from "./models.js";
import { FeedConsumer } from "./consumer.js";
import type {
  ReaderSessionClient,
  Subscription,
  WhitewaterClient,
  WriterSessionClient,
} from "./clients.js";

/**
 * One committed Feed record with its key and payload already decoded
 * through the bound codecs — the {@link FeedRecord} fields carry
 * through; only the payloads are typed. Times are signed 64-bit Unix
 * epoch nanoseconds; `cursor` is opaque — pass it back verbatim.
 */
export interface FeedMessage<K, V> {
  cursor: Cursor;
  messageId: string;
  producerId: string;
  sequence: number;
  eventTimeNs: bigint;
  ingestTimeNs: bigint;
  /** Decoded key, or null when the record carries no key. */
  key: K | null;
  value: V;
  metadata: Record<string, Buffer>;
}

/** One decoded temporary-fetch page: messages plus the opaque
 * continuation frontier. */
export interface FeedPage<K, V> {
  records: FeedMessage<K, V>[];
  nextCursor: Cursor | null;
}

/** One bounded page of committed records, decoded. Acknowledge
 * `deliveredCursor` — not the last record's cursor — to durably commit
 * progress. */
export interface FeedReaderPage<K, V> {
  records: FeedMessage<K, V>[];
  deliveredCursor: Cursor | null;
  acknowledgedCursor: Cursor | null;
  recommendedCapacity: number;
  retryAfterMs: number;
}

/** A typed record to append: request identity, key, payload, optional
 * event time and Metadata. The bound codecs serialize `key`/`value` at
 * write time — application code never touches bytes. */
export interface FeedEvent<K, V> {
  /** Empty generates a fresh identity; pin it for caller-driven retries
   * (identical retries dedupe server-side). */
  requestId?: string;
  /** Omit or null appends a keyless record. */
  key?: K | null;
  value: V;
  eventTimeNs?: bigint | null;
  metadata?: Record<string, Buffer>;
}

function decodeMessage<K, V>(
  record: FeedRecord,
  keyCodec: Codec<K>,
  valueCodec: Codec<V>,
): FeedMessage<K, V> {
  return {
    cursor: record.cursor,
    messageId: record.messageId,
    producerId: record.producerId,
    sequence: record.sequence,
    eventTimeNs: record.eventTimeNs,
    ingestTimeNs: record.ingestTimeNs,
    key: record.key === null ? null : keyCodec.decode(record.key),
    value: valueCodec.decode(record.payload),
    metadata: record.metadata,
  };
}

function decodeMessages<K, V>(
  records: FeedRecord[],
  keyCodec: Codec<K>,
  valueCodec: Codec<V>,
): FeedMessage<K, V>[] {
  return records.map((r) => decodeMessage(r, keyCodec, valueCodec));
}

/**
 * A session-bound Writer with the feed's codecs applied — the session
 * epoch and request identities stay inside the handle:
 *
 * ```ts
 * const writer = await orders.writer("ingest");
 * await writer.append("order-1", { id: "order-1", total: 42 });
 * await writer.appendEvent({ requestId: rid, key: "k", value: order });
 * ```
 */
/** Per-write delivery callback: `(result, error)` once the commit
 * lands — mirrors the Java `write(..., BiConsumer)` form. */
export type WriteCallback<V> = (
  result: AppendResult | null,
  error: unknown,
) => void;

interface PendingWrite {
  input: {
    requestId?: string;
    key: Buffer | null;
    payload: Buffer;
    eventTimeNs: bigint | null;
    metadata?: Record<string, Buffer>;
  };
  resolve: (result: AppendResult) => void;
  reject: (error: unknown) => void;
  callback?: WriteCallback<unknown>;
}

const DEFAULT_LINGER_MS = 5;
const DEFAULT_MAX_BATCH_RECORDS = 256;
const WRITE_MAX_ATTEMPTS = 3;
const WRITE_RETRY_BACKOFF_MS = 100;

export class FeedWriter<K, V> {
  private writeQueue: PendingWrite[] = [];
  private flushTimer: NodeJS.Timeout | null = null;
  private flushPromise: Promise<void> | null = null;
  private closed = false;

  constructor(
    private session: WriterSessionClient,
    private readonly keyCodec: Codec<K>,
    private readonly valueCodec: Codec<V>,
    /** Rebinds a fresh session epoch after StaleSession; stable request
     * identities make resubmits dedupe server-side. */
    private readonly reopen?: () => Promise<WriterSessionClient>,
    private lingerMs: number = DEFAULT_LINGER_MS,
    private maxBatch: number = DEFAULT_MAX_BATCH_RECORDS,
  ) {}

  /** The session's fencing epoch. */
  get sessionEpoch(): number {
    return this.session.sessionEpoch;
  }

  /** Enable wire compression on subsequent appends. */
  useCompression(enabled = true): FeedWriter<K, V> {
    return new FeedWriter<K, V>(
      this.session.useCompression(enabled),
      this.keyCodec,
      this.valueCodec,
      this.reopen,
      this.lingerMs,
      this.maxBatch,
    );
  }

  /** Coalescing window in ms before the first `write` — mirrors the
   * Java `.linger(Duration)` builder knob (default 5ms). */
  linger(ms: number): FeedWriter<K, V> {
    return new FeedWriter<K, V>(
      this.session, this.keyCodec, this.valueCodec,
      this.reopen, ms, this.maxBatch,
    );
  }

  /** Cap one coalesced commit (default 256). */
  maxBatchRecords(n: number): FeedWriter<K, V> {
    return new FeedWriter<K, V>(
      this.session, this.keyCodec, this.valueCodec,
      this.reopen, this.lingerMs, n,
    );
  }

  /** Encode one record through the bound codecs; request identity and
   * event time auto-generate. For a keyless record or pinned identity
   * use {@link appendEvent}. */
  append(key: K, value: V): Promise<AppendResult> {
    return this.appendEvent({ key, value });
  }

  /** Append one typed event. */
  appendEvent(event: FeedEvent<K, V>): Promise<AppendResult> {
    return this.session.append(this.wire(event));
  }

  /** One epoch-fenced batch; per-record results plus server batching
   * guidance. */
  appendAll(events: FeedEvent<K, V>[]): Promise<AppendBatchResult> {
    return this.session.appendBatch(events.map((e) => this.wire(e)));
  }

  private wire(event: FeedEvent<K, V>) {
    return {
      requestId: event.requestId,
      key: event.key == null ? null : this.keyCodec.encode(event.key),
      payload: this.valueCodec.encode(event.value),
      eventTimeNs: event.eventTimeNs ?? null,
      metadata: event.metadata,
    };
  }

  /**
   * The always-async write path: queues the record and returns a
   * Promise resolving once the coalesced batch majority-commits —
   * the `AppendResult` ack. `callback(result, error)` receives the
   * same outcome. `append`/`appendAll` remain the manual forms.
   */
  write(key: K | null, value: V, callback?: WriteCallback<V>): Promise<AppendResult> {
    return this.writeEvent({ key, value }, callback);
  }

  /** Queue one typed event for the coalescing sender — pin
   * `requestId` for caller-driven retries (identical retries dedupe
   * server-side). */
  writeEvent(event: FeedEvent<K, V>, callback?: WriteCallback<V>): Promise<AppendResult> {
    return new Promise<AppendResult>((resolve, reject) => {
      if (this.closed) {
        const error = new WhitewaterError(0, null, "whitewater: writer closed");
        callback?.(null, error);
        reject(error);
        return;
      }
      // Pin the stable request identity at enqueue: resubmits after a
      // StaleSession reopen carry the same id.
      const input = this.wire({ ...event, requestId: event.requestId ?? randomUUID() });
      this.writeQueue.push({ input, resolve, reject, callback: callback as WriteCallback<unknown> | undefined });
      this.schedule();
    });
  }

  /** Queued-not-yet-delivered writes — diagnostics only. */
  get pending(): number {
    return this.writeQueue.length;
  }

  /** Flush queued writes, deliver them, then stop the sender —
   * writes queued before close still complete; later writes fail. */
  async close(): Promise<void> {
    this.closed = true;
    if (this.flushTimer) {
      clearTimeout(this.flushTimer);
      this.flushTimer = null;
    }
    while (this.writeQueue.length > 0 || this.flushPromise) {
      if (this.flushPromise) await this.flushPromise;
      if (this.writeQueue.length > 0 && !this.flushPromise) {
        await this.flush();
      }
    }
  }

  private schedule(): void {
    if (this.flushPromise || this.flushTimer || this.closed) return;
    if (this.writeQueue.length >= this.maxBatch) {
      this.flushPromise = this.flush().finally(() => {
        this.flushPromise = null;
        if (this.writeQueue.length > 0) this.schedule();
      });
      return;
    }
    this.flushTimer = setTimeout(() => {
      this.flushTimer = null;
      this.flushPromise = this.flush().finally(() => {
        this.flushPromise = null;
        if (this.writeQueue.length > 0) this.schedule();
      });
    }, this.lingerMs);
  }

  /** Post one append-batch commit with the Java parity retry
   * contract: StaleSession rebinds a fresh epoch and resubmits,
   * Unavailable/AmbiguousResult retry bounded-ly, a definitive
   * refusal fails the batch whole. */
  private async flush(): Promise<void> {
    const batch = this.writeQueue.splice(0, this.maxBatch);
    if (batch.length === 0) return;
    const inputs = batch.map((p) => p.input);
    let lastError: unknown = null;
    for (let attempt = 0; attempt < WRITE_MAX_ATTEMPTS; attempt++) {
      try {
        const outcome = await this.session.appendBatch(inputs);
        if (outcome.results.length !== batch.length) {
          lastError = new WhitewaterError(
            0, null,
            "whitewater: batch result did not cover every record",
          );
          break;
        }
        batch.forEach((p, i) => {
          p.resolve(outcome.results[i]);
          p.callback?.(outcome.results[i], null);
        });
        return;
      } catch (error) {
        lastError = error;
        if (error instanceof StaleSession) {
          try {
            if (!this.reopen) throw error;
            this.session = await this.reopen();
          } catch (reopenError) {
            lastError = reopenError;
            if (attempt + 1 < WRITE_MAX_ATTEMPTS) {
              await sleep(WRITE_RETRY_BACKOFF_MS);
            }
            continue;
          }
          continue;
        }
        if (error instanceof Unavailable || error instanceof AmbiguousResult) {
          if (attempt + 1 < WRITE_MAX_ATTEMPTS) await sleep(WRITE_RETRY_BACKOFF_MS);
          continue;
        }
        break;
      }
    }
    for (const p of batch) {
      p.reject(lastError);
      p.callback?.(null, lastError);
    }
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * A session-bound named Reader with the feed's codecs applied —
 * {@link fetch} returns a bounded page of decoded messages; {@link ack}
 * commits the page's delivered Cursor durably:
 *
 * ```ts
 * const reader = await orders.reader("audit", 64);
 * const page = await reader.fetch();
 * for (const m of page.records) handle(m.value);
 * await reader.ack();
 * await reader.close();
 * ```
 */
export class FeedReader<K, V> {
  private lastPage: FeedReaderPage<K, V> | null = null;

  constructor(
    private readonly client: ReaderSessionClient,
    private session: ReaderSession,
    private readonly keyCodec: Codec<K>,
    private readonly valueCodec: Codec<V>,
  ) {}

  /** The session's fencing epoch. */
  get sessionEpoch(): number {
    return this.session.sessionEpoch;
  }

  /** Up to `limit` committed records after the frontier, decoded. */
  async fetch(limit?: number): Promise<FeedReaderPage<K, V>> {
    const page = await this.client.fetch(this.session, limit);
    const decoded: FeedReaderPage<K, V> = {
      records: decodeMessages(page.records, this.keyCodec, this.valueCodec),
      deliveredCursor: page.deliveredCursor,
      acknowledgedCursor: page.acknowledgedCursor,
      recommendedCapacity: page.recommendedCapacity,
      retryAfterMs: page.retryAfterMs,
    };
    this.lastPage = decoded;
    return decoded;
  }

  /** Durably commit the last fetched page's delivered Cursor. Call
   * after processing a page; anything unacknowledged may replay. */
  async ack(): Promise<ReaderSession> {
    const cursor = this.lastPage?.deliveredCursor;
    if (cursor == null) return this.session;
    return this.ackCursor(cursor);
  }

  /** Durably commit an explicit Cursor. */
  async ackCursor(cursor: Cursor): Promise<ReaderSession> {
    this.session = await this.client.ack(this.session, cursor);
    return this.session;
  }

  /**
   * Deliver every record to `handler` — one at a time — acking each
   * fetched page after its records are handled. The pump paces empty
   * fetches on the server's `retry_after_ms` and exits when the reader
   * is closed. `handler` returning `false` — or rejecting — leaves the
   * in-flight page unacked so its tail replays: at-least-once delivery,
   * never acknowledge undelivered records.
   */
  async forEach(
    handler: (message: FeedMessage<K, V>) => boolean | void | Promise<boolean | void>,
  ): Promise<void> {
    while (this.session.sessionEpoch !== 0) {
      const page = await this.fetch();
      if (page.records.length === 0) {
        await new Promise((resolve) =>
          setTimeout(resolve, Math.max(1, page.retryAfterMs)),
        );
        continue;
      }
      for (const message of page.records) {
        if ((await handler(message)) === false) return;
      }
      if (this.session.sessionEpoch === 0) return;
      await this.ack();
    }
  }

  /** `for await (const m of reader)` — iterates records one at a time,
   * acking each page once consumed. `break` leaves the in-flight page
   * unacked so its tail replays. */
  async *[Symbol.asyncIterator](): AsyncGenerator<FeedMessage<K, V>> {
    while (this.session.sessionEpoch !== 0) {
      const page = await this.fetch();
      if (page.records.length === 0) {
        await new Promise((resolve) =>
          setTimeout(resolve, Math.max(1, page.retryAfterMs)),
        );
        continue;
      }
      yield* page.records;
      if (this.session.sessionEpoch === 0) return;
      await this.ack();
    }
  }

  /** Release the session fence; durable acknowledged progress survives. */
  async close(): Promise<void> {
    if (this.session.sessionEpoch === 0) return;
    const session = this.session;
    this.session = { ...session, sessionEpoch: 0 };
    await this.client.close(session);
  }
}

/**
 * A typed facade over one Feed — the immutable temporal history on the
 * Nodes. The bound codecs serialize on write and deserialize on read,
 * so application code stays in its own types:
 *
 * ```ts
 * const orders = client.feed("orders.events", codecs.json<Order>());
 * const writer = await orders.writer("ingest");
 * await writer.append("order-1", { id: "order-1", total: 42 });
 * const reader = await orders.reader("audit", 64);
 * const page = await reader.fetch();   // FeedMessage<string, Order>[]
 * ```
 */
export class Feed<K, V> {
  private compression = false;

  /** @internal — bind via {@link WhitewaterClient.feed}. */
  constructor(
    private readonly client: WhitewaterClient,
    readonly name: string,
    private readonly keyCodec: Codec<K>,
    private readonly valueCodec: Codec<V>,
  ) {}

  /** Enable wire compression on fetches and ad-hoc appends through this
   * facade. */
  useCompression(enabled = true): Feed<K, V> {
    const bound = new Feed<K, V>(this.client, this.name, this.keyCodec, this.valueCodec);
    bound.compression = enabled;
    return bound;
  }

  /** The Domain prefix owning this Feed — the first dotted segment.
   * Writer/Reader/Subscription names resolve inside it so no
   * "randomly named" resource escapes the Domain. */
  private get domainPrefix(): string {
    return this.name.split(".")[0];
  }

  /** Resolve a writer/reader/subscription name inside this Feed's
   * Domain — idempotent for already-qualified names. */
  private scope(name: string): string {
    return name.startsWith(this.domainPrefix + ".")
      ? name
      : `${this.domainPrefix}.${name}`;
  }

  /** Rebind the same feed under different codecs — no wire call:
   * `feed.view(codecs.utf8(), codecs.bytes())` reads raw. */
  view<K2, V2>(keyCodec: Codec<K2>, valueCodec: Codec<V2>): Feed<K2, V2> {
    const bound = new Feed<K2, V2>(this.client, this.name, keyCodec, valueCodec);
    bound.compression = this.compression;
    return bound;
  }

  /** Committed records from the beginning or after an opaque `after`
   * Cursor, decoded through the bound codecs. */
  async records(limit?: number, after?: Cursor | null): Promise<FeedMessage<K, V>[]> {
    const records = await this.feedClient().records(this.name, limit, after);
    return decodeMessages(records, this.keyCodec, this.valueCodec);
  }

  /** Anonymous temporary Reader fetch: decoded records plus the opaque
   * continuation frontier. Retains no server-side progress. */
  async fetch(options: TemporaryFetchOptions = {}): Promise<FeedPage<K, V>> {
    const page = await this.feedClient().temporaryFetch(this.name, options);
    return {
      records: decodeMessages(page.records, this.keyCodec, this.valueCodec),
      nextCursor: page.nextCursor,
    };
  }

  /** Bulk-read committed history once, from `start`, delivering each
   * decoded page to `sink` until the frontier is drained. */
  load(
    start: ReaderStart,
    sink: (messages: FeedMessage<K, V>[]) => void | Promise<void>,
  ): Promise<number> {
    return this.feedClient().load(this.name, start, async (records) => {
      await sink(decodeMessages(records, this.keyCodec, this.valueCodec));
    });
  }

  /**
   * A session-bound Writer on this Feed: declares the Writer when
   * missing (reusing an existing one), opens a session, and binds the
   * feed's codecs so `writer.append(key, value)` just works.
   */
  async writer(writerName: string): Promise<FeedWriter<K, V>> {
    writerName = this.scope(writerName);
    try {
      await this.client.createWriter(writerName, this.name);
    } catch (error) {
      if (!(error instanceof WhitewaterError && error.message.includes("exists"))) {
        throw error;
      }
    }
    const openSession = async (): Promise<WriterSessionClient> => {
      const setup = await this.client.openWriterSession(writerName);
      const epoch = (setup.results[0]?.data as { session_epoch?: number } | undefined)
        ?.session_epoch;
      if (typeof epoch !== "number" || epoch === 0) {
        throw new Error(`open_writer_session returned no session_epoch for ${writerName}`);
      }
      return this.client.writer(writerName, epoch);
    };
    return new FeedWriter<K, V>(
      await openSession(),
      this.keyCodec,
      this.valueCodec,
      openSession,
    );
  }

  /**
   * A session-bound named Reader on this Feed: declares the Reader when
   * missing (at `start`, default BEGINNING — an existing declaration
   * keeps its own start and durable progress), opens a session with the
   * bounded page size, and returns decoded messages.
   */
  async reader(
    readerName: string,
    maxPageSize = 64,
    start: ReaderStart = ReaderStart.beginning(),
  ): Promise<FeedReader<K, V>> {
    readerName = this.scope(readerName);
    try {
      await this.client.createReader(readerName, this.name, start);
    } catch (error) {
      if (!(error instanceof WhitewaterError && error.message.includes("exists"))) {
        throw error;
      }
    }
    const session = await this.client.reader(readerName).open(maxPageSize);
    return new FeedReader<K, V>(
      this.client.reader(readerName),
      session,
      this.keyCodec,
      this.valueCodec,
    );
  }

  /** An ad-hoc single append with a caller-owned writer identity —
   * codecs serialize key/payload. For durable named producers prefer
   * {@link writer}. */
  append(
    writerSessionId: string,
    writerEpoch: number,
    sequence: number,
    key: K | null,
    value: V,
  ): Promise<AppendResult> {
    return this.feedClient().append(this.name, {
      requestId: randomUUID(),
      writerSessionId,
      writerEpoch,
      sequence,
      key: key == null ? null : this.keyCodec.encode(key),
      payload: this.valueCodec.encode(value),
    });
  }

  /** A running subscription on this Feed: a named Reader polled in the
   * background, delivering decoded batches and acknowledging durable
   * progress after each. */
  subscribe(
    reader: string,
    start: ReaderStart,
    onRecords: (messages: FeedMessage<K, V>[]) => void | Promise<void>,
    onError?: (error: WhitewaterError) => void,
  ): Subscription {
    return this.subscribeDecoded(reader, start, undefined, onRecords, onError);
  }

  /** Decoded subscribe with capacity override — the FeedConsumer seam.
   * @internal */
  subscribeDecoded(
    reader: string,
    start: ReaderStart | undefined,
    maxPageSize: number | undefined,
    onRecords: (messages: FeedMessage<K, V>[]) => void | Promise<void>,
    onError?: (error: WhitewaterError) => void,
  ): Subscription {
    return this.client.subscribe(
      this.scope(reader),
      this.name,
      start ?? ReaderStart.beginning(),
      async (records) => {
        await onRecords(decodeMessages(records, this.keyCodec, this.valueCodec));
      },
      onError,
      maxPageSize,
    );
  }

  /** A permanent, supervised consumer bound to this Feed's named
   * Reader — iterate decoded records or `run` a per-record handler;
   * open/fetch/ack/reopen/backoff stay internal:
   *
   * ```ts
   * for await (const msg of feed.consumer("enricher")) {
   *   await process(msg);
   * }
   * ```
   *
   * See {@link FeedConsumer} for the full contract. */
  consumer(reader: string): FeedConsumer<K, V> {
    return new FeedConsumer(this, reader);
  }

  private feedClient() {
    return this.client.feeds.useCompression(this.compression);
  }
}
