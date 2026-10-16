import { randomUUID } from "node:crypto";
import { Codec } from "./codecs.js";
import { WhitewaterError } from "./errors.js";
import type {
  AppendBatchResult,
  AppendResult,
  Cursor,
  FeedRecord,
  ReaderPage,
  ReaderSession,
  TemporaryFetchOptions,
} from "./models.js";
import type { ReaderStart } from "./models.js";
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
export class FeedWriter<K, V> {
  constructor(
    private readonly session: WriterSessionClient,
    private readonly keyCodec: Codec<K>,
    private readonly valueCodec: Codec<V>,
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
    try {
      await this.client.createWriter(writerName, this.name);
    } catch (error) {
      if (!(error instanceof WhitewaterError && error.message.includes("exists"))) {
        throw error;
      }
    }
    const setup = await this.client.openWriterSession(writerName);
    const epoch = (setup.results[0]?.data as { session_epoch?: number } | undefined)
      ?.session_epoch;
    if (typeof epoch !== "number" || epoch === 0) {
      throw new Error(`open_writer_session returned no session_epoch for ${writerName}`);
    }
    return new FeedWriter<K, V>(
      this.client.writer(writerName, epoch),
      this.keyCodec,
      this.valueCodec,
    );
  }

  /**
   * A session-bound named Reader on this Feed: declares the Reader (at
   * BEGINNING) when missing, opens a session with the bounded fetch
   * capacity, and returns decoded messages.
   */
  async reader(readerName: string, capacity = 64): Promise<FeedReader<K, V>> {
    try {
      await this.client.createReader(readerName, this.name);
    } catch (error) {
      if (!(error instanceof WhitewaterError && error.message.includes("exists"))) {
        throw error;
      }
    }
    const session = await this.client.reader(readerName).open(capacity);
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
    return this.client.subscribe(reader, this.name, start, async (records) => {
      await onRecords(decodeMessages(records, this.keyCodec, this.valueCodec));
    }, onError);
  }

  private feedClient() {
    return this.client.feeds.useCompression(this.compression);
  }
}
