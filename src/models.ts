/**
 * Whitewater wire-domain models.
 *
 * `Cursor` is a branded string: opaque by type, never parsed, ordered,
 * or fabricated — passed back to the Riverbed verbatim.
 * `eventTimeNs`/`ingestTimeNs` are `bigint` (signed 64-bit nanoseconds);
 * on the wire they travel as decimal strings because JSON numbers lose
 * i64 range.
 */

export type Cursor = string & { readonly __cursor: unique symbol };

export const Cursor = {
  of(value: string | null | undefined): Cursor | null {
    return value == null ? null : (value as Cursor);
  },
};

export interface FeedRecord {
  cursor: Cursor;
  messageId: string;
  producerId: string;
  sequence: number;
  eventTimeNs: bigint;
  ingestTimeNs: bigint;
  key: Buffer | null;
  payload: Buffer;
  metadata: Record<string, Buffer>;
}

export interface ServerFeedback {
  recommendedBatchCount: number;
  recommendedBatchBytes: number;
  pressure: number;
  retryAfterMs: number;
  maxFrameBytes: number;
  hotKey: boolean;
}

export interface AppendResult {
  messageId: string;
  cursor: Cursor;
  deduplicated: boolean;
  feedback: ServerFeedback;
}

export interface AppendBatchResult {
  results: AppendResult[];
  feedback: ServerFeedback;
}

export interface ReaderSession {
  readerId: string;
  feedId: string;
  sessionEpoch: number;
  capacity: number;
  deliveredCursor: Cursor | null;
  acknowledgedCursor: Cursor | null;
}

export interface ReaderPage {
  records: FeedRecord[];
  deliveredCursor: Cursor | null;
  acknowledgedCursor: Cursor | null;
  recommendedCapacity: number;
  retryAfterMs: number;
}

export interface StatementResult {
  statement: string;
  message: string;
  data: Record<string, unknown>;
}

export interface ControlExecution {
  requestId: string;
  revision: number;
  authority: string;
  warning: string;
  results: StatementResult[];
}

export interface Joined {
  memberId: string;
  memberEpoch: number;
  ownershipEpoch: number;
  tick: number;
}

export interface WorkLease {
  workId: string;
  memberId: string;
  memberEpoch: number;
  leaseEpoch: number;
  expiresAtTick: number;
}

export interface MemberPage {
  records: FeedRecord[];
  cursor: Cursor | null;
  positions: Record<string, string>;
  ownershipEpoch: number;
}

/**
 * One ad-hoc append for POST /v1/feeds/append: the caller supplies the
 * writer identity directly instead of holding a named Writer session.
 * Identical `requestId` retries dedupe server-side.
 */
export interface FeedAppend {
  requestId: string;
  writerSessionId: string;
  writerEpoch: number;
  sequence: number;
  key: Buffer | null;
  payload: Buffer;
  eventTimeNs?: bigint | null;
  metadata?: Record<string, Buffer>;
}

/**
 * Options for an anonymous temporary Reader fetch. `after` continues
 * after an opaque Cursor; `afterEventTimeNs` seeks by record event time;
 * `newOnly` positions at the current end without returning history;
 * `tail` plus `waitMs` long-polls for newly committed records.
 */
export interface TemporaryFetchOptions {
  after?: Cursor | null;
  limit?: number;
  newOnly?: boolean;
  tail?: boolean;
  waitMs?: number;
  afterEventTimeNs?: bigint | null;
}

export interface TemporaryFetchPage {
  records: FeedRecord[];
  nextCursor: Cursor | null;
}

/**
 * A named, in-memory store of Feed records — the target of
 * `client.feeds.load(...)` and a typical sink for `client.subscribe(...)`:
 *
 * ```ts
 * const store = RecordStore.named("employee-store");
 * await client.feeds.load("hr.employees", ReaderStart.beginning(), store);
 * const sub = client.subscribe("hr-view", "hr.employees", ReaderStart.now(),
 *   (records) => store.putAll(records));
 * await sub.close();
 * ```
 *
 * Rows key on the record's key bytes; unkeyed records key on their Cursor.
 */
export class RecordStore {
  /** Named secondary keys per store; matches the server Index field budget. */
  static readonly MAX_NAMED_KEYS = 10;

  private readonly rows = new Map<string, FeedRecord>();
  private readonly declaredKeys = new Set<string>();
  /** namedKey -> base64 value -> set of primary ids (non-unique postings). */
  private readonly namedIndex = new Map<string, Map<string, Set<string>>>();

  private constructor(readonly name: string) {}

  static named(name: string): RecordStore {
    if (!name || !name.trim()) throw new Error("store name is required");
    return new RecordStore(name);
  }

  /** Declare a named secondary key (at most {@link MAX_NAMED_KEYS}). */
  addKey(namedKey: string): this {
    if (!namedKey || !namedKey.trim()) throw new Error("named key is required");
    if (!this.declaredKeys.has(namedKey) && this.declaredKeys.size >= RecordStore.MAX_NAMED_KEYS) {
      throw new Error(`a RecordStore supports at most ${RecordStore.MAX_NAMED_KEYS} named keys`);
    }
    this.declaredKeys.add(namedKey);
    if (!this.namedIndex.has(namedKey)) this.namedIndex.set(namedKey, new Map());
    return this;
  }

  /** The declared named keys, in declaration order. */
  namedKeys(): string[] {
    return [...this.declaredKeys];
  }

  /** Start a row write: choose the primary key and named-key values. */
  upsert(record: FeedRecord): Upsert {
    return new Upsert(this, record);
  }

  /** Insert or replace the row for this record's key. */
  put(record: FeedRecord): void {
    this.commitUpsert(record, defaultId(record), new Map());
  }

  putAll(records: FeedRecord[]): void {
    for (const record of records) this.put(record);
  }

  /** The latest record for a key, when one has been loaded. */
  get(key: Buffer | string): FeedRecord | undefined {
    const id = typeof key === "string" ? Buffer.from(key, "utf8").toString("base64") : key.toString("base64");
    return this.rows.get(id);
  }

  /** Every row whose named key equals `value` — named keys are not unique. */
  getBy(namedKey: string, value: Buffer | string): FeedRecord[] {
    const postings = this.namedIndex.get(namedKey);
    if (!postings) throw new Error(`undeclared named key: ${namedKey}`);
    const id = typeof value === "string" ? Buffer.from(value, "utf8").toString("base64") : value.toString("base64");
    const rows: FeedRecord[] = [];
    for (const primaryId of postings.get(id) ?? []) {
      const record = this.rows.get(primaryId);
      if (record) rows.push(record);
    }
    return rows;
  }

  /** Remove the row for a key plus its named-key postings. */
  delete(key: Buffer | string): boolean {
    const id = typeof key === "string" ? Buffer.from(key, "utf8").toString("base64") : key.toString("base64");
    if (!this.rows.delete(id)) return false;
    this.removePostings(id);
    return true;
  }

  /** Empty the primary map and every named-key index. */
  clear(): void {
    this.rows.clear();
    for (const postings of this.namedIndex.values()) postings.clear();
  }

  /**
   * Copy every row into `target`, re-attaching named-key postings;
   * missing named keys are declared on the target.
   */
  copy(target: RecordStore): void {
    for (const namedKey of this.declaredKeys) target.addKey(namedKey);
    for (const [id, record] of this.rows) {
      target.commitUpsert(record, id, this.namedValuesOf(id));
    }
  }

  /** Apply `fn` to a stable snapshot of every row. */
  forEach(fn: (entry: StoreEntry) => void): void {
    for (const entry of this.entries()) fn(entry);
  }

  /**
   * `forEach` fanned out with at most `concurrency` in-flight callbacks —
   * useful when `fn` performs async work. Node runs callbacks on the event
   * loop (not CPU threads); concurrency bounds outstanding invocations.
   */
  async forEachParallel(concurrency: number, fn: (entry: StoreEntry) => void | Promise<void>): Promise<void> {
    if (!Number.isInteger(concurrency) || concurrency < 1) {
      throw new Error("concurrency must be a positive integer");
    }
    const entries = this.entries();
    let next = 0;
    const workers = Array.from({ length: Math.min(concurrency, entries.length) }, async () => {
      while (next < entries.length) {
        await fn(entries[next++]);
      }
    });
    await Promise.all(workers);
  }

  entries(): StoreEntry[] {
    return [...this.rows].map(([id, record]) => ({
      primaryKey: Buffer.from(id, "base64"),
      record,
    }));
  }

  get size(): number {
    return this.rows.size;
  }

  values(): FeedRecord[] {
    return [...this.rows.values()];
  }

  /** @internal */
  commitUpsert(record: FeedRecord, primaryId: string, named: Map<string, Buffer>): void {
    this.removePostings(primaryId);
    for (const [key, value] of named) {
      const postings = this.namedIndex.get(key);
      if (!postings) throw new Error(`undeclared named key: ${key}`);
      const valueId = value.toString("base64");
      let ids = postings.get(valueId);
      if (!ids) postings.set(valueId, (ids = new Set()));
      ids.add(primaryId);
    }
    this.rows.set(primaryId, record);
  }

  /** @internal */
  changePrimary(oldId: string, newId: string): void {
    this.rows.delete(oldId);
    this.removePostings(oldId);
    void newId;
  }

  private namedValuesOf(primaryId: string): Map<string, Buffer> {
    const named = new Map<string, Buffer>();
    for (const [key, postings] of this.namedIndex) {
      for (const [valueId, ids] of postings) {
        if (ids.has(primaryId)) named.set(key, Buffer.from(valueId, "base64"));
      }
    }
    return named;
  }

  private removePostings(primaryId: string): void {
    for (const postings of this.namedIndex.values()) {
      for (const [valueId, ids] of postings) {
        ids.delete(primaryId);
        if (ids.size === 0) postings.delete(valueId);
      }
    }
  }
}

/** A stored row: its primary key and the record value. */
export interface StoreEntry {
  primaryKey: Buffer;
  record: FeedRecord;
}

/** Row-level write builder returned by {@link RecordStore.upsert}. */
export class Upsert {
  private primaryId: string;
  private readonly named = new Map<string, Buffer>();

  /** @internal */
  constructor(private readonly store: RecordStore, private readonly record: FeedRecord) {
    this.primaryId = defaultId(record);
  }

  /** Choose the row's primary key. */
  withKey(key: Buffer | string): this {
    const id = typeof key === "string" ? Buffer.from(key, "utf8").toString("base64") : key.toString("base64");
    if (id !== this.primaryId) {
      this.store.changePrimary(this.primaryId, id);
      this.primaryId = id;
    }
    this.store.commitUpsert(this.record, this.primaryId, this.named);
    return this;
  }

  /** Add a named-key value (must be declared via {@link RecordStore.addKey}). */
  withNamedKey(namedKey: string, value: Buffer | string): this {
    const bytes = typeof value === "string" ? Buffer.from(value, "utf8") : value;
    this.named.set(namedKey, bytes);
    this.store.commitUpsert(this.record, this.primaryId, this.named);
    return this;
  }
}

function defaultId(record: FeedRecord): string {
  return (record.key != null ? record.key : Buffer.from(`cursor:${record.cursor}`, "utf8")).toString("base64");
}

/**
 * Where a Reader or Subscription starts consuming a Feed — maps to the
 * WCL `START AT` clause. Build with {@link ReaderStart} factories.
 */
export type ReaderStart =
  | { kind: "beginning" }
  | { kind: "now" }
  | { kind: "cursor"; cursor: Cursor }
  | { kind: "timestamp"; nanos: bigint };

export const ReaderStart = {
  /** From the oldest retained record. */
  beginning(): ReaderStart {
    return { kind: "beginning" };
  },
  /** From records appended after creation. */
  now(): ReaderStart {
    return { kind: "now" };
  },
  /** From an opaque Cursor previously returned by the Riverbed. */
  cursor(cursor: Cursor): ReaderStart {
    return { kind: "cursor", cursor };
  },
  /** From a signed Unix epoch nanosecond timestamp. */
  timestampNs(nanos: bigint): ReaderStart {
    return { kind: "timestamp", nanos };
  },
  /** The WCL `START AT` operand, e.g. `BEGINNING`. */
  wcl(start: ReaderStart): string {
    switch (start.kind) {
      case "beginning":
        return "BEGINNING";
      case "now":
        return "NOW";
      case "cursor":
        return `CURSOR '${start.cursor.replace(/'/g, "''")}'`;
      case "timestamp":
        return `TIMESTAMP ${start.nanos}`;
    }
  },
};
