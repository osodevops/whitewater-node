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
  private readonly rows = new Map<string, FeedRecord>();

  private constructor(readonly name: string) {}

  static named(name: string): RecordStore {
    if (!name || !name.trim()) throw new Error("store name is required");
    return new RecordStore(name);
  }

  /** Insert or replace the row for this record's key. */
  put(record: FeedRecord): void {
    this.rows.set(recordKey(record), record);
  }

  putAll(records: FeedRecord[]): void {
    for (const record of records) this.put(record);
  }

  /** The latest record for a key, when one has been loaded. */
  get(key: Buffer): FeedRecord | undefined {
    return this.rows.get(key.toString("base64"));
  }

  get size(): number {
    return this.rows.size;
  }

  values(): FeedRecord[] {
    return [...this.rows.values()];
  }
}

function recordKey(record: FeedRecord): string {
  return record.key != null ? record.key.toString("base64") : `cursor:${record.cursor}`;
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
