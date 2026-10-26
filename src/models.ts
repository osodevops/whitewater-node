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

/** Bounds the named secondary keys one StateStore declares. */
export const MAX_NAMED_KEYS = 10;

/**
 * One typed StateStore/Index value — strings, numbers, booleans,
 * Buffers, and null map to the wire's typed kinds; callers never
 * hand-encode bytes for ordinary values. A `StoreValue[]` carries one
 * typed value per posting component.
 */
export type StoreValue = null | boolean | number | bigint | string | Buffer;

/** One declared secondary key on a StateStore. */
export interface NamedKey {
  name: string;
  /** Non-unique keys map a value to every primary row carrying it (the
   * group/list lookup); `unique` enforces one row per value, arbitrated
   * first-committed-wins by the store's journal order. */
  unique: boolean;
  /** Feed-sourced stores only: the projection replay applies to each
   * committed source record. Build with `Extractor.*`; manual stores
   * take values on each `put` instead. */
  extractor?: StoreExtractor;
}

/** The wire `IndexFieldSpec` an extractor serializes to. */
export interface StoreExtractor {
  kind: "key" | "metadata" | "projection";
  name?: string;
  extractor?: string;
  version?: number;
  field?: string;
}

/** Factories for feed-sourced named-key projections. */
export const Extractor = {
  /** The source record's application Key. */
  recordKey: (): StoreExtractor => ({ kind: "key" }),
  /** A named record Metadata entry. */
  metadata: (name: string): StoreExtractor => ({ kind: "metadata", name }),
  /** A dotted JSON-path projection over the record payload. */
  json: (field: string): StoreExtractor => ({
    kind: "projection",
    extractor: "json",
    version: 1,
    field,
  }),
} as const;

/** Freshness evidence every StateStore read reports. */
export interface StoreFreshness {
  appliedSeq: number;
  committedSeq: number;
  behind: boolean;
  servedBy: string;
}

/** One StateStore row: its primary key and payload bytes. */
export interface StoreRow {
  key: Buffer;
  payload: Buffer;
  appliedCursor?: string;
  freshness?: StoreFreshness;
}

/** A named-key lookup result: rows plus the serving replica's freshness. */
export interface StoreRows {
  rows: StoreRow[];
  freshness: StoreFreshness;
}

/** One bounded iteration page in primary-key order. */
export interface StorePage {
  rows: StoreRow[];
  nextKey: Buffer | null;
  /** The serving replica's applied journal position the page was pinned to. */
  pinnedSeq: number;
  servedBy: string;
}

/** A committed StateStore mutation. */
export interface MutationResult {
  storeId: string;
  mutationId: string;
  sequence: number;
  deduplicated: boolean;
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
  /** Fluent start constant — `readerStartPoint(ReaderStart.BEGINNING)`. */
  BEGINNING: { kind: "beginning" } as ReaderStart,
  /** Fluent start constant — only-new records; alias of `now()`. */
  END: { kind: "now" } as ReaderStart,
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
  /** From a `Date` — millisecond precision widened to epoch nanos. */
  timestamp(when: Date): ReaderStart {
    return { kind: "timestamp", nanos: BigInt(when.getTime()) * 1_000_000n };
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
