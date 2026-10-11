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
