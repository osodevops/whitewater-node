/**
 * Wire codec: the only place request/response shapes are interpreted.
 *
 * Binary fields are base64; signed 64-bit nanoseconds are decimal
 * strings decoded into `bigint`; UUIDs serialize as lowercase
 * hyphenated strings; `metadata` maps to `metadata_base64` entries.
 */

import type {
  AppendResult,
  ControlExecution,
  Cursor,
  FeedRecord,
  MemberPage,
  ReaderPage,
  ReaderSession,
  ServerFeedback,
} from "./models.js";
import { Cursor as CursorOf } from "./models.js";

type Json = Record<string, unknown>;

const asObj = (v: unknown): Json => (v && typeof v === "object" ? (v as Json) : {});
const str = (v: unknown): string => (v == null ? "" : String(v));

export function appendRequest(
  requestId: string,
  writer: string,
  sessionEpoch: number,
  key: Buffer | null,
  payload: Buffer,
  eventTimeNs: bigint | null,
  metadata: Record<string, Buffer>,
): Json {
  const body: Json = {
    request_id: requestId,
    writer,
    session_epoch: sessionEpoch,
    payload_base64: payload.toString("base64"),
    metadata_base64: Object.fromEntries(
      Object.entries(metadata).map(([k, v]) => [k, v.toString("base64")]),
    ),
  };
  if (key !== null) body.key_base64 = key.toString("base64");
  if (eventTimeNs !== null) body.event_time_ns = eventTimeNs.toString(10);
  return body;
}

export function feedback(node: unknown): ServerFeedback {
  const n = asObj(node);
  return {
    recommendedBatchCount: Number(n.recommended_batch_count ?? 0),
    recommendedBatchBytes: Number(n.recommended_batch_bytes ?? 0),
    pressure: Number(n.pressure ?? 0),
    retryAfterMs: Number(n.retry_after_ms ?? 0),
    maxFrameBytes: Number(n.max_frame_bytes ?? 0),
    hotKey: Boolean(n.hot_key ?? false),
  };
}

export function appendResult(node: unknown): AppendResult {
  const n = asObj(node);
  return {
    messageId: str(n.message_id),
    cursor: CursorOf.of(str(n.cursor)) as Cursor,
    deduplicated: Boolean(n.deduplicated ?? false),
    feedback: feedback(n.feedback),
  };
}

export function record(node: unknown): FeedRecord {
  const n = asObj(node);
  const metadata: Record<string, Buffer> = {};
  for (const [k, v] of Object.entries(asObj(n.metadata_base64))) {
    metadata[k] = Buffer.from(str(v), "base64");
  }
  return {
    cursor: CursorOf.of(str(n.cursor)) as Cursor,
    messageId: str(n.message_id),
    producerId: str(n.producer_id),
    sequence: Number(n.sequence ?? 0),
    eventTimeNs: BigInt(str(n.event_time_ns)),
    ingestTimeNs: BigInt(str(n.ingest_time_ns)),
    key: n.key_base64 == null ? null : Buffer.from(str(n.key_base64), "base64"),
    payload: Buffer.from(str(n.payload_base64), "base64"),
    metadata,
  };
}

export function records(node: unknown): FeedRecord[] {
  return Array.isArray(node) ? node.map(record) : [];
}

export function readerSession(node: unknown): ReaderSession {
  const n = asObj(node);
  return {
    readerId: str(n.reader_id),
    feedId: str(n.feed_id),
    sessionEpoch: Number(n.session_epoch ?? 0),
    capacity: Number(n.capacity ?? 0),
    deliveredCursor: CursorOf.of(n.delivered_cursor == null ? null : str(n.delivered_cursor)),
    acknowledgedCursor: CursorOf.of(
      n.acknowledged_cursor == null ? null : str(n.acknowledged_cursor),
    ),
  };
}

export function readerPage(node: unknown): ReaderPage {
  const n = asObj(node);
  return {
    records: records(n.records),
    deliveredCursor: CursorOf.of(n.delivered_cursor == null ? null : str(n.delivered_cursor)),
    acknowledgedCursor: CursorOf.of(
      n.acknowledged_cursor == null ? null : str(n.acknowledged_cursor),
    ),
    recommendedCapacity: Number(n.recommended_capacity ?? 0),
    retryAfterMs: Number(n.retry_after_ms ?? 0),
  };
}

export function controlExecution(node: unknown): ControlExecution {
  const n = asObj(node);
  return {
    requestId: str(n.request_id),
    revision: Number(n.revision ?? 0),
    authority: str(n.authority),
    warning: str(n.warning),
    results: Array.isArray(n.results)
      ? n.results.map((item) => {
          const r = asObj(item);
          return {
            statement: str(r.statement),
            message: str(r.message),
            data: asObj(r.data),
          };
        })
      : [],
  };
}

export function memberPage(node: unknown): MemberPage {
  const n = asObj(node);
  const positions: Record<string, string> = {};
  for (const [k, v] of Object.entries(asObj(n.positions))) {
    positions[k] = str(v);
  }
  return {
    records: records(n.records),
    cursor: CursorOf.of(n.cursor == null ? null : str(n.cursor)),
    positions,
    ownershipEpoch: Number(n.ownership_epoch ?? 0),
  };
}
