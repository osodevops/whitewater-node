import { describe, expect, it } from "vitest";
import { Cursor } from "../src/models.js";
import * as wire from "../src/wire.js";

describe("wire codec", () => {
  it("encodes scalars on the wire", () => {
    const body = wire.appendRequest(
      "019a0000-0000-7000-8000-000000000001",
      "orders.wr",
      7,
      Buffer.from("key-1"),
      Buffer.from("payload"),
      BigInt("-9223372036854775808"),
      { source: Buffer.from("unit") },
    );
    expect(body.request_id).toBe("019a0000-0000-7000-8000-000000000001");
    expect(body.writer).toBe("orders.wr");
    expect(body.session_epoch).toBe(7);
    expect(body.key_base64).toBe("a2V5LTE=");
    expect(body.payload_base64).toBe("cGF5bG9hZA==");
    // i64 nanoseconds serialize as a decimal string, never a JSON number
    expect(body.event_time_ns).toBe("-9223372036854775808");
    expect(body.metadata_base64).toEqual({ source: "dW5pdA==" });
  });

  it("decodes opaque cursors and nanosecond strings into bigint", () => {
    const record = wire.record({
      cursor: "AAAABBBBCCCCDDDD",
      message_id: "019a0000-0000-7000-8000-000000000002",
      producer_id: "019a0000-0000-7000-8000-000000000003",
      sequence: 42,
      event_time_ns: "9223372036854775807",
      ingest_time_ns: "-5",
      key_base64: "3q2+7w==",
      payload_base64: "AAEC",
      metadata_base64: { m: "eA==" },
    });
    expect(record.cursor).toBe("AAAABBBBCCCCDDDD");
    expect(record.eventTimeNs).toBe(BigInt("9223372036854775807"));
    expect(record.ingestTimeNs).toBe(-5n);
    expect(record.key).toEqual(Buffer.from([0xde, 0xad, 0xbe, 0xef]));
    expect(record.payload).toEqual(Buffer.from([0, 1, 2]));
    expect(record.metadata.m).toEqual(Buffer.from("x"));
  });

  it("keeps cursors opaque strings", () => {
    const opaque = "TSLWCDHsmGBMXtKGSBS3vfBmPrsJnt1cYxWFYi4TpTA";
    const cursor = Cursor.of(opaque);
    expect(cursor).toBe(opaque);
    expect(Cursor.of(null)).toBeNull();
  });

  it("decodes the hot_key flag with a false default", () => {
    const flagged = wire.feedback({
      recommended_batch_count: 8,
      hot_key: true,
    });
    expect(flagged.hotKey).toBe(true);
    expect(wire.feedback({ recommended_batch_count: 100 }).hotKey).toBe(false);
  });
});
