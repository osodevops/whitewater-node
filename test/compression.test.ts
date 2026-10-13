import { describe, expect, it, vi } from "vitest";
import { FeedClient, WriterSessionClient } from "../src/clients.js";
import { Transport } from "../src/transport.js";

const result = { message_id: "00000000-0000-4000-8000-000000000001", cursor: "opaque", deduplicated: false };

describe("optional storage compression", () => {
  it("can toggle Feed compression without changing payloads or the original client", async () => {
    const post = vi.fn().mockResolvedValue(result);
    const original = new FeedClient({ post } as unknown as Transport);
    let configured = original;
    const record = { requestId: result.message_id, writerSessionId: result.message_id, writerEpoch: 1, sequence: 1, key: Buffer.from("key"), payload: Buffer.from([0, 255]) };
    for (const enabled of [false, true, false, true]) {
      configured = configured.useCompression(enabled);
      await configured.append("orders.events", record);
      const [path, body] = post.mock.calls.at(-1)!;
      expect(path).toBe("/v1/feeds/append");
      expect(body.use_compression ?? false).toBe(enabled);
      expect(body.payload_base64).toBe("AP8=");
      expect(body.request_id).toBe(record.requestId);
    }
    await original.append("orders.events", record);
    expect(post.mock.calls.at(-1)![1]).not.toHaveProperty("use_compression");
  });

  it("applies Writer compression to single and batch appends and can disable it", async () => {
    const post = vi.fn();
    let writer = new WriterSessionClient({ post } as unknown as Transport, "orders.writer", 7);
    const input = { requestId: result.message_id, key: Buffer.from("key"), payload: Buffer.from([0, 255]) };
    for (const enabled of [false, true, false]) {
      writer = writer.useCompression(enabled);
      post.mockResolvedValue(result);
      await writer.append(input);
      expect(post.mock.calls.at(-1)![1].use_compression ?? false).toBe(enabled);
      post.mockResolvedValue({ results: [result], feedback: {} });
      await writer.appendBatch([input]);
      const body = post.mock.calls.at(-1)![1].records[0];
      expect(body.use_compression ?? false).toBe(enabled);
      expect(body.payload_base64).toBe("AP8=");
      expect(body.request_id).toBe(input.requestId);
      expect(body.session_epoch).toBe(7);
    }
  });
});
