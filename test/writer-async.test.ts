/** FeedWriter.write()/writeEvent() — the always-async coalesced write
 * path. Mirrors the Java FeedWriter tests: per-record promises resolve
 * on majority commit, the sender coalesces queued writes into one
 * append-batch, StaleSession reopens resubmit identical request ids,
 * and a short-covering reply fails the batch whole. */
import http from "node:http";
import type { AddressInfo } from "node:net";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { WhitewaterClient } from "../src/clients.js";
import { codecs } from "../src/codecs.js";
import { WhitewaterError } from "../src/errors.js";

interface Order {
  id: string;
  total: number;
}

const WCL_SESSION = {
  request_id: "00000000-0000-7000-8000-000000000006",
  revision: 9,
  authority: "control_plane",
  results: [{ statement: "s", message: "ok", data: { session_epoch: 42 } }],
};

const APPEND_OK = {
  message_id: "00000000-0000-7000-8000-000000000009",
  cursor: "c1",
  deduplicated: false,
};

describe("FeedWriter.write()", () => {
  let server: http.Server;
  let client: WhitewaterClient;
  const calls: { method: string; path: string; body: string }[] = [];
  let batchScript: [number, unknown][] = [];

  const route = (path: string, body: string): [number, unknown] => {
    switch (path) {
      case "/v1/admin/wcl":
        return [200, WCL_SESSION];
      case "/v1/admin/commands":
        return [200, WCL_SESSION];
      case "/v1/writers/append-batch": {
        if (batchScript.length > 0) return batchScript.shift()!;
        const records = (JSON.parse(body).records ?? []) as unknown[];
        return [200, {
          results: records.map((_, i) => ({
            ...APPEND_OK,
            message_id: `00000000-0000-7000-8000-${String(i + 1).padStart(12, "0")}`,
            cursor: `c${i + 1}`,
          })),
          feedback: {},
        }];
      }
      case "/v1/writers/append":
        return [200, APPEND_OK];
      default:
        return [200, {}];
    }
  };

  beforeEach(async () => {
    calls.length = 0;
    batchScript = [];
    server = http.createServer((req, res) => {
      const chunks: Buffer[] = [];
      req.on("data", (c) => chunks.push(c));
      req.on("end", () => {
        const path = (req.url ?? "").split("?")[0];
        const body = Buffer.concat(chunks).toString("utf8");
        calls.push({ method: req.method ?? "GET", path, body });
        const [status, response] = route(path, body);
        res.statusCode = status;
        res.setHeader("content-type", "application/json");
        res.end(JSON.stringify(response));
      });
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const port = (server.address() as AddressInfo).port;
    client = new WhitewaterClient({
      apiKey: "test-key",
      endpoints: [`http://127.0.0.1:${port}`],
    });
  });

  afterEach(() => server.close());

  const bodies = (path: string) =>
    calls.filter((c) => c.path === path).map((c) => JSON.parse(c.body) as Record<string, unknown>);
  const count = (path: string) => calls.filter((c) => c.path === path).length;
  const writer = () =>
    client.feed("orders.events", codecs.json<Order>()).writer("ingest");

  it("coalesces queued writes into one batch", async () => {
    const w = (await writer()).linger(50);
    const pending = [0, 1, 2].map((i) =>
      w.write(`order-${i}`, { id: `order-${i}`, total: i }));
    const results = await Promise.all(pending);
    expect(results.every((r) => r.messageId)).toBe(true);
    expect(count("/v1/writers/append-batch")).toBe(1);
    const batch = bodies("/v1/writers/append-batch")[0];
    expect((batch.records as unknown[]).length).toBe(3);
    for (const record of batch.records as { request_id?: string }[]) {
      expect(record.request_id).toBeTruthy();
    }
    await w.close();
  });

  it("splits on maxBatchRecords", async () => {
    const w = (await writer()).linger(1000).maxBatchRecords(2);
    const pending = [0, 1, 2].map((i) => w.write(`o-${i}`, { id: "o", total: i }));
    await Promise.all(pending);
    const sizes = bodies("/v1/writers/append-batch")
      .map((b) => (b.records as unknown[]).length);
    expect(sizes).toEqual([2, 1]);
    await w.close();
  });

  it("callback receives the ack", async () => {
    const w = await writer();
    const seen: [unknown, unknown][] = [];
    await w.write("order-1", { id: "order-1", total: 9 }, (result, error) => {
      seen.push([result, error]);
    });
    expect(seen.length).toBe(1);
    expect(seen[0][1]).toBeNull();
    expect((seen[0][0] as { messageId: string }).messageId).toBeTruthy();
    await w.close();
  });

  it("reopens on stale session and resubmits the same request id", async () => {
    batchScript = [[409, { error: "fenced", code: "stale_epoch" }]];
    const w = await writer();
    const result = await w.write("order-1", { id: "o", total: 1 });
    expect(result.messageId).toBeTruthy();
    const batches = bodies("/v1/writers/append-batch");
    expect(batches.length).toBe(2);
    const first = (batches[0].records as { request_id: string }[])[0].request_id;
    const second = (batches[1].records as { request_id: string }[])[0].request_id;
    expect(second).toBe(first);
    await w.close();
  });

  it("retries unavailable then succeeds", async () => {
    batchScript = [[503, { error: "down", code: "unavailable" }]];
    const w = await writer();
    await w.write("order-1", { id: "o", total: 1 });
    expect(count("/v1/writers/append-batch")).toBe(2);
    await w.close();
  });

  it("fails every pending write on refusal", async () => {
    batchScript = [
      [400, { error: "bad record", code: "refused" }],
      [400, { error: "bad record", code: "refused" }],
    ];
    const w = (await writer()).linger(50);
    const pending = [0, 1].map((i) => w.write("o", { id: "o", total: i }));
    for (const p of pending) await expect(p).rejects.toBeInstanceOf(WhitewaterError);
    await w.close();
  });

  it("fails the batch on a short-covering reply", async () => {
    batchScript = [[200, {
      results: [{ message_id: "00000000-0000-7000-8000-000000000011", cursor: "c1", deduplicated: false }],
      feedback: {},
    }]];
    const w = (await writer()).linger(50);
    const pending = [0, 1].map((i) => w.write("o", { id: "o", total: i }));
    for (const p of pending) {
      await expect(p).rejects.toThrow("cover every record");
    }
    await w.close();
  });

  it("close flushes queued writes", async () => {
    const w = (await writer()).linger(30_000);
    const pending = w.write("order-1", { id: "order-1", total: 1 });
    await w.close();
    const result = await pending;
    expect(result.messageId).toBeTruthy();
    expect(count("/v1/writers/append-batch")).toBe(1);
  });

  it("write after close rejects", async () => {
    const w = await writer();
    await w.close();
    await expect(w.write("o", { id: "o", total: 1 })).rejects.toThrow("writer closed");
  });
});
