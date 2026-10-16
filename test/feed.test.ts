import { describe, expect, it, beforeEach, afterEach } from "vitest";
import * as http from "node:http";
import { AddressInfo } from "node:net";
import {
  codecs,
  Cursor,
  FeedMessage,
  ReaderStart,
  textMetadata,
  WhitewaterClient,
} from "../src/index.js";

interface Order {
  id: string;
  total: number;
}

/** Scripted stub covering the typed Feed facade endpoints. */
describe("typed feed facade", () => {
  let server: http.Server;
  let client: WhitewaterClient;
  const calls: { method: string; path: string; body: string }[] = [];
  const fetchPages: string[] = [];
  const temporaryPages: string[] = [];
  let recordsResponse: unknown[] = [];

  const WCL_OK = {
    request_id: "00000000-0000-7000-8000-000000000006",
    revision: 9,
    authority: "control_plane",
    results: [{ statement: "s", message: "ok", data: {} }],
  };

  const WCL_SESSION = {
    ...WCL_OK,
    results: [{ statement: "s", message: "ok", data: { session_epoch: 42 } }],
  };

  const SESSION = {
    reader_id: "00000000-0000-7000-8000-000000000004",
    feed_id: "00000000-0000-7000-8000-000000000005",
    session_epoch: 3,
    capacity: 128,
    delivered_cursor: null,
    acknowledged_cursor: null,
  };

  const APPEND_OK = {
    message_id: "00000000-0000-7000-8000-000000000009",
    cursor: "c1",
    deduplicated: false,
    feedback: { recommended_batch_count: 8, pressure: 0.1 },
  };

  const BATCH_OK = {
    results: [
      { message_id: "00000000-0000-7000-8000-000000000011", cursor: "c1", deduplicated: false },
    ],
    feedback: { recommended_batch_count: 8 },
  };

  const record = (cursor: string, key: string | null, payload: string, metadata?: Record<string, string>) => ({
    cursor,
    message_id: "00000000-0000-7000-8000-000000000002",
    producer_id: "00000000-0000-7000-8000-000000000003",
    sequence: 7,
    event_time_ns: "1697040000123456789",
    ingest_time_ns: "1697040000223456789",
    key_base64: key === null ? null : Buffer.from(key).toString("base64"),
    payload_base64: Buffer.from(payload).toString("base64"),
    metadata_base64: Object.fromEntries(
      Object.entries(metadata ?? {}).map(([n, v]) => [n, Buffer.from(v).toString("base64")]),
    ),
  });

  const route = (path: string): [number, unknown] => {
    switch (path) {
      case "/v1/admin/wcl":
        return [200, WCL_SESSION];
      case "/v1/admin/commands":
        return [200, WCL_OK];
      case "/v1/writers/append":
      case "/v1/feeds/append":
        return [200, APPEND_OK];
      case "/v1/writers/append-batch":
        return [200, BATCH_OK];
      case "/v1/readers/open":
      case "/v1/readers/ack":
      case "/v1/readers/close":
        return [200, SESSION];
      case "/v1/readers/fetch":
        return [200, JSON.parse(fetchPages.shift() ?? `{"records":[],"delivered_cursor":null}`)];
      case "/v1/readers/temporary/fetch":
        return [200, JSON.parse(temporaryPages.shift() ?? `{"records":[],"next_cursor":null}`)];
      case "/v1/feeds/records":
        return [200, recordsResponse];
      default:
        return [200, {}];
    }
  };

  beforeEach(async () => {
    calls.length = 0;
    fetchPages.length = 0;
    temporaryPages.length = 0;
    recordsResponse = [];
    server = http.createServer((req, res) => {
      const chunks: Buffer[] = [];
      req.on("data", (c) => chunks.push(c));
      req.on("end", () => {
        const path = (req.url ?? "").split("?")[0];
        calls.push({ method: req.method ?? "GET", path, body: Buffer.concat(chunks).toString("utf8") });
        const [status, response] = route(path);
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
    calls.filter((c) => c.path === path).map((c) => JSON.parse(c.body) as Record<string, never>);
  const count = (path: string) => calls.filter((c) => c.path === path).length;
  const decodeB64 = (value: unknown) => Buffer.from(String(value), "base64").toString("utf8");

  it("binds a feed and refuses invalid dotted names", () => {
    expect(() => client.feed("Bad Name", codecs.json<Order>())).toThrow("invalid dotted name");
    expect(client.feed("orders.events", codecs.json<Order>()).name).toBe("orders.events");
  });

  it("records() decodes JSON and preserves the envelope", async () => {
    recordsResponse = [
      record("c1", "order-1", `{"id":"order-1","total":42}`, { tenant: "acme" }),
    ];
    const orders = client.feed("orders.events", codecs.json<Order>());
    const messages = await orders.records(10);
    expect(messages).toHaveLength(1);
    const m = messages[0];
    expect(m.key).toBe("order-1");
    expect(m.value).toEqual({ id: "order-1", total: 42 });
    expect(m.cursor).toBe("c1");
    expect(m.sequence).toBe(7);
    expect(m.eventTimeNs).toBe(1697040000123456789n);
    expect(m.metadata.tenant?.toString("utf8")).toBe("acme");
  });

  it("fetch() returns a decoded temporary page with next cursor", async () => {
    temporaryPages.push(
      JSON.stringify({
        records: [record("c1", "o1", `{"id":"o1","total":1}`)],
        next_cursor: "c9",
      }),
    );
    const orders = client.feed("orders.events", codecs.json<Order>());
    const page = await orders.fetch({ limit: 4 });
    expect(page.records[0].value.total).toBe(1);
    expect(page.nextCursor).toBe("c9");
  });

  it("load() delivers decoded batches until drained", async () => {
    temporaryPages.push(
      JSON.stringify({
        records: [
          record("c1", "k1", `{"id":"a","total":1}`),
          record("c2", "k2", `{"id":"b","total":2}`),
        ],
        next_cursor: "c2",
      }),
    );
    const orders = client.feed("orders.events", codecs.json<Order>());
    const seen: Order[] = [];
    const loaded = await orders.load(ReaderStart.beginning(), (messages) => {
      for (const m of messages) seen.push(m.value);
    });
    expect(loaded).toBe(2);
    expect(seen.map((o) => o.id)).toEqual(["a", "b"]);
  });

  it("writer() declares, opens, and encodes through the codecs", async () => {
    const orders = client.feed("orders.events", codecs.json<Order>());
    const writer = await orders.writer("ingest");
    expect(writer.sessionEpoch).toBe(42);
    await writer.append("order-1", { id: "order-1", total: 7 });

    const wcl = bodies("/v1/admin/wcl").map((b) => b.script as string);
    expect(wcl).toEqual([
      "CREATE WRITER ingest TO orders.events",
      "OPEN WRITER SESSION ingest",
    ]);
    const appends = bodies("/v1/writers/append");
    expect(appends).toHaveLength(1);
    expect(appends[0].writer).toBe("ingest");
    expect(appends[0].session_epoch).toBe(42);
    expect(decodeB64(appends[0].key_base64)).toBe("order-1");
    expect(JSON.parse(decodeB64(appends[0].payload_base64))).toEqual({
      id: "order-1",
      total: 7,
    });
    expect(appends[0].request_id).toBeTruthy();
  });

  it("appendAll() batches events incl. keyless, pinned ids, times, metadata", async () => {
    const orders = client.feed("orders.events", codecs.json<Order>());
    const writer = await orders.writer("ingest");
    const result = await writer.appendAll([
      {
        requestId: "rid-1",
        key: "order-9",
        value: { id: "order-9", total: 9 },
        eventTimeNs: 1697040000000000000n,
        metadata: textMetadata({ tenant: "acme" }),
      },
      { value: { id: "keyless", total: 0 } },
    ]);
    expect(result.results).toHaveLength(1);
    const batches = bodies("/v1/writers/append-batch");
    expect(batches).toHaveLength(1);
    const records = batches[0].records as Record<string, never>[];
    expect(records).toHaveLength(2);
    expect(records[0].request_id).toBe("rid-1");
    expect(records[0].event_time_ns).toBe("1697040000000000000");
    expect((records[0].metadata_base64 as Record<string, string>).tenant)
      .toBe(Buffer.from("acme").toString("base64"));
    expect(records[1].key_base64).toBeUndefined();
  });

  it("reader() fetches decoded pages and acks delivered cursor", async () => {
    fetchPages.push(
      JSON.stringify({
        records: [record("c1", "k1", `{"id":"o1","total":3}`)],
        delivered_cursor: "c1",
        acknowledged_cursor: null,
        recommended_capacity: 8,
      }),
    );
    const orders = client.feed("orders.events", codecs.json<Order>());
    const reader = await orders.reader("audit", 64);
    expect(reader.sessionEpoch).toBe(3);
    const page = await reader.fetch();
    expect(page.records[0].value.id).toBe("o1");
    expect(page.deliveredCursor).toBe("c1");
    await reader.ack();
    expect(bodies("/v1/readers/ack")[0].cursor).toBe("c1");
    await reader.close();
    expect(count("/v1/readers/close")).toBe(1);
  });

  it("ad-hoc append() posts encoded fields to /v1/feeds/append", async () => {
    const orders = client.feed("orders.events", codecs.json<Order>());
    await orders.append("ws-1", 7, 3, "order-2", { id: "order-2", total: 11 });
    const bodies_ = bodies("/v1/feeds/append");
    expect(bodies_).toHaveLength(1);
    expect(bodies_[0].feed).toBe("orders.events");
    expect(bodies_[0].writer_session_id).toBe("ws-1");
    expect(bodies_[0].writer_epoch).toBe(7);
    expect(bodies_[0].sequence).toBe(3);
    expect(decodeB64(bodies_[0].key_base64)).toBe("order-2");
  });

  it("view() rebinds codecs without a wire call", async () => {
    recordsResponse = [record("c1", "k1", "raw-bytes")];
    const orders = client.feed("orders.events", codecs.json<Order>());
    const raw = orders.view(codecs.utf8(), codecs.bytes());
    const messages = await raw.records(1);
    expect(messages[0].value.toString("utf8")).toBe("raw-bytes");
    expect(raw.name).toBe("orders.events");
  });

  it("a custom codec feeds through encode and decode", async () => {
    const xmlish = codecs.custom<string>(
      (s) => Buffer.from(`<v>${s}</v>`, "utf8"),
      (b) => b.toString("utf8").replace(/^<v>|<\/v>$/g, ""),
    );
    recordsResponse = [record("c1", "k1", "<v>hello</v>")];
    const feed = client.feed("orders.events", codecs.utf8(), xmlish);
    const messages = await feed.records(1);
    expect(messages[0].value).toBe("hello");
  });

  it("decode failures surface rather than corrupting", async () => {
    recordsResponse = [record("c1", "k1", "not-json")];
    const orders = client.feed("orders.events", codecs.json<Order>());
    await expect(orders.records(1)).rejects.toThrow();
  });

  it("the default feed binds UTF-8 keys and raw payload bytes", async () => {
    recordsResponse = [record("c1", "k1", "raw-payload")];
    const feed = client.feed("orders.events");
    const messages = await feed.records(1);
    expect(messages[0].key).toBe("k1");
    expect(messages[0].value.toString("utf8")).toBe("raw-payload");
  });

  it("keyless records decode key as null", async () => {
    recordsResponse = [record("c1", null, `{"id":"x","total":1}`)];
    const orders = client.feed("orders.events", codecs.json<Order>());
    const messages = await orders.records(1);
    expect(messages[0].key).toBeNull();
    expect(messages[0].value.id).toBe("x");
  });

  it("subscribe() delivers decoded batches and acks", async () => {
    fetchPages.push(
      JSON.stringify({
        records: [record("c1", "k1", `{"id":"o1","total":3}`)],
        delivered_cursor: "c1",
      }),
      `{"records":[],"delivered_cursor":null}`,
    );
    const orders = client.feed("orders.events", codecs.json<Order>());
    const received: FeedMessage<string, Order>[][] = [];
    const sub = orders.subscribe("audit", ReaderStart.beginning(), (messages) => {
      received.push(messages);
      sub.close();
    });
    const deadline = Date.now() + 5000;
    while (received.length === 0 && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 20));
    }
    expect(received).toHaveLength(1);
    expect(received[0][0].value.id).toBe("o1");
    expect(count("/v1/readers/ack")).toBeGreaterThanOrEqual(1);
  });

  it("int64 codec round-trips bigint keys", async () => {
    const codec = codecs.int64();
    const encoded = codec.encode(1697040000123456789n);
    expect(encoded).toHaveLength(8);
    expect(codec.decode(encoded)).toBe(1697040000123456789n);
    recordsResponse = [
      {
        ...record("c1", null, `{"id":"x","total":1}`),
        key_base64: encoded.toString("base64"),
      },
    ];
    const feed = client.feed("orders.events", codecs.int64(), codecs.json<Order>());
    const messages = await feed.records(1);
    expect(messages[0].key).toBe(1697040000123456789n);
  });
});
