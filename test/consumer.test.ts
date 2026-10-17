import { describe, expect, it, beforeEach, afterEach } from "vitest";
import * as http from "node:http";
import { AddressInfo } from "node:net";
import {
  codecs,
  FeedMessage,
  Refused,
  WhitewaterClient,
  WhitewaterError,
} from "../src/index.js";

/**
 * The stub holds a pending page: once queued it keeps answering fetches
 * until an ack lands — exactly like the server redelivering an
 * unacknowledged page.
 */
describe("feed.consumer + store find helpers", () => {
  let server: http.Server;
  let client: WhitewaterClient;
  const calls: { path: string; body: string }[] = [];
  let pending: string | null = null;
  let queuedPages: string[] = [];
  let wclStatus = 200;
  let wclBody: unknown = null;
  let opens = 0;
  let openError: [number, unknown] | null = null;
  let delivered = 0;

  const SESSION = {
    reader_id: "00000000-0000-7000-8000-000000000004",
    feed_id: "00000000-0000-7000-8000-000000000005",
    session_epoch: 3,
    capacity: 128,
    delivered_cursor: null,
    acknowledged_cursor: null,
  };

  const WCL_OK = {
    request_id: "00000000-0000-7000-8000-000000000006",
    revision: 9,
    authority: "control_plane",
    results: [{ statement: "s", message: "ok", data: {} }],
  };

  const record = (cursor: string, payload: string) => ({
    cursor,
    message_id: "00000000-0000-7000-8000-000000000002",
    producer_id: "00000000-0000-7000-8000-000000000003",
    sequence: 7,
    event_time_ns: "1697040000123456789",
    ingest_time_ns: "1697040000223456789",
    key_base64: Buffer.from("k-" + cursor).toString("base64"),
    payload_base64: Buffer.from(payload).toString("base64"),
    metadata_base64: {},
  });

  const page = (delivered: string | null, ...payloads: string[]) =>
    JSON.stringify({
      records: payloads.map((p, i) => record(`c${i + 1}`, p)),
      delivered_cursor: delivered,
      acknowledged_cursor: null,
      recommended_capacity: 0,
      retry_after_ms: 0,
    });

  const route = (path: string): [number, unknown] => {
    switch (path) {
      case "/v1/admin/wcl":
        return [wclStatus, wclBody ?? WCL_OK];
      case "/v1/readers/open":
        opens++;
        return openError ?? [200, SESSION];
      case "/v1/readers/fetch": {
        delivered++;
        let response: string;
        if (pending !== null) {
          response = pending; // unacked: redeliver
        } else if (queuedPages.length > 0) {
          pending = queuedPages.shift()!;
          response = pending;
        } else {
          response = page(null);
        }
        return [200, JSON.parse(response)];
      }
      case "/v1/readers/ack":
        pending = null;
        return [200, SESSION];
      case "/v1/readers/close":
        return [200, SESSION];
      default:
        return [200, {}];
    }
  };

  const callsFor = (path: string) =>
    calls.filter((c) => c.path === path).map((c) => JSON.parse(c.body));

  const waitFor = async (cond: () => boolean, what: string) => {
    const deadline = Date.now() + 5000;
    while (!cond() && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 10));
    }
    if (!cond()) throw new Error(`timed out waiting for ${what}`);
  };

  beforeEach(async () => {
    calls.length = 0;
    pending = null;
    queuedPages = [];
    wclStatus = 200;
    wclBody = null;
    opens = 0;
    openError = null;
    delivered = 0;
    server = http.createServer((req, res) => {
      const chunks: Buffer[] = [];
      req.on("data", (c) => chunks.push(c));
      req.on("end", () => {
        const path = (req.url ?? "").split("?")[0];
        calls.push({ path, body: Buffer.concat(chunks).toString("utf8") });
        const [status, response] = route(path);
        res.statusCode = status;
        res.setHeader("content-type", "application/json");
        res.end(JSON.stringify(response));
      });
    });
    await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
    const { port } = server.address() as AddressInfo;
    client = new WhitewaterClient({
      apiKey: "test-key",
      endpoints: [`http://127.0.0.1:${port}`],
    });
  });

  afterEach(async () => {
    await new Promise<void>((r) => server.close(() => r()));
  });

  it("delivers each record and acks the page on run()", async () => {
    queuedPages = [page("c2", "alpha", "beta")];
    const feed = client.feed("users.update.request");
    const seen: FeedMessage<string, Buffer>[] = [];
    const consumer = feed.consumer("enricher");
    const running = consumer.run((m) => {
      seen.push(m);
    });

    await waitFor(() => seen.length === 2, "records");
    await waitFor(() => callsFor("/v1/readers/ack").length === 1, "ack");
    await consumer.close();
    await running;

    expect(seen.map((m) => m.value.toString())).toEqual([
      "alpha",
      "beta",
    ]);
    expect(callsFor("/v1/readers/ack")[0].cursor).toBe("c2");
    expect(callsFor("/v1/readers/close").length).toBeGreaterThan(0);
  });

  it("iterates records via for-await; break closes cleanly", async () => {
    queuedPages = [page("c1", "alpha"), page("c2", "beta")];
    const feed = client.feed("users.update.request");
    const seen: string[] = [];

    for await (const msg of feed.consumer("enricher")) {
      seen.push(Buffer.from(msg.value).toString());
      if (seen.length === 2) break;
    }

    expect(seen).toEqual(["alpha", "beta"]);
    await waitFor(() => callsFor("/v1/readers/close").length > 0, "close");
  });

  it("a thrown handler skips the ack and the page redelivers", async () => {
    queuedPages = [page("c1", "alpha")];
    const feed = client.feed("users.update.request");
    let attempts = 0;
    const errors: unknown[] = [];
    const consumer = feed
      .consumer("enricher")
      .backoff(10, 50)
      .onError((e) => errors.push(e));
    const running = consumer.run(() => {
      attempts++;
      if (attempts === 1) throw new Error("transient handler bug");
      void consumer.close();
    });

    await running;
    expect(attempts).toBeGreaterThanOrEqual(2); // redelivered
    expect(callsFor("/v1/readers/ack")).toHaveLength(1); // only the replay
    expect(opens).toBeGreaterThanOrEqual(2); // supervised restart
    expect(errors.some((e) => e instanceof Error)).toBe(true);
  });

  it("reopens and resumes after a fenced session", async () => {
    openError = [409, { error: "fenced", code: "stale_epoch" }];
    const feed = client.feed("users.update.request");
    const seen: string[] = [];
    const consumer = feed.consumer("enricher").backoff(10, 50);
    const running = consumer.run((m) => {
      seen.push(m.value.toString());
      void consumer.close();
    });
    void waitFor(() => opens > 0, "open").then(() => {
      openError = null;
      queuedPages = [page("c1", "alpha")];
    });

    await running;
    expect(seen).toEqual(["alpha"]);
    expect(opens).toBeGreaterThanOrEqual(2);
  });

  it("fails fast on startup refusal", async () => {
    wclStatus = 400;
    wclBody = { error: "refused: denied", code: "refused" };
    const feed = client.feed("users.update.request");

    await expect(feed.consumer("enricher").run(() => {})).rejects.toBeInstanceOf(
      Refused,
    );
    expect(opens).toBe(0);
  });

  it("propagates startup refusal through the iterator", async () => {
    wclStatus = 400;
    wclBody = { error: "refused: denied", code: "refused" };
    const feed = client.feed("users.update.request");

    await expect(async () => {
      for await (const _msg of feed.consumer("enricher")) {
        void _msg;
      }
    }).rejects.toBeInstanceOf(Refused);
  });

  it("fails fast on a silent death instead of looping", async () => {
    const feed = client.feed("users.update.request");
    await expect(
      feed.consumer("Bad Name!").run(() => {}),
    ).rejects.toBeInstanceOf(WhitewaterError);
    expect(calls).toHaveLength(0); // died on local validation
  });

  it("store find/findBy/getOrDefault return values not rows", async () => {
    const storeBodies: Record<string, unknown> = {
      "/v1/state-stores/lookup": {
        found: true,
        payload_base64: Buffer.from('"alice"').toString("base64"),
        applied_cursor: "00000000000000000003",
        applied_seq: 7,
        committed_seq: 7,
        behind: false,
        served_by: "storage-1",
      },
      "/v1/state-stores/get-by": {
        rows: [
          {
            key_base64: Buffer.from("a@x.io").toString("base64"),
            payload_base64: Buffer.from('"alice"').toString("base64"),
            applied_cursor: "00000000000000000003",
          },
        ],
        applied_seq: 7,
        committed_seq: 7,
        behind: false,
        served_by: "storage-1",
      },
    };
    // Extend the stub for store routes this test needs.
    const storeServer = http.createServer((req, res) => {
      const chunks: Buffer[] = [];
      req.on("data", (c) => chunks.push(c));
      req.on("end", () => {
        const path = (req.url ?? "").split("?")[0];
        calls.push({ path, body: Buffer.concat(chunks).toString("utf8") });
        res.statusCode = 200;
        res.setHeader("content-type", "application/json");
        res.end(JSON.stringify(storeBodies[path] ?? {}));
      });
    });
    await new Promise<void>((r) => storeServer.listen(0, "127.0.0.1", r));
    const { port } = storeServer.address() as AddressInfo;
    const storeClient = new WhitewaterClient({
      apiKey: "test-key",
      endpoints: [`http://127.0.0.1:${port}`],
    });
    try {
      const profiles = storeClient.store<string, string>(
        "users.profiles",
        codecs.utf8(),
        codecs.json<string>(),
      );
      expect(await profiles.find("a@x.io")).toBe("alice");
      expect(await profiles.findBy("user_id", "u-1")).toBe("alice");
      expect(callsFor("/v1/state-stores/get-by")[0].limit).toBe(1);

      storeBodies["/v1/state-stores/lookup"] = {
        found: false,
        applied_seq: 7,
        committed_seq: 7,
      };
      storeBodies["/v1/state-stores/get-by"] = {
        rows: [],
        applied_seq: 7,
        committed_seq: 7,
      };
      expect(await profiles.find("ghost")).toBeNull();
      expect(await profiles.findBy("user_id", "ghost")).toBeNull();
      expect(await profiles.getOrDefault("ghost", "fallback")).toBe("fallback");
    } finally {
      await new Promise<void>((r) => storeServer.close(() => r()));
    }
  });
});
