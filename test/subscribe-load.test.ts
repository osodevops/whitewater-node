import { describe, expect, it, beforeEach, afterEach } from "vitest";
import * as http from "node:http";
import { AddressInfo } from "node:net";
import {
  FeedRecord,
  ReaderStart,
  StaleSession,
  WhitewaterClient,
} from "../src/index.js";

/** Scripted multi-path stub covering subscribe()'s session loop and load()'s paging. */
describe("subscribe + load", () => {
  let server: http.Server;
  let client: WhitewaterClient;
  const calls: { path: string; body: string }[] = [];
  const fetchPages: string[] = [];
  const temporaryPages: string[] = [];

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

  const record = (cursor: string) => ({
    cursor,
    message_id: "00000000-0000-7000-8000-000000000002",
    producer_id: "00000000-0000-7000-8000-000000000003",
    sequence: 7,
    event_time_ns: "1697040000123456789",
    ingest_time_ns: "1697040000223456789",
    key_base64: Buffer.from("k-" + cursor).toString("base64"),
    payload_base64: Buffer.from("val").toString("base64"),
    metadata_base64: {},
  });

  const page = (delivered: string | null, ...cursors: string[]) =>
    JSON.stringify({
      records: cursors.map(record),
      delivered_cursor: delivered,
      acknowledged_cursor: null,
      recommended_capacity: 0,
      retry_after_ms: 0,
    });

  const temporaryPage = (next: string | null, ...cursors: string[]) =>
    JSON.stringify({ records: cursors.map(record), next_cursor: next });

  const route = (path: string): [number, unknown] => {
    switch (path) {
      case "/v1/admin/wcl":
        return [200, WCL_OK];
      case "/v1/readers/open":
      case "/v1/readers/ack":
      case "/v1/readers/close":
        return [200, SESSION];
      case "/v1/readers/fetch":
        return [200, JSON.parse(fetchPages.shift() ?? page(null))];
      case "/v1/readers/temporary/fetch":
        return [200, JSON.parse(temporaryPages.shift() ?? temporaryPage(null))];
      default:
        return [200, {}];
    }
  };

  beforeEach(async () => {
    calls.length = 0;
    fetchPages.length = 0;
    temporaryPages.length = 0;
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
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const port = (server.address() as AddressInfo).port;
    client = new WhitewaterClient({
      apiKey: "test-key",
      endpoints: [`http://127.0.0.1:${port}`],
    });
  });

  afterEach(() => server.close());

  const temporaryBodies = () =>
    calls
      .filter((c) => c.path === "/v1/readers/temporary/fetch")
      .map((c) => JSON.parse(c.body) as Record<string, unknown>);

  const waitFor = async (predicate: () => boolean, ms = 5_000) => {
    const deadline = Date.now() + ms;
    while (!predicate() && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 25));
    }
    expect(predicate()).toBe(true);
  };

  it("load pages until empty and delivers every record", async () => {
    temporaryPages.push(temporaryPage("c1", "r1"), temporaryPage("c2", "r2"));

    const sink: FeedRecord[][] = [];
    const loaded = await client.feeds.load(
      "hr.employees",
      ReaderStart.beginning(),
      (records) => sink.push(records),
    );

    expect(loaded).toBe(2);
    expect(sink.flat().map((r) => r.cursor)).toEqual(["r1", "r2"]);
    const bodies = temporaryBodies();
    expect(bodies.length).toBe(3);
    expect(bodies[0].after).toBeUndefined();
    expect(bodies[1].after).toBe("c1");
  });

  it("load seeks by event time then continues on the cursor", async () => {
    temporaryPages.push(temporaryPage("c1", "r1"), temporaryPage(null));

    const sink: FeedRecord[][] = [];
    await client.feeds.load(
      "hr.employees",
      ReaderStart.timestampNs(1697040000000000000n),
      (records) => sink.push(records),
    );

    const bodies = temporaryBodies();
    expect(bodies[0].after_event_time_ns).toBe("1697040000000000000");
    expect(bodies[0].after).toBeUndefined();
    expect(bodies[1].after).toBe("c1");
    expect(sink.length).toBe(1);
  });

  it("subscribe opens, delivers, acks, and closes", async () => {
    fetchPages.push(page("d1", "r1"));
    const batches: FeedRecord[][] = [];
    let resolveDelivery!: () => void;
    const delivered = new Promise<void>((resolve) => {
      resolveDelivery = resolve;
    });
    const sub = client.subscribe(
      "hr.view",
      "hr.employees",
      ReaderStart.beginning(),
      (records) => {
        batches.push(records);
        resolveDelivery();
      },
    );
    await delivered;
    await waitFor(() => calls.some((c) => c.path === "/v1/readers/ack"));
    await sub.close();

    expect(batches[0].length).toBe(1);
    expect(calls.filter((c) => c.path === "/v1/admin/wcl").length).toBe(1);
    const ack = calls.find((c) => c.path === "/v1/readers/ack");
    expect(JSON.parse(ack!.body).cursor).toBe("d1");
    expect(calls.some((c) => c.path === "/v1/readers/close")).toBe(true);
    expect(sub.running).toBe(false);
  });

  it("subscribe reuses an existing reader and reports stale sessions", async () => {
    const errors: Error[] = [];
    const stale = new Promise<void>((resolve) => {
      const refusing = http.createServer((req, res) => {
        const path = (req.url ?? "").split("?")[0];
        let status = 200;
        let response: unknown = {};
        if (path === "/v1/admin/wcl") {
          status = 400;
          response = { error: "Reader hr.view already exists" };
        } else if (path === "/v1/readers/open") {
          status = 400;
          response = { error: "stale session epoch", code: "stale_epoch" };
        } else {
          response = SESSION;
        }
        res.statusCode = status;
        res.end(JSON.stringify(response));
        void req.resume();
      });
      void refusing.listen(0, "127.0.0.1", async () => {
        const port = (refusing.address() as AddressInfo).port;
        const c = new WhitewaterClient({
          apiKey: "test-key",
          endpoints: [`http://127.0.0.1:${port}`],
        });
        const sub = c.subscribe("hr.view", "hr.employees", ReaderStart.now(), () => {},
          (error) => {
            errors.push(error);
            resolve();
            void sub.close().then(() => refusing.close());
          });
      });
    });
    await stale;
    expect(errors[0]).toBeInstanceOf(StaleSession);
  });

});
