import { describe, expect, it, beforeEach, afterEach } from "vitest";
import * as http from "node:http";
import { AddressInfo } from "node:net";
import {
  demandCounter,
  indexBound,
  indexPredicate,
  WhitewaterClient,
} from "../src/index.js";

/** Scripted stub covering Pipe lifecycle, index queries, and demand. */
describe("operational facades", () => {
  let server: http.Server;
  let client: WhitewaterClient;
  const calls: { method: string; path: string; body: string }[] = [];

  const PIPE_ROW = {
    name: "orders.relay",
    stage: "paused",
    subscription_id: "019a0000-0000-7000-8000-000000000020",
    operation: { operation: "forward" },
    output_feed_id: "019a0000-0000-7000-8000-000000000021",
    output_store_id: null,
    status: "active",
    created_at_ns: 42,
  };

  const commandsWith = (data: unknown) => ({
    request_id: "019a0000-0000-7000-8000-000000000006",
    revision: 9,
    authority: "control_plane",
    warning: null,
    results: [{ statement: "s", message: "ok", data }],
  });

  function route(path: string, body: string): unknown {
    if (path === "/v1/admin/commands") {
      const parsed = JSON.parse(body);
      const verb = parsed.commands[0].command;
      if (verb === "describe") return commandsWith(PIPE_ROW);
      if (verb === "show") return commandsWith({ rows: [PIPE_ROW] });
      return commandsWith({});
    }
    if (path === "/v1/admin/demand/feeds") {
      return {
        feeds: [{
          feed: "orders.events", domain: "orders",
          appended_records: 1500, appended_bytes: 2048000,
          read_pages: 42,
        }],
        domains: { orders: { feeds: 1, appended_records: 1500 } },
      };
    }
    if (path === "/v1/admin/demand/hot-keys") {
      return {
        hot_keys: [{
          feed: "orders.events", share_bps: 9000,
          window_records: 900, window_bytes: 12000,
        }],
      };
    }
    if (path.startsWith("/v1/admin/demand/")) {
      const kind = path.slice(path.lastIndexOf("/") + 1);
      return { [kind]: [], domains: {} };
    }
    if (path === "/v1/indexes/query") {
      return {
        index: "crm.byregion",
        index_id: "019a0000-0000-7000-8000-000000000030",
        generation: 2,
        rows: [{
          key_base64: Buffer.from("acct-1").toString("base64"),
          payload_base64: Buffer.from('{"tier":"gold"}').toString("base64"),
        }],
        applied_cursor: "idx-cursor",
        target_cursor: "tip-cursor",
        behind: false, truncated: false,
        served_by: "node-1",
      };
    }
    return {};
  }

  beforeEach(async () => {
    calls.length = 0;
    server = http.createServer((req, res) => {
      const chunks: Buffer[] = [];
      req.on("data", (chunk) => chunks.push(chunk));
      req.on("end", () => {
        const body = Buffer.concat(chunks).toString();
        calls.push({ method: req.method ?? "", path: req.url ?? "", body });
        res.setHeader("content-type", "application/json");
        res.end(JSON.stringify(route(req.url ?? "", body)));
      });
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const port = (server.address() as AddressInfo).port;
    client = new WhitewaterClient({
      endpoints: [`http://127.0.0.1:${port}`],
      apiKey: "test-key",
    });
  });

  afterEach(async () => {
    await new Promise<void>((resolve) => server.close(() => resolve()));
  });

  // ---- Pipe lifecycle ----------------------------------------------------

  it("pipe.describe decodes the catalog row", async () => {
    const info = await client.pipe("orders.relay").describe();
    expect(info.name).toBe("orders.relay");
    expect(info.stage).toBe("paused");
    expect(info.subscriptionId).toBe("019a0000-0000-7000-8000-000000000020");
    expect(info.operation.operation).toBe("forward");
    expect(info.outputFeedId).toBe("019a0000-0000-7000-8000-000000000021");
    expect(info.outputStoreId).toBeNull();
    expect(info.createdAtNs).toBe(42n);
    const command = JSON.parse(calls[0].body).commands[0];
    expect(command).toMatchObject({ command: "describe", kind: "pipe" });
  });

  it("pipe lifecycle posts the typed verbs", async () => {
    const pipe = client.pipe("orders.relay");
    await pipe.pause();
    await pipe.resume();
    await pipe.drop();
    const verbs = calls.map((call) => JSON.parse(call.body).commands[0].command);
    expect(verbs).toEqual(["pause_pipe", "resume_pipe", "drop"]);
    expect(JSON.parse(calls[2].body).commands[0].kind).toBe("pipe");
  });

  it("pipes lists the catalog", async () => {
    const pipes = await client.pipes();
    expect(pipes).toHaveLength(1);
    expect(pipes[0].name).toBe("orders.relay");
    const command = JSON.parse(calls[0].body).commands[0];
    expect(command).toEqual({ command: "show", kind: "pipes" });
  });

  // ---- Demand observability ------------------------------------------------

  it("demandFeeds decodes rows and domain rollups", async () => {
    const report = await client.demandFeeds();
    expect(report.rows).toHaveLength(1);
    const row = report.rows[0];
    expect(row.name).toBe("orders.events");
    expect(row.domain).toBe("orders");
    expect(demandCounter(row, "appended_records")).toBe(1500);
    expect(demandCounter(row, "nonsense")).toBeNull();
    expect(report.domains.orders.feeds).toBe(1);
    expect(calls[0].path).toBe("/v1/admin/demand/feeds");
    expect(calls[0].method).toBe("GET");
  });

  it("demand endpoints cover every resource kind", async () => {
    expect((await client.demandReaders()).rows).toHaveLength(0);
    expect((await client.demandSubscriptions()).rows).toHaveLength(0);
    expect((await client.demandIndexes()).rows).toHaveLength(0);
    expect((await client.demandPipes()).rows).toHaveLength(0);
    expect(calls.map((call) => call.path)).toEqual([
      "/v1/admin/demand/readers",
      "/v1/admin/demand/subscriptions",
      "/v1/admin/demand/indexes",
      "/v1/admin/demand/pipes",
    ]);
  });

  it("hotKeys decodes dominant-key evidence", async () => {
    const keys = await client.hotKeys();
    expect(keys).toHaveLength(1);
    expect(keys[0].evidence.feed).toBe("orders.events");
    expect(keys[0].evidence.share_bps).toBe(9000);
    expect(calls[0].path).toBe("/v1/admin/demand/hot-keys");
  });

  // ---- Index queries ---------------------------------------------------------

  it("queryIndex flattens range bounds and decodes rows", async () => {
    const page = await client.queryIndex(
      "crm.byregion",
      indexPredicate.range(
        ["emea"], indexBound.inclusive(0), indexBound.exclusive(99)),
      { limit: 50, strict: true },
    );
    expect(page.behind).toBe(false);
    expect(page.generation).toBe(2);
    expect(page.rows).toHaveLength(1);
    expect(page.rows[0].key.toString()).toBe("acct-1");
    expect(page.servedBy).toBe("node-1");
    const body = JSON.parse(calls[0].body);
    expect(body.mode).toBe("range");
    expect(body.leading).toEqual([{ kind: "text", value: "emea" }]);
    expect(body.upper.inclusive).toBe(false);
    expect(body.limit).toBe(50);
    expect(body.strict).toBe(true);
    expect(calls[0].path).toBe("/v1/indexes/query");
  });

  it("queryIndex exact carries every typed kind", async () => {
    await client.queryIndex(
      "crm.byregion",
      indexPredicate.exact(["emea", 42, true, null, Buffer.from([1, 2])]),
    );
    const values = JSON.parse(calls[0].body).values;
    expect(values).toEqual([
      { kind: "text", value: "emea" },
      { kind: "i64", value: 42 },
      { kind: "bool", value: true },
      { kind: "null" },
      { kind: "bytes", value_base64: Buffer.from([1, 2]).toString("base64") },
    ]);
  });

  it("queryIndex prefix omits partial_text when absent", async () => {
    await client.queryIndex("crm.byregion", indexPredicate.prefix(["emea"]));
    expect(JSON.parse(calls[0].body).partial_text).toBeUndefined();
    await client.queryIndex("crm.byregion", indexPredicate.prefix(["emea"], "gold"));
    expect(JSON.parse(calls[1].body).partial_text).toBe("gold");
  });
});
