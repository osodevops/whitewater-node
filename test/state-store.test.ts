import { describe, expect, it, beforeEach, afterEach } from "vitest";
import * as http from "node:http";
import { AddressInfo } from "node:net";
import { Extractor, StateStore, WhitewaterClient } from "../src/index.js";

/** Scripted stub covering the StateStore facade endpoints. */
describe("state store facade", () => {
  let server: http.Server;
  let client: WhitewaterClient;
  const calls: { path: string; body: string }[] = [];

  const COMMANDS_OK = {
    request_id: "00000000-0000-7000-8000-000000000006",
    revision: 9,
    authority: "control_plane",
    results: [{ statement: "s", message: "ok", data: {} }],
  };

  const MUTATE_OK = {
    store_id: "00000000-0000-7000-8000-000000000007",
    mutation_id: "00000000-0000-7000-8000-000000000008",
    sequence: 4,
    deduplicated: false,
  };

  const ROW = (key: string, payload: string) => ({
    key_base64: Buffer.from(key).toString("base64"),
    payload_base64: Buffer.from(payload).toString("base64"),
    applied_seq: 4,
  });

  const route = (path: string): [number, unknown] => {
    switch (path) {
      case "/v1/admin/commands":
        return [200, COMMANDS_OK];
      case "/v1/state-stores/mutate":
      case "/v1/state-stores/clear":
        return [200, MUTATE_OK];
      case "/v1/state-stores/lookup":
        return [
          200,
          {
            found: true,
            payload_base64: Buffer.from("payload-1").toString("base64"),
            applied_seq: 4,
            committed_seq: 5,
            applied_cursor: "41",
            served_by: "n1",
          },
        ];
      case "/v1/state-stores/get-by":
        return [
          200,
          { rows: [ROW("user-1", "p1"), ROW("user-2", "p2")], applied_seq: 4, committed_seq: 4 },
        ];
      case "/v1/state-stores/iterate":
        return [200, { rows: [ROW("a", "pa"), ROW("b", "pb")], pinned_seq: 4, served_by: "n1" }];
      default:
        return [200, {}];
    }
  };

  beforeEach(async () => {
    calls.length = 0;
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

  const bodies = (path: string) =>
    calls.filter((c) => c.path === path).map((c) => JSON.parse(c.body) as Record<string, never>);

  it("define sends the typed command with declared named keys", async () => {
    const spec = StateStore.named("accounts.employees")
      .addKey("passport", true)
      .addKey("department", false);
    const store = await client.stateStores.define(spec);

    expect(store.name).toBe("accounts.employees");
    const sent = bodies("/v1/admin/commands")[0].commands[0];
    expect(sent.command).toBe("define_state_store");
    expect(sent.name).toBe("accounts.employees");
    expect(sent.source).toEqual({ kind: "manual" });
    expect(sent.named_keys).toEqual([
      { name: "passport", unique: true },
      { name: "department", unique: false },
    ]);
  });

  it("bounds named keys at MAX_NAMED_KEYS and rejects duplicates", () => {
    const spec = StateStore.named("accounts.ten");
    for (let i = 0; i < 10; i++) spec.addKey(`k${i}`);
    expect(() => spec.addKey("k10")).toThrow("at most 10 named keys");
    expect(() => StateStore.named("accounts.d").addKey("a").addKey("a")).toThrow("duplicate");
  });

  it("define feed-sourced named keys need extractors", async () => {
    // A feed-sourced named key without a projection is a declaration bug.
    const bare = StateStore.named("accounts.src").addKey("k");
    await expect(client.stateStores.define(bare, "accounts.events")).rejects.toThrow(
      "require an extractor",
    );
    // An extractor on a manual store is refused — puts carry the values.
    const manual = StateStore.named("accounts.m").addKey("k", false, Extractor.metadata("pp"));
    await expect(client.stateStores.define(manual)).rejects.toThrow("manual stores");
  });

  it("define feed-sourced emits extractors in the typed command", async () => {
    const spec = StateStore.named("accounts.tagged")
      .addKey("passport", true, Extractor.metadata("pp"))
      .addKey("department", false, Extractor.json("dept"));
    await client.stateStores.define(spec, "accounts.events");

    const sent = bodies("/v1/admin/commands")[0].commands[0];
    expect(sent.source).toEqual({ kind: "feed", feed: "accounts.events" });
    expect(sent.named_keys).toEqual([
      {
        name: "passport",
        unique: true,
        extractor: { kind: "metadata", name: "pp" },
      },
      {
        name: "department",
        unique: false,
        extractor: { kind: "projection", extractor: "json", version: 1, field: "dept" },
      },
    ]);
  });

  it("unbound spec refuses row operations", async () => {
    const spec = StateStore.named("accounts.unbound");
    await expect(spec.get("k")).rejects.toThrow("unbound StateStore spec");
  });

  it("put encodes typed named values and delete posts the op", async () => {
    const store = client.stateStore("accounts.employees");
    const put = await store.put("user-1", "profile-1", {
      passport: "P111",
      salary: 92000n,
      active: true,
      badge: Buffer.from([1, 2]),
      note: null,
    });
    expect(put.sequence).toBe(4);
    expect(put.deduplicated).toBe(false);

    const sent = bodies("/v1/state-stores/mutate")[0];
    expect(sent.store).toBe("accounts.employees");
    expect(sent.mutation_id).toBeTruthy();
    expect(sent.op.kind).toBe("put_row");
    expect(Buffer.from(sent.op.key_base64, "base64").toString()).toBe("user-1");
    expect(sent.op.named).toEqual([
      { name: "passport", values: [{ kind: "text", value: "P111" }] },
      { name: "salary", values: [{ kind: "i64", value: 92000 }] },
      { name: "active", values: [{ kind: "bool", value: true }] },
      { name: "badge", values: [{ kind: "bytes", value_base64: "AQI=" }] },
      { name: "note", values: [{ kind: "null" }] },
    ]);

    await store.delete("user-1");
    const del = bodies("/v1/state-stores/mutate")[1];
    expect(del.op).toEqual({
      kind: "delete_row",
      key_base64: Buffer.from("user-1").toString("base64"),
    });
  });

  it("get returns the row with freshness evidence", async () => {
    const store = client.stateStore("accounts.employees");
    const row = await store.get("user-1", { strict: true, waitMs: 1000 });
    expect(row).not.toBeNull();
    expect(row!.payload.toString()).toBe("payload-1");
    expect(row!.appliedCursor).toBe("41");
    expect(row!.freshness?.committedSeq).toBe(5);
    const sent = bodies("/v1/state-stores/lookup")[0];
    expect(sent.strict).toBe(true);
    expect(sent.wait_ms).toBe(1000);
  });

  it("getBy posts the named key lookup and decodes rows", async () => {
    const store = client.stateStore("accounts.employees");
    const result = await store.getBy("department", "eng", { limit: 50 });
    expect(result.rows.length).toBe(2);
    expect(result.rows[0].key.toString()).toBe("user-1");
    const sent = bodies("/v1/state-stores/get-by")[0];
    expect(sent.named_key).toBe("department");
    expect(sent.values).toEqual([{ kind: "text", value: "eng" }]);
    expect(sent.limit).toBe(50);
  });

  it("iterate pages until no next key", async () => {
    let pages = 0;
    server.removeAllListeners("request");
    server.on("request", (req, res) => {
      const chunks: Buffer[] = [];
      req.on("data", (c) => chunks.push(c));
      req.on("end", () => {
        const sent = JSON.parse(Buffer.concat(chunks).toString("utf8"));
        calls.push({ path: req.url ?? "", body: "" });
        const response =
          pages++ === 0
            ? { rows: [ROW("a", "pa")], next_key_base64: Buffer.from("a").toString("base64"), pinned_seq: 9 }
            : { rows: [ROW("b", "pb")], pinned_seq: 9 };
        expect(sent.limit).toBe(1);
        res.statusCode = 200;
        res.setHeader("content-type", "application/json");
        res.end(JSON.stringify(response));
      });
    });

    const store = client.stateStore("accounts.employees");
    const keys: string[] = [];
    for await (const row of store.iterate(1)) keys.push(row.key.toString());
    expect(keys).toEqual(["a", "b"]);
  });

  it("clear posts a journaled mutation and drop issues the command", async () => {
    const store = client.stateStore("accounts.employees");
    const cleared = await store.clear();
    expect(cleared.sequence).toBe(4);
    const sent = bodies("/v1/state-stores/clear")[0];
    expect(sent.store).toBe("accounts.employees");
    expect(sent.mutation_id).toBeTruthy();

    await store.drop();
    const sent2 = bodies("/v1/admin/commands").at(-1)!.commands[0];
    expect(sent2).toEqual({ command: "drop", kind: "state_store", name: "accounts.employees" });
  });
});
