import { describe, expect, it, beforeEach, afterEach } from "vitest";
import * as http from "node:http";
import { AddressInfo } from "node:net";
import { codecs, Store, WhitewaterClient } from "../src/index.js";

interface Profile {
  id: string;
  name: string;
}

/** Scripted stub covering the codec-bound Store<K,V> facade. */
describe("typed store facade", () => {
  let server: http.Server;
  let client: WhitewaterClient;
  const calls: { path: string; body: string }[] = [];

  const MUTATE_OK = {
    store_id: "00000000-0000-7000-8000-000000000007",
    mutation_id: "00000000-0000-7000-8000-000000000008",
    sequence: 4,
    deduplicated: false,
  };

  const row = (key: string, payload: unknown, extra: Record<string, unknown> = {}) => ({
    key_base64: Buffer.from(key).toString("base64"),
    payload_base64: Buffer.from(JSON.stringify(payload)).toString("base64"),
    applied_seq: 4,
    ...extra,
  });

  const route = (path: string): [number, unknown] => {
    switch (path) {
      case "/v1/state-stores/mutate":
      case "/v1/state-stores/clear":
        return [200, MUTATE_OK];
      case "/v1/state-stores/lookup":
        return [
          200,
          {
            found: true,
            payload_base64: Buffer.from(`{"id":"user-1","name":"Ada"}`).toString("base64"),
            applied_seq: 4,
            committed_seq: 5,
            applied_cursor: "41",
            served_by: "n1",
          },
        ];
      case "/v1/state-stores/get-by":
        return [
          200,
          {
            rows: [row("user-1", { id: "user-1", name: "Ada" }, { applied_cursor: "9" })],
            applied_seq: 4,
            committed_seq: 4,
          },
        ];
      case "/v1/state-stores/iterate":
        return [
          200,
          {
            rows: [row("user-1", { id: "user-1", name: "Ada" })],
            next_key_base64: null,
            pinned_seq: 4,
            served_by: "n1",
          },
        ];
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
  const decodeB64 = (value: unknown) => Buffer.from(String(value), "base64").toString("utf8");

  it("put() encodes key and payload through the bound codecs", async () => {
    const profiles = client.store("accounts.employees", codecs.json<Profile>());
    await profiles.put("user-1", { id: "user-1", name: "Ada" }, { passport: "P111" });
    const ops = bodies("/v1/state-stores/mutate").map((b) => b.op as Record<string, never>);
    expect(ops).toHaveLength(1);
    expect(ops[0].kind).toBe("put_row");
    expect(decodeB64(ops[0].key_base64)).toBe("user-1");
    expect(JSON.parse(decodeB64(ops[0].payload_base64))).toEqual({
      id: "user-1",
      name: "Ada",
    });
    const named = ops[0].named as { name: string; values: unknown[] }[];
    expect(named).toEqual([
      { name: "passport", values: [{ kind: "text", value: "P111" }] },
    ]);
  });

  it("get() decodes the row and preserves freshness", async () => {
    const profiles = client.store("accounts.employees", codecs.json<Profile>());
    const found = await profiles.get("user-1");
    expect(found?.key).toBe("user-1");
    expect(found?.value).toEqual({ id: "user-1", name: "Ada" });
    expect(found?.appliedCursor).toBe("41");
    const lookup = bodies("/v1/state-stores/lookup")[0];
    expect(decodeB64(lookup.key_base64)).toBe("user-1");
  });

  it("getBy() decodes every matched primary row", async () => {
    const profiles = client.store("accounts.employees", codecs.json<Profile>());
    const result = await profiles.getBy("passport", "P111");
    expect(result.rows).toHaveLength(1);
    expect(result.rows[0].value.name).toBe("Ada");
    expect(result.rows[0].appliedCursor).toBe("9");
  });

  it("iterate() yields decoded rows in key order", async () => {
    const profiles = client.store("accounts.employees", codecs.json<Profile>());
    const visited: string[] = [];
    for await (const r of profiles.iterate()) {
      visited.push(`${r.key}:${r.value.name}`);
    }
    expect(visited).toEqual(["user-1:Ada"]);
  });

  it("delete() encodes the key and posts delete_row", async () => {
    const profiles = client.store("accounts.employees", codecs.json<Profile>());
    await profiles.delete("user-1");
    const ops = bodies("/v1/state-stores/mutate").map((b) => b.op as Record<string, never>);
    const last = ops[ops.length - 1];
    expect(last.kind).toBe("delete_row");
    expect(decodeB64(last.key_base64)).toBe("user-1");
  });

  it("view() rebinds codecs with no wire call; store(name) defaults to UTF-8 keys + raw bytes", async () => {
    const profiles = client.store("accounts.employees", codecs.json<Profile>());
    const raw = profiles.view(codecs.utf8(), codecs.bytes());
    const found = await raw.get("user-1");
    expect(found?.value.toString("utf8")).toBe(`{"id":"user-1","name":"Ada"}`);

    const defaults = client.store("accounts.employees");
    const d = await defaults.get("user-1");
    expect(d?.key).toBe("user-1");
    expect(d?.value.toString("utf8")).toBe(`{"id":"user-1","name":"Ada"}`);
  });

  it("Store.bind types an already-bound byte-level facade", async () => {
    const bound = client.stateStore("accounts.employees");
    const profiles = Store.bind(bound, codecs.utf8(), codecs.json<Profile>());
    const found = await profiles.get("user-1");
    expect(found?.value.name).toBe("Ada");
    expect(profiles.raw()).toBe(bound);
    expect(profiles.name).toBe("accounts.employees");
  });
});
