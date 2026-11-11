import { describe, expect, it, beforeEach, afterEach } from "vitest";
import * as http from "node:http";
import { AddressInfo } from "node:net";
import { AbstractStoreKeySet, codecs, StateStore, Store, StoreKeySet, WhitewaterClient } from "../src/index.js";

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
      case "/v1/admin/commands":
        return [
          200,
          {
            request_id: "00000000-0000-7000-8000-000000000006",
            revision: 9,
            authority: "control_plane",
            warning: null,
            results: [{ statement: "s", message: "ok", data: {} }],
          },
        ];
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

  class PassportKeySet extends AbstractStoreKeySet {
    setPassport(v: string): this {
      return this.put("passport", v);
    }
    setDepartment(v: string): this {
      return this.put("department", v);
    }
    passport(v: string): this {
      return this.setPassport(v);
    }
    department(v: string): this {
      return this.setDepartment(v);
    }
    static with(passport: string, department: string): PassportKeySet {
      return new PassportKeySet().setPassport(passport).setDepartment(department);
    }
    static withPassport(passport: string): PassportKeySet {
      return new PassportKeySet().setPassport(passport);
    }
    static of(builder: (k: PassportKeySet) => void): PassportKeySet {
      const ks = new PassportKeySet();
      builder(ks);
      return ks;
    }
  }

  const lastNamed = () => {
    const ops = bodies("/v1/state-stores/mutate").map((b) => b.op as Record<string, unknown>);
    const last = ops[ops.length - 1];
    const entries = (last.named as { name: string; values: { value: unknown }[] }[]) ?? [];
    return Object.fromEntries(entries.map((e) => [e.name, e.values[0]?.value]));
  };

  it("StoreKeySet supports all inline styles and single-string fallback", async () => {
    const accounts = client.domain("accounts");
    const spec = StateStore.named("employees").addKey("passport", true).addKey("department");
    await accounts.defineStateStore(spec);
    const profiles = accounts.store("employees", codecs.json<Profile>());

    // 1A: Static factory PassportKeySet.with("P123", "eng")
    await profiles.put("emp-1", { id: "emp-1", name: "Ada" }, PassportKeySet.with("P123", "eng"));
    expect(lastNamed()).toEqual({ passport: "P123", department: "eng" });

    // 1B: Static starter + fluent chaining
    await profiles.put("emp-2", { id: "emp-2", name: "Bob" }, PassportKeySet.withPassport("P222").department("ops"));
    expect(lastNamed()).toEqual({ passport: "P222", department: "ops" });

    // 2A: Lambda on KeySet class
    await profiles.put("emp-3", { id: "emp-3", name: "Cara" }, PassportKeySet.of((k) => k.passport("P333").department("eng")));
    expect(lastNamed()).toEqual({ passport: "P333", department: "eng" });

    // 2B: Store typed with KeySet class via withKeySet
    const keyed = profiles.withKeySet(PassportKeySet);
    await keyed.put("emp-4", { id: "emp-4", name: "Dan" }, (k) => k.passport("P444").department("sales"));
    expect(lastNamed()).toEqual({ passport: "P444", department: "sales" });

    // 3: Single default string fallback -> maps to first declared key ("passport")
    await profiles.put("emp-5", { id: "emp-5", name: "Eve" }, "P555");
    expect(lastNamed()).toEqual({ passport: "P555" });

    // StoreKeySet.of static helper
    await profiles.put("emp-6", { id: "emp-6", name: "Frank" }, StoreKeySet.of("passport", "P666", "department", "qa"));
    expect(lastNamed()).toEqual({ passport: "P666", department: "qa" });
  });
});
