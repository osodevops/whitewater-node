import { describe, expect, it } from "vitest";
import { StateStore, WhitewaterClient } from "../src/index.js";

/**
 * Live store smoke against a running Riverbed — declares a named-key
 * StateStore on the Nodes, then exercises put/get/getBy/iterate/delete/
 * clear end to end.
 *
 * Runs only when WHITEWATER_ENDPOINTS is set (comma-separated), e.g.
 * `http://localhost:7371,http://localhost:7372,http://localhost:7373`;
 * WHITEWATER_API_KEY defaults to the documented development key.
 */
const ENDPOINTS = process.env.WHITEWATER_ENDPOINTS;
const LIVE = Boolean(ENDPOINTS);

describe.skipIf(!LIVE)("live store", () => {
  it("declares and exercises a server-side StateStore", async () => {
    const endpoints = (ENDPOINTS as string)
      .split(/[;,]/)
      .map((e) => e.trim())
      .filter(Boolean)
      .map((e) => (e.startsWith("http") ? e : `http://${e}`));
    const client = new WhitewaterClient({
      apiKey: process.env.WHITEWATER_API_KEY ?? "whitewater-local-development-admin-key",
      endpoints,
    });

    const run = `n${(Date.now() & 0x7fffffff).toString(36)}`;
    const domain = `smoke.${run}`;
    expect(await client.healthy()).toBe(true);
    await client.createDomain(domain);

    const spec = StateStore.named(`${domain}.employees`)
      .addKey("passport", true)
      .addKey("department", false);
    const store = await client.stateStores.define(spec);
    expect(store.name).toBe(`${domain}.employees`);

    await store.put("emp-1", "alice", { passport: "P111", department: "eng" });
    await store.put("emp-2", "bob", { passport: "P222", department: "eng" });
    await store.put("emp-3", "cara", { passport: "P333", department: "ops" });

    const row = await store.get("emp-1", { strict: true });
    expect(row?.payload.toString("utf8")).toBe("alice");

    const eng = await store.getBy("department", "eng", { strict: true });
    expect(eng.rows.map((r) => r.key.toString("utf8")).sort()).toEqual(["emp-1", "emp-2"]);
    expect(eng.freshness.appliedSeq).toBeGreaterThan(0);

    const keys: string[] = [];
    for await (const r of store.iterate(1)) keys.push(r.key.toString("utf8"));
    expect(keys.sort()).toEqual(["emp-1", "emp-2", "emp-3"]);

    await store.delete("emp-1");
    expect(await store.get("emp-1", { strict: true })).toBeNull();
    expect((await store.getBy("department", "eng", { strict: true })).rows.length).toBe(1);

    await store.clear();
    const page = await store.page();
    expect(page.rows.length).toBe(0);
  });
});
