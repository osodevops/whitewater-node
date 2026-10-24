/**
 * Shared-corpus conformance runner.
 *
 * Executes every scenario in `sdk/spec/conformance/*.json` against a
 * real three-Node Riverbed. Enabled when both environment variables are
 * set:
 *
 *   WHITEWATER_ENDPOINTS=http://localhost:7071,...
 *   WHITEWATER_SPEC_DIR=.../whitewater/sdk/spec/conformance
 *
 * `WHITEWATER_API_KEY` defaults to the local development key.
 * `WHITEWATER_CHAOS_CMD` names the environment's fault driver; without
 * it `chaos.*` steps report unsupported rather than skipping. A
 * scenario whose `op` this SDK cannot express fails explicitly rather
 * than silently skipping — a spec-required signal.
 *
 * Steps dispatch at the wire surface so `expect` asserts the same
 * (status, body) contract every language runner implements: poll
 * windows re-dispatch asynchronous steps until expectations settle,
 * while a separate transient budget rides out transport errors and 5xx
 * unavailability without spending poll attempts.
 *
 * Record nanoseconds stay decimal strings or bigint — never imprecise
 * JavaScript numbers.
 */

import { execSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

const ENDPOINTS = process.env.WHITEWATER_ENDPOINTS;
const API_KEY = process.env.WHITEWATER_API_KEY ?? "whitewater-local-development-admin-key";
const SPEC_DIR =
  process.env.WHITEWATER_SPEC_DIR ?? new URL("../spec/conformance", import.meta.url).pathname;
const CHAOS_CMD = process.env.WHITEWATER_CHAOS_CMD ?? "";

const FIXTURES = (() => {
  try {
    return readdirSync(SPEC_DIR)
      .filter((f) => f.endsWith(".json"))
      .sort()
      .map((f) => join(SPEC_DIR, f));
  } catch {
    return [] as string[];
  }
})();

const ENABLED = Boolean(ENDPOINTS) && FIXTURES.length > 0;

type Json = Record<string, unknown>;

/** The shared op vocabulary onto the wire surface. */
const WIRE_ENDPOINTS: Record<string, [string, string]> = {
  wcl: ["POST", "/v1/admin/wcl"],
  "admin.wcl": ["POST", "/v1/admin/wcl"],
  "admin.commands": ["POST", "/v1/admin/commands"],
  "writer.append": ["POST", "/v1/writers/append"],
  "writer.append_batch": ["POST", "/v1/writers/append-batch"],
  "reader.open": ["POST", "/v1/readers/open"],
  "reader.fetch": ["POST", "/v1/readers/fetch"],
  "reader.ack": ["POST", "/v1/readers/ack"],
  "reader.close": ["POST", "/v1/readers/close"],
  "reader.temporary_fetch": ["POST", "/v1/readers/temporary/fetch"],
  "feed.append": ["POST", "/v1/feeds/append"],
  "member.join": ["POST", "/v1/subscriptions/members/join"],
  "member.claim": ["POST", "/v1/subscriptions/members/claim"],
  "member.renew": ["POST", "/v1/subscriptions/members/renew"],
  "member.release": ["POST", "/v1/subscriptions/members/release"],
  "member.transfer": ["POST", "/v1/subscriptions/members/transfer"],
  "member.fetch": ["POST", "/v1/subscriptions/members/fetch"],
  "member.ack": ["POST", "/v1/subscriptions/members/ack"],
  "subscription.seek": ["POST", "/v1/subscriptions/seek"],
  "statestore.lookup": ["POST", "/v1/state-stores/lookup"],
  "statestore.get_by": ["POST", "/v1/state-stores/get-by"],
  "statestore.iterate": ["POST", "/v1/state-stores/iterate"],
  "statestore.clear": ["POST", "/v1/state-stores/clear"],
};

const TRANSIENT_ATTEMPTS = 120;
const TRANSIENT_INTERVAL_MS = 500;

class TransportError extends Error {}

/** Structural equality for arrays/objects — fixture expectations like
 * `rows: []` compare values, not references. */
function deepEqual(a: unknown, b: unknown): boolean {
  if (a === b) return true;
  if (typeof a === "bigint") a = a.toString();
  if (typeof b === "bigint") b = b.toString();
  if (a === b) return true;
  if (Array.isArray(a) && Array.isArray(b)) {
    return a.length === b.length && a.every((v, i) => deepEqual(v, b[i]));
  }
  if (
    a !== null &&
    b !== null &&
    typeof a === "object" &&
    typeof b === "object" &&
    !Array.isArray(a) &&
    !Array.isArray(b)
  ) {
    const ak = Object.keys(a as Json);
    const bk = Object.keys(b as Json);
    return (
      ak.length === bk.length && ak.every((k) => deepEqual((a as Json)[k], (b as Json)[k]))
    );
  }
  return false;
}

/** Failure-message formatting that survives bigint and circular data. */
function preview(value: unknown): string {
  try {
    return (
      JSON.stringify(value, (_, v) => (typeof v === "bigint" ? v.toString() : v)) ??
      String(value)
    );
  } catch {
    return String(value);
  }
}

/** Status-preserving JSON-over-HTTP calls; non-2xx replies return the
 * parsed body so `expect` asserts error_status/error_code against it. */
class Wire {
  private next = 0;
  constructor(
    private readonly endpoints: string[],
    private readonly apiKey: string,
  ) {}

  async request(
    method: string,
    path: string,
    body: unknown,
  ): Promise<{ status: number; body: unknown }> {
    const url = this.endpoints[this.next] + path;
    this.next = (this.next + 1) % this.endpoints.length;
    let response: Response;
    try {
      response = await fetch(url, {
        method,
        headers: {
          authorization: `Bearer ${this.apiKey}`,
          accept: "application/json",
          "content-type": "application/json",
        },
        body: body === undefined || body === null ? null : JSON.stringify(body),
      });
    } catch (error) {
      throw new TransportError(`Node unreachable: ${(error as Error).message}`);
    }
    const text = await response.text();
    let parsed: unknown = null;
    if (text.length > 0) {
      try {
        parsed = JSON.parse(text);
      } catch {
        parsed = null;
      }
    }
    return { status: response.status, body: parsed };
  }
}

/** Decode {"utf8": ..} / {"hex": ..} / {"base64": ..} wrappers; a plain
 * string is already utf8 bytes. */
function rawBytes(node: unknown): Buffer {
  if (node == null) return Buffer.alloc(0);
  if (typeof node === "object" && !Buffer.isBuffer(node)) {
    const obj = node as Json;
    if (typeof obj.utf8 === "string") return Buffer.from(obj.utf8, "utf-8");
    if (typeof obj.hex === "string") return Buffer.from(obj.hex, "hex");
    if (typeof obj.base64 === "string") return Buffer.from(obj.base64, "base64");
  }
  return Buffer.from(String(node), "utf-8");
}

function wireFields(row: Json): Json {
  for (const [raw, encoded] of [
    ["key", "key_base64"],
    ["payload", "payload_base64"],
    ["after_key", "after_key_base64"],
  ] as const) {
    if (raw in row) {
      row[encoded] = rawBytes(row[raw]).toString("base64");
      delete row[raw];
    }
  }
  if ("metadata" in row) {
    const metadata = row.metadata as Json | undefined;
    const encoded: Json = {};
    for (const [name, value] of Object.entries(metadata ?? {})) {
      encoded[name] = rawBytes(value).toString("base64");
    }
    row.metadata_base64 = encoded;
    delete row.metadata;
  }
  for (const field of ["event_time_ns", "after_event_time_ns", "timestamp_ns"]) {
    const value = row[field];
    if (typeof value === "number" || typeof value === "bigint") row[field] = String(value);
  }
  return row;
}

/** Lower fixture vocabulary onto the wire: raw objects become
 * *_base64 and nanosecond numbers become decimal strings. */
function wireArgs(args: unknown): unknown {
  if (!args || typeof args !== "object" || Array.isArray(args)) return args;
  const out = { ...(args as Json) };
  if (Array.isArray(out.records)) {
    out.records = out.records.map((r) =>
      r && typeof r === "object" && !Array.isArray(r) ? wireFields({ ...(r as Json) }) : r,
    );
  }
  return wireFields(out);
}

class Runner {
  bound: Record<string, unknown> = {};
  namedUuids: Record<string, string> = {};
  uniq = "u" + Date.now().toString(36) + Math.random().toString(16).slice(2, 8);

  constructor(
    private readonly wire: Wire,
    private readonly chaosCmd: string,
  ) {}

  // ---- substitution ----

  static nowNs(token: string): string {
    // Wall-clock epoch nanoseconds as a decimal string (bigint, never a
    // float); an optional signed offset shifts the instant.
    let shift = 0n;
    if (token.startsWith("now_ns:")) shift = BigInt(token.slice(7));
    return (BigInt(Date.now()) * 1_000_000n + shift).toString();
  }

  resolveNode(token: string): unknown {
    token = token.trim();
    if (token === "uniq") return this.uniq;
    if (token === "uuid") return randomUUID();
    if (token.startsWith("uuid:")) {
      const name = token.slice(5);
      return (this.namedUuids[name] ??= randomUUID());
    }
    if (token === "now_ns" || token.startsWith("now_ns:")) return Runner.nowNs(token);
    const dot = token.indexOf(".");
    if (dot > 0) {
      const bound = this.bound[token.slice(0, dot)];
      if (bound != null) {
        const value = Runner.pathGet(bound, token.slice(dot + 1));
        if (value != null) return value;
      }
    }
    throw new Error(`unresolved fixture token {{${token}}}`);
  }

  substitute(node: unknown): unknown {
    if (typeof node === "string") {
      const whole = /^\{\{(.*?)\}\}$/.exec(node);
      if (whole) {
        try {
          return this.resolveNode(whole[1]);
        } catch {
          return "null";
        }
      }
      return node.replace(/\{\{(.*?)\}\}/g, (_, token: string) => {
        try {
          return String(this.resolveNode(token));
        } catch {
          return "null";
        }
      });
    }
    if (Array.isArray(node)) return node.map((v) => this.substitute(v));
    if (node && typeof node === "object") {
      return Object.fromEntries(
        Object.entries(node as Json).map(([k, v]) => [k, this.substitute(v)]),
      );
    }
    return node;
  }

  static splitPath(path: string): string[] {
    const segments: string[] = [];
    let depth = 0;
    let start = 0;
    for (let i = 0; i < path.length; i++) {
      const char = path[i];
      if (char === "[") depth += 1;
      else if (char === "]") depth = Math.max(0, depth - 1);
      else if (char === "." && depth === 0) {
        segments.push(path.slice(start, i));
        start = i + 1;
      }
    }
    segments.push(path.slice(start));
    return segments;
  }

  static pathGet(node: unknown, path: string): unknown {
    let current: unknown = node;
    for (const segment of Runner.splitPath(path)) {
      if (current == null) return undefined;
      let name = segment;
      const indices: string[] = [];
      const bracket = segment.indexOf("[");
      if (bracket >= 0) {
        name = segment.slice(0, bracket);
        let rest = segment.slice(bracket);
        while (rest.startsWith("[")) {
          const close = rest.indexOf("]");
          if (close < 0) return undefined;
          indices.push(rest.slice(1, close));
          rest = rest.slice(close + 1);
        }
      }
      if (name.length > 0) {
        if (typeof current !== "object" || Array.isArray(current)) return undefined;
        current = (current as Json)[name];
      }
      for (const index of indices) {
        if (index.length >= 2 && index.startsWith('"') && index.endsWith('"')) {
          if (typeof current !== "object" || current === null || Array.isArray(current))
            return undefined;
          current = (current as Json)[index.slice(1, -1)];
          continue;
        }
        const n = Number(index);
        if (!Array.isArray(current) || !Number.isInteger(n) || n >= current.length)
          return undefined;
        current = current[n];
      }
    }
    return current;
  }

  // ---- dispatch ----

  async dispatch(op: string, args: unknown): Promise<{ status: number; body: unknown }> {
    if (op === "health") return this.wire.request("GET", "/health", undefined);
    if (op === "feed.records") {
      const a = (args ?? {}) as Json;
      let path = "/v1/feeds/records?feed=" + encodeURIComponent(String(a.feed ?? ""));
      if (a.limit != null) path += "&limit=" + String(a.limit);
      if (a.after != null) path += "&after=" + encodeURIComponent(String(a.after));
      const reply = await this.wire.request("GET", path, undefined);
      if (Array.isArray(reply.body)) reply.body = { records: reply.body };
      return reply;
    }
    if (op === "statestore.mutate") {
      // Flat {store, mutation_id, kind, key, payload, named} lowers to
      // {store, mutation_id, op: {kind, key_base64?, ...}}.
      const body = wireArgs(args) as Json;
      const kind = body.kind;
      if (typeof kind !== "string" || kind.length === 0)
        throw new Error("statestore.mutate needs a `kind`");
      delete body.kind;
      const operation: Json = { kind };
      for (const name of ["key_base64", "payload_base64", "named"]) {
        if (name in body) {
          operation[name] = body[name];
          delete body[name];
        }
      }
      body.op = operation;
      return this.wire.request("POST", "/v1/state-stores/mutate", body);
    }
    const target = WIRE_ENDPOINTS[op];
    if (!target) throw new Error(`unsupported op ${op}`);
    return this.wire.request(target[0], target[1], wireArgs(args));
  }

  runChaos(op: string, args: unknown) {
    if (!this.chaosCmd) {
      throw new Error(`unsupported: ${op} needs a WHITEWATER_CHAOS_CMD hook`);
    }
    const rendered = (this.substitute(args ?? {}) ?? {}) as Json;
    let action = op;
    for (const key of Object.keys(rendered).sort()) action += " " + String(rendered[key]);
    try {
      execSync(`${this.chaosCmd} ${action}`, { stdio: "pipe" });
    } catch (error) {
      throw new Error(`chaos hook failed: ${(error as Error).message}`);
    }
  }

  // ---- expectations ----

  checkExpect(status: number, body: unknown, expectation: Json) {
    for (const [path, expected] of Object.entries(expectation)) {
      if (path === "error_status") {
        if (status !== Number(expected))
          throw new Error(`expected status ${String(expected)}, got ${status}: ${preview(body)}`);
        continue;
      }
      if (path === "error_code") {
        const code = (body as Json)?.code;
        if (code !== expected)
          throw new Error(`expected code ${preview(expected)}, got body ${preview(body)}`);
        continue;
      }
      this.check(path, Runner.pathGet(body, path), expected, body);
    }
  }

  check(path: string, actual: unknown, expected: unknown, body: unknown) {
    if (typeof expected === "string") {
      if (expected === "present" || expected === "!=null") {
        if (actual == null) throw new Error(`${path} absent in ${preview(body)}`);
        return;
      }
      if (expected === "absent") {
        if (actual != null) throw new Error(`${path} present: ${preview(actual)}`);
        return;
      }
      if (expected.startsWith("len>=")) {
        const minimum = Number(expected.slice(5));
        const length =
          Array.isArray(actual) || typeof actual === "string" ? actual.length : -1;
        if (length < minimum)
          throw new Error(`${path} len ${length} < ${minimum} in ${preview(body)}`);
        return;
      }
      if (expected.startsWith("==")) {
        const reference = expected.slice(2);
        const dot = reference.indexOf(".");
        const bound = dot >= 0 ? this.bound[reference.slice(0, dot)] : this.bound[reference];
        expected = dot >= 0 ? Runner.pathGet(bound, reference.slice(dot + 1)) : bound;
        if (!deepEqual(actual, expected))
          throw new Error(`${path} = ${preview(actual)}, expected ${preview(expected)}`);
        return;
      }
    }
    if (expected && typeof expected === "object" && !Array.isArray(expected)) {
      const wrapper = expected as Json;
      if (wrapper.base64_json != null) {
        // Decode the actual base64 string as JSON, then apply each named
        // expectation to the decoded document.
        let decoded: unknown = null;
        if (typeof actual === "string") {
          try {
            decoded = JSON.parse(Buffer.from(actual, "base64").toString("utf-8"));
          } catch {
            decoded = null;
          }
        }
        if (decoded == null)
          throw new Error(`${path} is not base64 JSON: ${preview(actual)}`);
        try {
          this.checkExpect(0, decoded, wrapper.base64_json as Json);
        } catch (error) {
          throw new Error(`${path} decoded: ${(error as Error).message}`);
        }
        return;
      }
    }
    // i64 nanoseconds may arrive as decimal strings or JSON numbers.
    if (typeof expected === "string" && typeof actual === "number") {
      const numeric = Number(expected);
      if (!Number.isNaN(numeric)) expected = numeric;
    } else if (typeof expected === "number" && typeof actual === "string") {
      const numeric = Number(actual);
      if (!Number.isNaN(numeric)) actual = numeric;
    }
    if (!deepEqual(actual, expected))
      throw new Error(`${path} = ${preview(actual)}, expected ${preview(expected)}`);
  }

  // ---- anonymous-uuid rebinding ----

  static bindAnonymousUuids(node: unknown, step: number, ordinal: number[]): unknown {
    if (typeof node === "string") {
      let out = node;
      while (out.includes("{{uuid}}")) {
        out = out.replace("{{uuid}}", `{{uuid:anon-${step}-${ordinal[0]}}}`);
        ordinal[0] += 1;
      }
      return out;
    }
    if (Array.isArray(node))
      return node.map((v) => Runner.bindAnonymousUuids(v, step, ordinal));
    if (node && typeof node === "object") {
      return Object.fromEntries(
        Object.entries(node as Json).map(([k, v]) => [
          k,
          Runner.bindAnonymousUuids(v, step, ordinal),
        ]),
      );
    }
    return node;
  }

  // ---- driver ----

  async run(fixturePath: string) {
    const scenario = JSON.parse(readFileSync(fixturePath, "utf-8")) as {
      steps: { op: string; args?: unknown; bind?: string; expect?: Json; poll?: Json }[];
    };
    for (const [index, step] of scenario.steps.entries()) {
      const op = step.op;

      if (op.startsWith("chaos.")) {
        try {
          this.runChaos(op, step.args ?? {});
        } catch (error) {
          throw new Error(`step ${index} (${op}): ${(error as Error).message}`);
        }
        continue;
      }

      const isSpecial = op === "feed.records" || op === "health" || op === "statestore.mutate";
      if (WIRE_ENDPOINTS[op] == null && !isSpecial) {
        throw new Error(`step ${index}: unsupported op ${op}`);
      }

      const argsTemplate = Runner.bindAnonymousUuids(step.args ?? {}, index, [0]);
      const poll = (step.poll ?? {}) as Json;
      const pollTimes = Math.min(Math.max(Number(poll.times ?? 1), 1), 30);
      const pollIntervalMs = Math.min(Number(poll.interval_ms ?? 500) || 500, 10_000);

      let attempt = 0;
      let transient = 0;
      let status = 0;
      let body: unknown = null;
      for (;;) {
        const args = this.substitute(argsTemplate);
        try {
          ({ status, body } = await this.dispatch(op, args));
        } catch (error) {
          if (error instanceof TransportError && transient < TRANSIENT_ATTEMPTS) {
            transient += 1;
            await new Promise((resolve) => setTimeout(resolve, TRANSIENT_INTERVAL_MS));
            continue;
          }
          throw new Error(`step ${index} (${op}): ${(error as Error).message}`);
        }
        if (status >= 500 && transient < TRANSIENT_ATTEMPTS) {
          transient += 1;
          await new Promise((resolve) => setTimeout(resolve, TRANSIENT_INTERVAL_MS));
          continue;
        }
        attempt += 1;
        if (step.expect != null) {
          const substituted = this.substitute(step.expect) as Json;
          try {
            this.checkExpect(status, body, substituted);
          } catch (error) {
            if (attempt < pollTimes) {
              await new Promise((resolve) => setTimeout(resolve, pollIntervalMs));
              continue;
            }
            throw new Error(
              `step ${index} (${op}) expectation failed (status ${status}): ${(error as Error).message}`,
            );
          }
        }
        break;
      }
      if (step.bind) this.bound[step.bind] = body;
    }
  }
}

describe.skipIf(!ENABLED)("conformance", () => {
  for (const fixture of FIXTURES) {
    // Chaos fixtures drive multi-Node outages and majority re-gates —
    // they legitimately outlast vitest's 60s default.
    it(fixture.split(/[\\/]/).pop()!, { timeout: 300_000 }, async () => {
      const scenario = JSON.parse(readFileSync(fixture, "utf-8")) as { api_key?: string };
      const wire = new Wire(
        (ENDPOINTS as string).split(",").map((e) => e.trim()),
        scenario.api_key ?? API_KEY,
      );
      await new Runner(wire, CHAOS_CMD).run(fixture);
    });
  }
});
