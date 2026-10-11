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
 * WHITEWATER_API_KEY defaults to the local development key. A scenario
 * whose `op` this SDK cannot express fails explicitly rather than
 * silently skipping — a spec-required signal.
 */

import { randomUUID } from "node:crypto";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { WhitewaterClient } from "../src/clients.js";
import { WhitewaterError } from "../src/errors.js";
import type { Joined, MemberPage, ReaderSession, WorkLease } from "../src/models.js";
import { Cursor } from "../src/models.js";

const ENDPOINTS = process.env.WHITEWATER_ENDPOINTS;
const API_KEY = process.env.WHITEWATER_API_KEY ?? "whitewater-local-development-admin-key";
const SPEC_DIR =
  process.env.WHITEWATER_SPEC_DIR ?? new URL("../spec/conformance", import.meta.url).pathname;

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

/** Failure-message formatting that survives bigint and circular data. */
function preview(value: unknown): string {
  try {
    return JSON.stringify(
      value,
      (_, v) => (typeof v === "bigint" ? v.toString() : v),
      undefined,
      0,
    ) ?? String(value);
  } catch {
    return String(value);
  }
}

class Runner {
  bound: Record<string, unknown> = {};
  namedUuids: Record<string, string> = {};
  uniq = "u" + Date.now().toString(36) + Math.random().toString(16).slice(2, 8);
  lastError: WhitewaterError | null = null;

  constructor(private readonly client: WhitewaterClient) {}

  // ---- substitution ----

  resolveNode(token: string): unknown {
    if (token === "uniq") return this.uniq;
    if (token === "uuid") return randomUUID();
    if (token.startsWith("uuid:")) {
      const name = token.slice(5);
      return (this.namedUuids[name] ??= randomUUID());
    }
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
      if (whole) return this.resolveNode(whole[1]);
      return node.replace(/\{\{(.*?)\}\}/g, (_, t) => String(this.resolveNode(t)));
    }
    if (Array.isArray(node)) return node.map((v) => this.substitute(v));
    if (node && typeof node === "object") {
      return Object.fromEntries(
        Object.entries(node as Json).map(([k, v]) => [k, this.substitute(v)]),
      );
    }
    return node;
  }

  static pathGet(node: unknown, path: string): unknown {
    let current: unknown = node;
    for (const segment of path.split(".")) {
      const match = /^([^\[]+)\[(\d+)\]$/.exec(segment);
      if (match) {
        const field = (current as Json)?.[match[1]];
        if (!Array.isArray(field) || Number(match[2]) >= field.length) return undefined;
        current = field[Number(match[2])];
      } else {
        current = (current as Json)?.[segment];
      }
      if (current == null) return undefined;
    }
    return current;
  }

  // ---- decode helpers ----

  static bytes(node: unknown): Buffer | null {
    if (node == null) return null;
    if (typeof node === "object" && !Buffer.isBuffer(node)) {
      const obj = node as Json;
      if (typeof obj.utf8 === "string") return Buffer.from(obj.utf8, "utf-8");
      if (typeof obj.hex === "string") return Buffer.from(obj.hex, "hex");
      if (typeof obj.base64 === "string") return Buffer.from(obj.base64, "base64");
    }
    return Buffer.from(String(node), "utf-8");
  }

  metadata(node: unknown): Record<string, Buffer> {
    if (!node || typeof node !== "object") return {};
    return Object.fromEntries(
      Object.entries(node as Json).map(([k, v]) => [k, Runner.bytes(v) as Buffer]),
    );
  }

  sessionOf(args: Json): ReaderSession {
    return {
      readerId: randomUUID(),
      feedId: randomUUID(),
      sessionEpoch: Number(args.session_epoch),
      capacity: 0,
      deliveredCursor: null,
      acknowledgedCursor: null,
    };
  }

  joinedOf(args: Json): Joined {
    return {
      memberId: String(args.member_id),
      memberEpoch: Number(args.member_epoch),
      ownershipEpoch: 0,
      tick: 0,
    };
  }

  leaseOf(args: Json): WorkLease {
    const member = this.joinedOf(args);
    return {
      workId: String(args.work_id),
      memberId: member.memberId,
      memberEpoch: member.memberEpoch,
      leaseEpoch: Number(args.lease_epoch),
      expiresAtTick: 0,
    };
  }

  // ---- operations (return snake_case wire-shaped values for binding) ----

  async execute(op: string, args: Json): Promise<unknown> {
    const client = this.client;
    switch (op) {
      case "wcl": {
        const requestId = typeof args.request_id === "string" ? args.request_id : randomUUID();
        const r = await client.wcl(String(args.script), requestId);
        return {
          request_id: r.requestId,
          revision: r.revision,
          authority: r.authority,
          warning: r.warning,
          results: r.results,
        };
      }
      case "writer.append": {
        const r = await client
          .writer(String(args.writer), Number(args.session_epoch))
          .append({
            requestId: String(args.request_id),
            key: Runner.bytes(args.key),
            payload: Runner.bytes(args.payload) as Buffer,
            eventTimeNs: args.event_time_ns != null ? BigInt(String(args.event_time_ns)) : null,
            metadata: this.metadata(args.metadata),
          });
        return {
          request_id: args.request_id,
          message_id: r.messageId,
          cursor: r.cursor,
          deduplicated: r.deduplicated,
          feedback: {
            recommended_batch_count: r.feedback.recommendedBatchCount,
            hot_key: r.feedback.hotKey,
          },
        };
      }
      case "writer.append_batch": {
        const records = args.records as Json[];
        const batch = await client
          .writer(String(records[0].writer), Number(records[0].session_epoch))
          .appendBatch(
            records.map((r) => ({
              requestId: String(r.request_id),
              key: Runner.bytes(r.key),
              payload: Runner.bytes(r.payload) as Buffer,
              eventTimeNs: r.event_time_ns != null ? BigInt(String(r.event_time_ns)) : null,
              metadata: this.metadata(r.metadata),
            })),
          );
        return {
          results: batch.results.map((r) => ({
            message_id: r.messageId,
            cursor: r.cursor,
            deduplicated: r.deduplicated,
          })),
          feedback: { hot_key: batch.feedback.hotKey },
        };
      }
      case "reader.open": {
        const s = await client
          .reader(String(args.reader))
          .open(Number(args.capacity ?? 64), String(args.request_id));
        return Runner.sessionBind(s);
      }
      case "reader.fetch": {
        const p = await client
          .reader(String(args.reader))
          .fetch(
            this.sessionOf(args),
            args.limit != null ? Number(args.limit) : undefined,
            String(args.request_id),
          );
        return {
          records: p.records,
          delivered_cursor: p.deliveredCursor,
          acknowledged_cursor: p.acknowledgedCursor,
          recommended_capacity: p.recommendedCapacity,
          retry_after_ms: p.retryAfterMs,
        };
      }
      case "reader.ack": {
        const s = await client
          .reader(String(args.reader))
          .ack(this.sessionOf(args), Cursor.of(String(args.cursor))!, String(args.request_id));
        return Runner.sessionBind(s);
      }
      case "reader.close": {
        const s = await client.reader(String(args.reader)).close(this.sessionOf(args));
        return Runner.sessionBind(s);
      }
      case "member.join": {
        const j = await client
          .subscriptionMember(String(args.subscription))
          .join(String(args.member_id));
        return {
          member_id: j.memberId,
          member_epoch: j.memberEpoch,
          ownership_epoch: j.ownershipEpoch,
          tick: j.tick,
        };
      }
      case "member.claim":
      case "member.renew": {
        const member = this.joinedOf(args);
        const memberClient = client.subscriptionMember(String(args.subscription));
        const ticks = args.lease_ticks != null ? Number(args.lease_ticks) : undefined;
        const lease =
          op === "member.claim"
            ? await memberClient.claim(member, String(args.work_id), ticks)
            : await memberClient.renew(member, this.leaseOf(args), ticks);
        return {
          lease: {
            work_id: lease.workId,
            member_id: lease.memberId,
            member_epoch: lease.memberEpoch,
            lease_epoch: lease.leaseEpoch,
            expires_at_tick: lease.expiresAtTick,
          },
        };
      }
      case "member.release": {
        const member = this.joinedOf(args);
        await client
          .subscriptionMember(String(args.subscription))
          .release(member, this.leaseOf(args));
        return {};
      }
      case "member.fetch": {
        const member = this.joinedOf(args);
        const p = await client
          .subscriptionMember(String(args.subscription))
          .fetch(
            member,
            this.leaseOf(args),
            args.limit != null ? Number(args.limit) : undefined,
          );
        return {
          records: p.records,
          cursor: p.cursor,
          positions: p.positions,
          ownership_epoch: p.ownershipEpoch,
        };
      }
      case "member.ack": {
        const member = this.joinedOf(args);
        const page: MemberPage = {
          records: [],
          cursor: args.cursor != null ? Cursor.of(String(args.cursor)) : null,
          positions: Object.fromEntries(
            Object.entries((args.positions as Json) ?? {}).map(([k, v]) => [k, String(v)]),
          ),
          ownershipEpoch: 0,
        };
        await client
          .subscriptionMember(String(args.subscription))
          .ack(member, this.leaseOf(args), page);
        return {};
      }
      case "admin.commands": {
        const r = await client.commands(args.commands as Array<Record<string, unknown>>);
        return {
          request_id: r.requestId,
          revision: r.revision,
          authority: r.authority,
          warning: r.warning,
          results: r.results,
        };
      }
      case "feed.append": {
        const r = await client.feeds.append(String(args.feed), {
          requestId: String(args.request_id),
          writerSessionId: String(args.writer_session_id),
          writerEpoch: Number(args.writer_epoch),
          sequence: Number(args.sequence),
          key: Runner.bytes(args.key),
          payload: Runner.bytes(args.payload) as Buffer,
          eventTimeNs: args.event_time_ns != null ? BigInt(String(args.event_time_ns)) : null,
          metadata: this.metadata(args.metadata),
        });
        return {
          message_id: r.messageId,
          cursor: r.cursor,
          deduplicated: r.deduplicated,
          feedback: { hot_key: r.feedback.hotKey },
        };
      }
      case "reader.temporary_fetch": {
        const p = await client.feeds.temporaryFetch(String(args.feed), {
          after: args.after != null ? Cursor.of(String(args.after)) : null,
          limit: args.limit != null ? Number(args.limit) : undefined,
          newOnly: Boolean(args.new_only),
          tail: Boolean(args.tail),
          waitMs: args.wait_ms != null ? Number(args.wait_ms) : undefined,
          afterEventTimeNs:
            args.after_event_time_ns != null ? BigInt(String(args.after_event_time_ns)) : null,
        });
        return { records: p.records, next_cursor: p.nextCursor };
      }
      case "feed.records": {
        let path = "/v1/feeds/records?feed=" + String(args.feed);
        if (args.limit != null) path += "&limit=" + String(args.limit);
        return { records: await this.client.transport.get(path) };
      }
      default:
        throw new Error(`unsupported operation in scenario: ${op}`);
    }
  }

  static sessionBind(s: ReaderSession): Json {
    return {
      reader_id: s.readerId,
      feed_id: s.feedId,
      session_epoch: s.sessionEpoch,
      capacity: s.capacity,
      delivered_cursor: s.deliveredCursor,
      acknowledged_cursor: s.acknowledgedCursor,
    };
  }

  // ---- expectations ----

  checkExpect(expectation: Json, result: unknown) {
    if (expectation.error_code != null || expectation.error_status != null) {
      expect(this.lastError, `expected error ${JSON.stringify(expectation)}`).not.toBeNull();
      if (expectation.error_code != null) {
        expect(this.lastError!.code).toBe(expectation.error_code);
      }
      if (expectation.error_status != null) {
        expect(this.lastError!.status).toBe(expectation.error_status);
      }
      return;
    }
    if (this.lastError) {
      throw new Error(`step failed: ${this.lastError.message}`);
    }
    for (const [path, expected] of Object.entries(expectation)) {
      this.check(path, Runner.pathGet(result, path), expected);
    }
  }

  check(path: string, actual: unknown, expected: unknown) {
    if (typeof expected === "string") {
      if (expected === "present") {
        if (actual == null) throw new Error(`${path} expected present`);
        return;
      }
      if (expected === "absent") {
        if (actual != null) throw new Error(`${path} expected absent but was ${preview(actual)}`);
        return;
      }
      if (expected === "!=null") {
        if (actual == null) throw new Error(`${path} expected non-null`);
        return;
      }
      if (expected.startsWith("len>=")) {
        const min = Number(expected.slice(5));
        const length = Array.isArray(actual) ? actual.length : -1;
        if (length < min) {
          throw new Error(`${path} expected len>=${min} but was ${preview(actual)}`);
        }
        return;
      }
      if (expected.startsWith("==")) {
        const ref = expected.slice(2);
        const dot = ref.indexOf(".");
        expected = Runner.pathGet(this.bound[ref.slice(0, dot)], ref.slice(dot + 1));
      } else if (expected.includes("{{")) {
        expected = this.substitute(expected);
      }
    }
    // i64 nanoseconds may arrive as decimal strings or JSON numbers.
    if (typeof expected === "string" && typeof actual === "number") {
      expected = Number(expected);
    } else if (typeof expected === "number" && typeof actual === "string") {
      actual = Number(actual);
    }
    if (actual !== expected) {
      throw new Error(`${path} expected ${preview(expected)} but was ${preview(actual)}`);
    }
  }

  // ---- driver ----

  async run(fixturePath: string) {
    const scenario = JSON.parse(readFileSync(fixturePath, "utf-8")) as {
      steps: { op: string; args?: Json; bind?: string; expect?: Json }[];
    };
    for (const [index, step] of scenario.steps.entries()) {
      const args = (this.substitute(step.args ?? {}) ?? {}) as Json;
      this.lastError = null;
      let result: unknown = null;
      try {
        result = await this.execute(step.op, args);
      } catch (error) {
        if (error instanceof WhitewaterError) {
          this.lastError = error;
        } else {
          throw error;
        }
      }
      if (this.lastError !== null && !step.expect) {
        // An unhandled step failure is a scenario failure — surface it
        // rather than letting downstream tokens resolve against a null
        // binding and failing confusingly.
        throw new Error(`step ${index} (${step.op}) failed: ${this.lastError.message}`);
      }
      if (step.bind) this.bound[step.bind] = result;
      if (step.expect) {
        try {
          this.checkExpect(step.expect, result);
        } catch (error) {
          throw new Error(`step ${index} (${step.op}): ${(error as Error).message}`);
        }
      }
    }
  }
}

describe.skipIf(!ENABLED)("conformance", () => {
  for (const fixture of FIXTURES) {
    it(fixture.split(/[\\/]/).pop()!, async () => {
      const scenario = JSON.parse(readFileSync(fixture, "utf-8")) as { api_key?: string };
      const client = new WhitewaterClient({
        apiKey: scenario.api_key ?? API_KEY,
        endpoints: (ENDPOINTS as string).split(",").map((e) => e.trim()),
      });
      expect(await client.healthy()).toBe(true);
      await new Runner(client).run(fixture);
    });
  }
});
