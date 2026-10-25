import { describe, expect, it, beforeEach, afterEach } from "vitest";
import * as http from "node:http";
import { AddressInfo } from "node:net";
import {
  Extractor,
  PipeOperation,
  Time,
  WhitewaterClient,
  WindowAggregation,
} from "../src/index.js";

/**
 * The typed `define_pipe` surface: PipeOperation builders must serialize
 * to exactly the wire contract — internally tagged on `operation`, u64
 * fields as strict JSON numbers (never decimal strings) — and the three
 * client methods must post exactly one `define_pipe` command while
 * failing locally on kind mismatches and bad names.
 */
describe("PipeOperation builders", () => {
  it("forward serializes to the bare tag", () => {
    expect(PipeOperation.forward().toWire()).toEqual({ operation: "forward" });
    expect(PipeOperation.forward().operation).toBe("forward");
  });

  it("enrich defaults to latest freshness and forward-on-miss", () => {
    expect(PipeOperation.enrich(Extractor.recordKey()).toWire()).toEqual({
      operation: "enrich",
      key: { kind: "key" },
      freshness: "latest",
      on_miss: "forward",
    });
  });

  it("enrich carries every option and every extractor kind", () => {
    expect(
      PipeOperation.enrich(Extractor.metadata("email"), {
        freshness: "strict",
        named: "by_email",
        onMiss: "quarantine",
      }).toWire(),
    ).toEqual({
      operation: "enrich",
      key: { kind: "metadata", name: "email" },
      freshness: "strict",
      named: "by_email",
      on_miss: "quarantine",
    });
    expect(
      PipeOperation.enrich(Extractor.json("user.id"), { onMiss: "drop" }).toWire(),
    ).toEqual({
      operation: "enrich",
      key: { kind: "projection", extractor: "json", version: 1, field: "user.id" },
      freshness: "latest",
      on_miss: "drop",
    });
  });

  it("enrich rejects a malformed named key", () => {
    expect(() =>
      PipeOperation.enrich(Extractor.recordKey(), { named: "By-Email" }),
    ).toThrow("invalid named key");
  });

  it("window serializes strict u64 numbers, never strings", () => {
    const wire = PipeOperation.window(
      Time.s(8),
      Extractor.json("customer"),
      WindowAggregation.sum(Extractor.json("amount")),
    ).toWire();
    expect(wire).toEqual({
      operation: "window",
      window_ns: 8_000_000_000,
      key: { kind: "projection", extractor: "json", version: 1, field: "customer" },
      aggregation: {
        aggregation: "sum",
        value: { kind: "projection", extractor: "json", version: 1, field: "amount" },
      },
      time: "event",
    });
    expect(typeof wire.window_ns).toBe("number");
  });

  it("window carries the lateness, retention, and bound options", () => {
    expect(
      PipeOperation.window(
        Time.m(1),
        Extractor.recordKey(),
        WindowAggregation.average(Extractor.metadata("latency")),
        {
          time: "ingest",
          expire: Time.d(1),
          keyLimit: 512,
          byteLimit: 262_144,
        },
      ).toWire(),
    ).toEqual({
      operation: "window",
      window_ns: 60_000_000_000,
      key: { kind: "key" },
      aggregation: {
        aggregation: "average",
        value: { kind: "metadata", name: "latency" },
      },
      time: "ingest",
      expire_ns: 86_400_000_000_000,
      key_limit: 512,
      byte_limit: 262_144,
    });
    expect(
      PipeOperation.window(Time.us(1), Extractor.recordKey(), WindowAggregation.count(), {
        watermark: Time.us(5),
        grace: Time.us(2),
      }).toWire(),
    ).toMatchObject({
      watermark_ns: 5_000,
      grace_ns: 2_000,
      aggregation: { aggregation: "count" },
    });
    expect(
      PipeOperation.window(Time.us(1), Extractor.recordKey(), WindowAggregation.count(), {
        maxLate: Time.s(15),
      }).toWire().max_late_ns,
    ).toBe(15_000_000_000);
  });

  it("Time extracts exact nanoseconds in every unit", () => {
    expect(Time.ns(1).nanos).toBe(1n);
    expect(Time.us(1).nanos).toBe(1_000n);
    expect(Time.ms(1).nanos).toBe(1_000_000n);
    expect(Time.s(1).nanos).toBe(1_000_000_000n);
    expect(Time.m(1).nanos).toBe(60_000_000_000n);
    expect(Time.h(1).nanos).toBe(3_600_000_000_000n);
    expect(Time.d(1).nanos).toBe(86_400_000_000_000n);
    expect(Time.m(5).nanos).toBe(Time.s(300).nanos);
    expect(Time.h(1.5).nanos).toBe(5_400_000_000_000n);
    expect(() => Time.s(Number.NaN)).toThrow("finite");
    expect(() => Time.h(3_000_000)).toThrow(/overflows|safe-integer/);
    expect(() => Time.ns(9_007_199_254_740_993n)).toThrow(/overflows|safe-integer/);
    // The exact boundary still builds.
    expect(Time.ns(9_007_199_254_740_991n).nanos).toBe(9_007_199_254_740_991n);
  });

  it("window rejects non-positive and non-Time durations", () => {
    const count = WindowAggregation.count();
    const key = Extractor.recordKey();
    expect(() => PipeOperation.window(Time.s(0), key, count)).toThrow(
      "window must be a positive Time",
    );
    expect(() => PipeOperation.window(Time.s(-1), key, count)).toThrow(
      "window must be a positive Time",
    );
    expect(() =>
      PipeOperation.window(1_000 as never, key, count),
    ).toThrow("requires a Time");
    expect(() =>
      PipeOperation.window(Time.s(1), key, count, { watermark: Time.s(-5) }),
    ).toThrow("watermark must be a positive Time");
    expect(() =>
      PipeOperation.window(Time.s(1), key, count, { maxLate: 60 as never }),
    ).toThrow("requires a Time");
  });

  it("window validates the lateness models", () => {
    const count = WindowAggregation.count();
    const key = Extractor.recordKey();
    // MAX_LATE and WATERMARK are alternative lateness models.
    expect(() =>
      PipeOperation.window(Time.m(1), key, count, {
        maxLate: Time.m(1),
        watermark: Time.m(5),
      }),
    ).toThrow("alternative lateness models");
    // WATERMARK follows the event clock only.
    expect(() =>
      PipeOperation.window(Time.m(1), key, count, {
        time: "ingest",
        watermark: Time.m(5),
      }),
    ).toThrow("ingest clock");
    // GRACE requires WATERMARK — revision bounds ride the horizon.
    expect(() =>
      PipeOperation.window(Time.m(1), key, count, { grace: Time.s(30) }),
    ).toThrow("grace requires watermark");
  });

  it("refuses u64 values JSON numbers cannot carry exactly", () => {
    const count = WindowAggregation.count();
    const key = Extractor.recordKey();
    expect(() =>
      PipeOperation.window(Time.ns(9_007_199_254_740_993n), key, count),
    ).toThrow(/overflows|safe-integer/);
    expect(() =>
      PipeOperation.window(Time.s(1), key, count, { byteLimit: Number.MAX_SAFE_INTEGER + 1 }),
    ).toThrow("non-negative safe integer");
    // The exact boundary still emits.
    expect(
      PipeOperation.window(Time.ns(9_007_199_254_740_991n), key, count).toWire().window_ns,
    ).toBe(9_007_199_254_740_991);
  });

  it("rejects malformed specs instead of trusting them", () => {
    const count = WindowAggregation.count();
    expect(() =>
      PipeOperation.window(Time.s(1), { kind: "nonsense" } as never, count),
    ).toThrow(/IndexFieldSpec|KEY, METADATA/);
    expect(() =>
      PipeOperation.enrich({ kind: "metadata" } as never),
    ).toThrow("metadata extractor requires a name");
    expect(() =>
      PipeOperation.window(Time.s(1), Extractor.recordKey(), {
        aggregation: "sum",
      } as never),
    ).toThrow();
  });
});

describe("define pipe commands", () => {
  let server: http.Server;
  let client: WhitewaterClient;
  const calls: { method: string; path: string; body: string }[] = [];

  beforeEach(async () => {
    calls.length = 0;
    server = http.createServer((req, res) => {
      const chunks: Buffer[] = [];
      req.on("data", (chunk) => chunks.push(chunk));
      req.on("end", () => {
        calls.push({
          method: req.method ?? "",
          path: req.url ?? "",
          body: Buffer.concat(chunks).toString("utf8"),
        });
        res.setHeader("content-type", "application/json");
        res.end(
          JSON.stringify({
            request_id: "00000000-0000-0000-0000-000000000000",
            revision: 3,
            authority: "control_plane",
            warning: null,
            results: [{ statement: "define_pipe", message: "ok", data: {} }],
          }),
        );
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

  function posted(): Record<string, unknown> {
    expect(calls).toHaveLength(1);
    expect(calls[0].path).toBe("/v1/admin/commands");
    const commands = (JSON.parse(calls[0].body) as { commands: unknown[] }).commands;
    expect(commands).toHaveLength(1);
    return commands[0] as Record<string, unknown>;
  }

  it("defineForwardPipe posts the exact command", async () => {
    await client.defineForwardPipe("pf.copy", "pf.sub", "pf.out");
    expect(posted()).toEqual({
      command: "define_pipe",
      name: "pf.copy",
      subscription: "pf.sub",
      output_feed: "pf.out",
      operation: { operation: "forward" },
    });
  });

  it("defineEnrichPipe posts feed, store, quarantine, and operation", async () => {
    await client.defineEnrichPipe(
      "en.enrich",
      "en.flow",
      "en.enriched",
      "en.profiles",
      PipeOperation.enrich(Extractor.recordKey(), { onMiss: "quarantine" }),
      "en.quarantine",
    );
    expect(posted()).toEqual({
      command: "define_pipe",
      name: "en.enrich",
      subscription: "en.flow",
      output_feed: "en.enriched",
      output_store: "en.profiles",
      quarantine_feed: "en.quarantine",
      operation: {
        operation: "enrich",
        key: { kind: "key" },
        freshness: "latest",
        on_miss: "quarantine",
      },
    });
  });

  it("defineEnrichPipe omits quarantine_feed when undeclared", async () => {
    await client.defineEnrichPipe(
      "en.enrich",
      "en.flow",
      "en.enriched",
      "en.profiles",
      PipeOperation.enrich(Extractor.json("user_id"), {
        freshness: "strict",
        named: "by_user",
      }),
    );
    const command = posted();
    expect(command.quarantine_feed).toBeUndefined();
    expect(command.operation).toEqual({
      operation: "enrich",
      key: { kind: "projection", extractor: "json", version: 1, field: "user_id" },
      freshness: "strict",
      named: "by_user",
      on_miss: "forward",
    });
  });

  it("defineWindowPipe posts store, emit feed, and operation", async () => {
    await client.defineWindowPipe(
      "wn.agg",
      "wn.flow",
      "wn.panes",
      PipeOperation.window(
        Time.s(8),
        Extractor.json("customer"),
        WindowAggregation.sum(Extractor.json("amount")),
        { maxLate: Time.s(15) },
      ),
      "wn.results",
    );
    expect(posted()).toEqual({
      command: "define_pipe",
      name: "wn.agg",
      subscription: "wn.flow",
      output_store: "wn.panes",
      emit_feed: "wn.results",
      operation: {
        operation: "window",
        window_ns: 8_000_000_000,
        key: { kind: "projection", extractor: "json", version: 1, field: "customer" },
        aggregation: {
          aggregation: "sum",
          value: {
            kind: "projection",
            extractor: "json",
            version: 1,
            field: "amount",
          },
        },
        time: "event",
        max_late_ns: 15_000_000_000,
      },
    });
  });

  it("defineWindowPipe omits emit_feed when undeclared", async () => {
    await client.defineWindowPipe(
      "wn.agg",
      "wn.flow",
      "wn.panes",
      PipeOperation.window(Time.s(1), Extractor.recordKey(), WindowAggregation.count()),
    );
    const command = posted();
    expect(command.emit_feed).toBeUndefined();
    expect(command.output_feed).toBeUndefined();
  });

  it("rejects an operation of the wrong kind without a call", () => {
    expect(() =>
      client.defineEnrichPipe(
        "x.pipe",
        "x.flow",
        "x.out",
        "x.store",
        PipeOperation.forward(),
      ),
    ).toThrow("enrich pipes take PipeOperation.enrich");
    expect(() =>
      client.defineWindowPipe("x.pipe", "x.flow", "x.store", PipeOperation.forward()),
    ).toThrow("window pipes take PipeOperation.window");
    expect(() =>
      client.defineWindowPipe(
        "x.pipe",
        "x.flow",
        "x.store",
        PipeOperation.enrich(Extractor.recordKey()),
      ),
    ).toThrow("window pipes take PipeOperation.window");
    expect(calls).toHaveLength(0);
  });

  it("rejects invalid names locally", () => {
    expect(() =>
      client.defineForwardPipe("Bad Name", "pf.sub", "pf.out"),
    ).toThrow("invalid dotted name");
    expect(() =>
      client.defineForwardPipe("pf.copy", "pf.sub", "UPPER.out"),
    ).toThrow("invalid dotted name");
    expect(() =>
      client.defineWindowPipe(
        "wn.agg",
        "wn.flow",
        "wn..panes",
        PipeOperation.window(Time.us(1), Extractor.recordKey(), WindowAggregation.count()),
      ),
    ).toThrow("invalid dotted name");
    expect(calls).toHaveLength(0);
  });
});
