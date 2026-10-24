/** Operational surfaces: Pipe lifecycle, replicated Index queries, and
 * demand observability — the same topology-free model as Feeds (names,
 * never partitions or Nodes). */

import { Buffer } from "node:buffer";
import type { StoreExtractor, StoreValue } from "./models.js";
import * as wire from "./wire.js";

// ---------------------------------------------------------------------------
// Pipes
// ---------------------------------------------------------------------------

/** The catalog row `DESCRIBE PIPE`/`SHOW PIPES` returns for a declared
 * Pipe. `operation` stays an opaque object — Forward, Window, and
 * Enrich variants evolve server-side without an SDK bump. */
export interface PipeInfo {
  name: string;
  /** `declared` while driven, `paused` under `PAUSE PIPE`. */
  stage: string;
  subscriptionId: string;
  operation: Record<string, unknown>;
  outputFeedId: string | null;
  outputStoreId: string | null;
  status: string;
  createdAtNs: bigint;
}

function decodePipeRow(row: unknown): PipeInfo | null {
  if (typeof row !== "object" || row === null) return null;
  const r = row as Record<string, unknown>;
  if (typeof r.name !== "string" || typeof r.subscription_id !== "string") {
    return null;
  }
  return {
    name: r.name,
    stage: typeof r.stage === "string" ? r.stage : "",
    subscriptionId: r.subscription_id,
    operation:
      typeof r.operation === "object" && r.operation !== null
        ? (r.operation as Record<string, unknown>)
        : {},
    outputFeedId: typeof r.output_feed_id === "string" ? r.output_feed_id : null,
    outputStoreId: typeof r.output_store_id === "string" ? r.output_store_id : null,
    status: typeof r.status === "string" ? r.status : "",
    createdAtNs: BigInt(r.created_at_ns as number | string | bigint ?? 0),
  };
}

/** Accepts every shape the catalog uses: a bare list, a `{"rows": []}`
 * envelope, or a single row object (DESCRIBE PIPE). */
export function decodePipes(data: unknown): PipeInfo[] {
  let rows: unknown[];
  if (Array.isArray(data)) {
    rows = data;
  } else if (
    typeof data === "object" && data !== null &&
    Array.isArray((data as Record<string, unknown>).rows)
  ) {
    rows = (data as Record<string, unknown>).rows as unknown[];
  } else {
    rows = [data];
  }
  return rows.map(decodePipeRow).filter((p): p is PipeInfo => p !== null);
}

interface PipeCommands {
  commands(
    commands: Array<Record<string, unknown>>,
    requestId?: string,
  ): Promise<{ results: Array<{ data: Record<string, unknown> }> }>;
}

/** A declared Pipe's lifecycle surface. Pausing is cooperative — an
 * in-flight effect finishes and the frontier stays put so redelivery
 * stays replay-safe — and resume restarts driving from the same
 * durable frontier. */
export class Pipe {
  readonly name: string;
  private readonly client: PipeCommands;

  constructor(client: PipeCommands, name: string) {
    this.client = client;
    this.name = name;
  }

  /** `DESCRIBE PIPE`: the live catalog row including stage and the
   * opaque operation definition. */
  async describe(): Promise<PipeInfo> {
    const execution = await this.client.commands([
      { command: "describe", kind: "pipe", name: this.name },
    ]);
    const data = execution.results[0]?.data ?? {};
    const pipes = decodePipes(data);
    if (pipes.length === 0) {
      throw new Error("describe pipe returned no definition");
    }
    return pipes[0];
  }

  /** `PAUSE PIPE`: halts every drive cooperatively; idempotent. */
  async pause(): Promise<void> {
    await this.lifecycle("pause_pipe");
  }

  /** `RESUME PIPE`: restarts driving from the durable frontier. */
  async resume(): Promise<void> {
    await this.lifecycle("resume_pipe");
  }

  /** `DROP PIPE`: retires the declaration and its compute lease. */
  async drop(): Promise<void> {
    await this.client.commands([
      { command: "drop", kind: "pipe", name: this.name },
    ]);
  }

  private async lifecycle(command: string): Promise<void> {
    await this.client.commands([{ command, name: this.name }]);
  }
}

// ---------------------------------------------------------------------------
// Pipe operations (define_pipe)
// ---------------------------------------------------------------------------

/** Which record clock a `window` Pipe buckets into panes — both are
 * signed 64-bit Unix epoch nanoseconds on the record itself. `"event"`
 * (the default) tracks the application-stamped occurrence clock;
 * `"ingest"` tracks the durable-arrival clock. */
export type WindowTime = "event" | "ingest";

/** How fresh an `enrich` Pipe lookup row must be before it is journaled.
 * `"latest"` (the default) serves the replica's applied state — the
 * resolution is journaled either way, never re-read on replay.
 * `"strict"` requires applied to cover committed at read entry; a
 * lagging replica yields a retryable drive failure, never a stale row. */
export type EnrichFreshness = "latest" | "strict";

/** What an `enrich` Pipe lookup miss does to the output record.
 * `"forward"` (the default) emits with `enrich.matched = false`;
 * `"drop"` consumes without emitting — an acknowledged, replay-safe
 * decision; `"quarantine"` routes the miss to the Pipe's declared
 * quarantine Feed. */
export type EnrichMiss = "forward" | "drop" | "quarantine";

/** What a `window` Pipe adds to each pane's accumulator — the wire
 * `WindowAggregation` object verbatim: `{"aggregation":"count"}`,
 * `{"aggregation":"sum","value":<IndexFieldSpec>}`, or
 * `{"aggregation":"average","value":<IndexFieldSpec>}`. Build with the
 * {@link WindowAggregation} factories. */
export type WindowAggregation =
  | { aggregation: "count" }
  | { aggregation: "sum"; value: StoreExtractor }
  | { aggregation: "average"; value: StoreExtractor };

/** Factories for `window` Pipe aggregations. */
export const WindowAggregation = {
  /** Count records per (group Key, pane). */
  count: (): WindowAggregation => ({ aggregation: "count" }),
  /** Sum one extracted integer value per pane. */
  sum: (value: StoreExtractor): WindowAggregation => {
    indexFieldSpecWire(value, "sum value");
    return { aggregation: "sum", value };
  },
  /** Track count and integer sum per pane; readers divide. */
  average: (value: StoreExtractor): WindowAggregation => {
    indexFieldSpecWire(value, "average value");
    return { aggregation: "average", value };
  },
} as const;

/** Options for {@link PipeOperation.enrich}. */
export interface EnrichPipeOptions {
  /** Lookup freshness; default `"latest"`. */
  freshness?: EnrichFreshness;
  /** Resolve through a declared named secondary key instead of the
   * store's primary key — the store must declare the named key. */
  named?: string;
  /** Miss disposition; default `"forward"`. `"quarantine"` requires the
   * Pipe declaration to name a `quarantineFeed`. */
  onMiss?: EnrichMiss;
}

/** Options for {@link PipeOperation.window}. Every `*Ns` bound is a u64
 * duration in nanoseconds and travels as a strict JSON number — never a
 * decimal string — so values must fit the safe-integer range. */
export interface WindowPipeOptions {
  /** Which record clock assigns panes; default `"event"`. */
  time?: WindowTime;
  /** Lateness bound: a record arriving more than this far behind its own
   * `event_time_ns` quarantines into a countable `late` row. Event clock
   * only; mutually exclusive with `watermarkNs`. */
  maxLateNs?: number | bigint;
  /** Event-time watermark: the driver closes a pane once the durable
   * horizon `max(event_time) - watermarkNs` passes its end. Event clock
   * only; mutually exclusive with `maxLateNs`. */
  watermarkNs?: number | bigint;
  /** Revision bound, requires `watermarkNs`: an event landing on an
   * already-emitted pane within the grace bound revises and re-emits it;
   * past the bound it counts into the `late` row. */
  graceNs?: number | bigint;
  /** Pane-row retention bound: a pane row deletes once
   * `pane_end + expireNs` is behind the close clock. Must exceed the
   * declared lateness/revision bound so a pane provably closes first. */
  expireNs?: number | bigint;
  /** Bound on distinct active group Keys held in pane/sentinel state
   * (the server accepts at most 512); unset leaves growth unbounded. */
  keyLimit?: number | bigint;
  /** Bound on accounted pane-state bytes (the server accepts at most
   * 256 KiB); unset leaves byte growth unbounded. */
  byteLimit?: number | bigint;
}

/**
 * The processing step a Pipe applies to each delivered input record.
 * Build with {@link PipeOperation.forward}, {@link PipeOperation.enrich},
 * or {@link PipeOperation.window}; the wire form is internally tagged on
 * `operation` and embedded under `operation` in `define_pipe`.
 */
export class PipeOperation {
  private constructor(
    /** The operation tag — also the wire discriminant. */
    readonly operation: "forward" | "enrich" | "window",
    private readonly wireForm: Record<string, unknown>,
  ) {}

  /** Copy each delivered record to the output Feed unchanged. */
  static forward(): PipeOperation {
    return new PipeOperation("forward", { operation: "forward" });
  }

  /**
   * Point-lookup enrichment against the Pipe's `output_store`: resolve
   * one row by the projected `key` and copy its payload into output
   * Metadata (`enrich.payload`, `enrich.matched`). The lookup resolution
   * rides inside the effect journal, so replayed outputs are
   * byte-identical.
   */
  static enrich(key: StoreExtractor, options: EnrichPipeOptions = {}): PipeOperation {
    const wireForm: Record<string, unknown> = {
      operation: "enrich",
      key: indexFieldSpecWire(key, "enrich key"),
      freshness: options.freshness ?? "latest",
    };
    if (options.named !== undefined) {
      wireForm.named = enrichNamedKey(options.named);
    }
    wireForm.on_miss = options.onMiss ?? "forward";
    return new PipeOperation("enrich", wireForm);
  }

  /**
   * Accumulate each delivered record into fixed `windowNs`-wide panes of
   * a managed StateStore view. Panes are immutable once written; declare
   * `maxLateNs` or `watermarkNs` to bound late-event policy (and to let
   * closed panes emit to a Feed) and `expireNs` to bound pane retention.
   */
  static window(
    windowNs: number | bigint,
    key: StoreExtractor,
    aggregation: WindowAggregation,
    options: WindowPipeOptions = {},
  ): PipeOperation {
    const width = u64Number(windowNs, "windowNs");
    if (width <= 0) {
      throw new Error("windowNs must be positive");
    }
    const wireForm: Record<string, unknown> = {
      operation: "window",
      window_ns: width,
      key: indexFieldSpecWire(key, "window key"),
      aggregation: windowAggregationWire(aggregation),
      time: options.time ?? "event",
    };
    if (options.maxLateNs !== undefined) {
      wireForm.max_late_ns = u64Number(options.maxLateNs, "maxLateNs");
    }
    if (options.watermarkNs !== undefined) {
      wireForm.watermark_ns = u64Number(options.watermarkNs, "watermarkNs");
    }
    if (options.graceNs !== undefined) {
      wireForm.grace_ns = u64Number(options.graceNs, "graceNs");
    }
    if (options.expireNs !== undefined) {
      wireForm.expire_ns = u64Number(options.expireNs, "expireNs");
    }
    if (options.keyLimit !== undefined) {
      wireForm.key_limit = u64Number(options.keyLimit, "keyLimit");
    }
    if (options.byteLimit !== undefined) {
      wireForm.byte_limit = u64Number(options.byteLimit, "byteLimit");
    }
    return new PipeOperation("window", wireForm);
  }

  /** The wire object embedded under `operation` in `define_pipe`. */
  toWire(): Record<string, unknown> {
    return { ...this.wireForm };
  }
}

/**
 * Command u64 fields (`window_ns`, `max_late_ns`, …) travel as strict
 * JSON numbers — never the decimal strings record nanoseconds use — so
 * the exact-wire bound is the safe-integer range, not u64::MAX. A bigint
 * beyond `Number.MAX_SAFE_INTEGER` cannot be carried exactly and is
 * refused here rather than silently rounded.
 */
function u64Number(value: number | bigint, field: string): number {
  if (typeof value === "bigint") {
    if (value < 0n || value > BigInt(Number.MAX_SAFE_INTEGER)) {
      throw new Error(
        `${field} must be within 0..${Number.MAX_SAFE_INTEGER}: ` +
          "define_pipe u64 fields are strict JSON numbers and cannot " +
          `carry ${value} exactly`,
      );
    }
    return Number(value);
  }
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new Error(`${field} must be a non-negative safe integer`);
  }
  return value;
}

/** Lower a declared `IndexFieldSpec` onto the wire, validating the
 * grammar the server enforces: KEY, METADATA 'name', or JSON 'path'
 * (exactly what `Extractor.recordKey()/metadata()/json()` build). */
function indexFieldSpecWire(spec: StoreExtractor, what: string): Record<string, unknown> {
  if (typeof spec !== "object" || spec === null) {
    throw new Error(
      `${what} requires an IndexFieldSpec — ` +
        "use Extractor.recordKey()/metadata()/json()",
    );
  }
  switch (spec.kind) {
    case "key":
      return { kind: "key" };
    case "metadata":
      if (typeof spec.name !== "string" || spec.name.length === 0 || spec.name.length > 256) {
        throw new Error(`${what} metadata extractor requires a name of 1-256 characters`);
      }
      return { kind: "metadata", name: spec.name };
    case "projection":
      if (
        spec.extractor !== "json" ||
        spec.version !== 1 ||
        typeof spec.field !== "string" ||
        spec.field.length === 0 ||
        spec.field.length > 512
      ) {
        throw new Error(
          `${what} accepts KEY, METADATA 'name', or JSON 'path' — ` +
            "use Extractor.json(field)",
        );
      }
      return { kind: "projection", extractor: "json", version: 1, field: spec.field };
    default:
      throw new Error(
        `${what} accepts KEY, METADATA 'name', or JSON 'path' — ` +
          "use Extractor.recordKey()/metadata()/json()",
      );
  }
}

/** Lower a `WindowAggregation` spec onto the wire. */
function windowAggregationWire(aggregation: WindowAggregation): Record<string, unknown> {
  if (typeof aggregation !== "object" || aggregation === null) {
    throw new Error(
      "window aggregation is required — " +
        "use WindowAggregation.count()/sum()/average()",
    );
  }
  switch (aggregation.aggregation) {
    case "count":
      return { aggregation: "count" };
    case "sum":
    case "average":
      return {
        aggregation: aggregation.aggregation,
        value: indexFieldSpecWire(aggregation.value, `${aggregation.aggregation} value`),
      };
    default:
      throw new Error(
        "window aggregation must be count, sum, or average — " +
          "use WindowAggregation.count()/sum()/average()",
      );
  }
}

/** The named-key grammar the server enforces on `enrich` `named`. */
function enrichNamedKey(name: string): string {
  if (!/^[a-z][a-z0-9_]{0,63}$/.test(name)) {
    throw new Error(`invalid named key: ${name}`);
  }
  return name;
}

// ---------------------------------------------------------------------------
// Index queries
// ---------------------------------------------------------------------------

/** Inclusive/exclusive bound for a range query. */
export interface IndexBound {
  value: StoreValue;
  inclusive: boolean;
}

export const indexBound = {
  inclusive: (value: StoreValue): IndexBound => ({ value, inclusive: true }),
  exclusive: (value: StoreValue): IndexBound => ({ value, inclusive: false }),
};

/** The query shape `queryIndex` lowers onto the wire: every declared
 * field exact, a prefix over leading fields, or a range over the next
 * field after exact leaders. Values take `StoreValue` scalars; `bigint`
 * narrows to i64 and `number` must be a safe integer. */
export type IndexPredicate =
  | { mode: "exact"; values: StoreValue[] }
  | { mode: "prefix"; leading: StoreValue[]; partialText?: string }
  | { mode: "range"; leading: StoreValue[]; lower: IndexBound; upper: IndexBound };

export const indexPredicate = {
  exact: (values: StoreValue[]): IndexPredicate => ({ mode: "exact", values }),
  prefix: (leading: StoreValue[], partialText?: string): IndexPredicate => ({
    mode: "prefix",
    leading,
    partialText,
  }),
  range: (leading: StoreValue[], lower: IndexBound, upper: IndexBound): IndexPredicate => ({
    mode: "range",
    leading,
    lower,
    upper,
  }),
};

export function predicateFields(predicate: IndexPredicate): Record<string, unknown> {
  switch (predicate.mode) {
    case "exact":
      return { mode: "exact", values: predicate.values.map(wire.indexValue) };
    case "prefix": {
      const body: Record<string, unknown> = {
        mode: "prefix",
        leading: predicate.leading.map(wire.indexValue),
      };
      if (predicate.partialText !== undefined && predicate.partialText !== "") {
        body.partial_text = predicate.partialText;
      }
      return body;
    }
    case "range":
      return {
        mode: "range",
        leading: predicate.leading.map(wire.indexValue),
        lower: {
          value: wire.indexValue(predicate.lower.value),
          inclusive: predicate.lower.inclusive,
        },
        upper: {
          value: wire.indexValue(predicate.upper.value),
          inclusive: predicate.upper.inclusive,
        },
      };
  }
}

/** One decoded index posting row. */
export interface IndexRow {
  key: Buffer;
  payload: Buffer;
}

/** A page returned by `queryIndex`. */
export interface IndexPage {
  rows: IndexRow[];
  /** Active generation that served — changes on rebuild. */
  generation: number;
  appliedCursor: string | null;
  targetCursor: string | null;
  /** Serving Node still materializing the activation frontier. */
  behind: boolean;
  /** The page hit a server bound; narrow the query to continue. */
  truncated: boolean;
  /** Node identity that served — diagnostics, never routing input. */
  servedBy: string;
}

export function decodeIndexPage(body: unknown): IndexPage {
  const b = (typeof body === "object" && body !== null ? body : {}) as Record<string, unknown>;
  const rows = (Array.isArray(b.rows) ? b.rows : []).map((row) => {
    const r = row as Record<string, unknown>;
    if (typeof r.key_base64 !== "string" || typeof r.payload_base64 !== "string") {
      throw new Error("index row missing key_base64/payload_base64");
    }
    return {
      key: Buffer.from(r.key_base64, "base64"),
      payload: Buffer.from(r.payload_base64, "base64"),
    };
  });
  return {
    rows,
    generation: Number(b.generation ?? 0),
    appliedCursor: typeof b.applied_cursor === "string" ? b.applied_cursor : null,
    targetCursor: typeof b.target_cursor === "string" ? b.target_cursor : null,
    behind: b.behind === true,
    truncated: b.truncated === true,
    servedBy: typeof b.served_by === "string" ? b.served_by : "",
  };
}

// ---------------------------------------------------------------------------
// Demand observability
// ---------------------------------------------------------------------------

/** One demand row for a declared resource. `fields` carries the
 * server's counters verbatim — new counters arrive without an SDK
 * update, the same contract as `PipeInfo.operation`. */
export interface DemandRow {
  name: string;
  /** Owning Domain when the server attributes one. */
  domain: string | null;
  fields: Record<string, unknown>;
}

/** One counter as number when present. */
export function demandCounter(row: DemandRow, name: string): number | null {
  const value = row.fields[name];
  return typeof value === "number" ? value : null;
}

/** A demand snapshot for one resource kind: per-resource rows plus the
 * per-domain rollup map. */
export interface DemandReport {
  rows: DemandRow[];
  /** `{"<domain>": {<counters>}}` — opaque rollup rows keyed by Domain. */
  domains: Record<string, Record<string, unknown>>;
}

export function decodeDemandReport(
  body: unknown,
  resourceKey: string,
  nameField: string,
): DemandReport {
  const b = (typeof body === "object" && body !== null ? body : {}) as Record<string, unknown>;
  const rawRows = Array.isArray(b[resourceKey]) ? (b[resourceKey] as unknown[]) : [];
  const rows = rawRows
    .filter((row): row is Record<string, unknown> => typeof row === "object" && row !== null)
    .map((row) => ({
      name: typeof row[nameField] === "string" ? (row[nameField] as string) : "",
      domain: typeof row.domain === "string" ? row.domain : null,
      fields: row,
    }));
  const domains =
    typeof b.domains === "object" && b.domains !== null
      ? (b.domains as Record<string, Record<string, unknown>>)
      : {};
  return { rows, domains };
}

/** Evidence that one Feed Key dominates its traffic window. A strictly
 * ordered hot Key is never parallelized — the honest operator signal
 * is to spread or shed load for the Key, not add capacity. */
export interface HotKey {
  /** The evidence object verbatim — includes any range-placement
   * fields the server reports; those are diagnostics, not a public
   * placement contract. */
  evidence: Record<string, unknown>;
}

export function decodeHotKeys(body: unknown): HotKey[] {
  const b = (typeof body === "object" && body !== null ? body : {}) as Record<string, unknown>;
  const rows = Array.isArray(b.hot_keys) ? b.hot_keys : [];
  return rows
    .filter((row): row is Record<string, unknown> => typeof row === "object" && row !== null)
    .map((evidence) => ({ evidence }));
}
