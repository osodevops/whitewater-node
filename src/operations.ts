/** Operational surfaces: Pipe lifecycle, replicated Index queries, and
 * demand observability — the same topology-free model as Feeds (names,
 * never partitions or Nodes). */

import { Buffer } from "node:buffer";
import type { StoreValue } from "./models.js";
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
