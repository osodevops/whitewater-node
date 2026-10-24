/**
 * Whitewater client facades: Writer session, Reader session, and
 * Subscription member — mirroring the conformance fixture operation
 * vocabulary. Every call is async; endpoint round-robining and error
 * classification live in {@link Transport}.
 */

import { randomUUID } from "node:crypto";
import { codecs, type Codec } from "./codecs.js";
import { AmbiguousResult, Conflict, Unavailable, WhitewaterError } from "./errors.js";
import { Feed } from "./feed.js";
import {
  decodeDemandReport,
  decodeHotKeys,
  decodeIndexPage,
  decodePipes,
  predicateFields,
  Pipe,
  PipeOperation,
  type HotKey,
  type IndexPage,
  type IndexPredicate,
  type PipeInfo,
} from "./operations.js";
import { Store } from "./store-view.js";
import { MAX_NAMED_KEYS } from "./models.js";
import type {
  AppendBatchResult,
  AppendResult,
  ControlExecution,
  Cursor,
  FeedAppend,
  FeedRecord,
  Joined,
  MemberPage,
  MutationResult,
  NamedKey,
  ReaderPage,
  ReaderSession,
  ReaderStart,
  StoreExtractor,
  StorePage,
  StoreRow,
  StoreRows,
  StoreValue,
  TemporaryFetchOptions,
  TemporaryFetchPage,
  WorkLease,
} from "./models.js";
import { ReaderStart as readerStart } from "./models.js";
import { Transport } from "./transport.js";
import * as wire from "./wire.js";

export interface AppendInput {
  requestId?: string;
  key?: Buffer | null;
  payload: Buffer;
  eventTimeNs?: bigint | null;
  metadata?: Record<string, Buffer>;
}

export class WriterSessionClient {
  constructor(
    private readonly transport: Transport,
    private readonly writer: string,
    private readonly _sessionEpoch: number,
    private readonly compression = false,
  ) {}

  /** The session's fencing epoch. */
  get sessionEpoch(): number {
    return this._sessionEpoch;
  }

  useCompression(enabled = true): WriterSessionClient {
    return new WriterSessionClient(this.transport, this.writer, this.sessionEpoch, enabled);
  }

  /** Idempotent single append; same requestId dedups on the server. */
  async append(input: AppendInput): Promise<AppendResult> {
    const body = wire.appendRequest(
      input.requestId ?? randomUUID(),
      this.writer,
      this.sessionEpoch,
      input.key ?? null,
      input.payload,
      input.eventTimeNs ?? null,
      input.metadata ?? {},
    );
    if (this.compression) body.use_compression = true;
    return wire.appendResult(await this.transport.post("/v1/writers/append", body));
  }

  async appendBatch(records: AppendInput[]): Promise<AppendBatchResult> {
    const body = {
      records: records.map((r) =>
        wire.appendRequest(
          r.requestId ?? randomUUID(),
          this.writer,
          this.sessionEpoch,
          r.key ?? null,
          r.payload,
          r.eventTimeNs ?? null,
          r.metadata ?? {},
        ),
      ),
    };
    if (this.compression) {
      for (const record of body.records) record.use_compression = true;
    }
    const response = (await this.transport.post("/v1/writers/append-batch", body)) as {
      results?: unknown[];
      feedback?: unknown;
    };
    return {
      results: (response.results ?? []).map(wire.appendResult),
      feedback: wire.feedback(response.feedback),
    };
  }
}

export class ReaderSessionClient {
  constructor(
    private readonly transport: Transport,
    private readonly reader: string,
  ) {}

  async open(capacity = 64, requestId = randomUUID()): Promise<ReaderSession> {
    const body = { request_id: requestId, reader: this.reader, capacity };
    return wire.readerSession(await this.transport.post("/v1/readers/open", body));
  }

  async fetch(
    session: ReaderSession,
    limit?: number,
    requestId = randomUUID(),
  ): Promise<ReaderPage> {
    const body: Record<string, unknown> = {
      request_id: requestId,
      reader: this.reader,
      session_epoch: session.sessionEpoch,
    };
    if (limit !== undefined) body.limit = limit;
    return wire.readerPage(await this.transport.post("/v1/readers/fetch", body));
  }

  /** Durably commit progress at a page's delivered cursor. */
  async ack(
    session: ReaderSession,
    cursor: Cursor,
    requestId = randomUUID(),
  ): Promise<ReaderSession> {
    const body = {
      request_id: requestId,
      reader: this.reader,
      session_epoch: session.sessionEpoch,
      cursor,
    };
    return wire.readerSession(await this.transport.post("/v1/readers/ack", body));
  }

  async close(session: ReaderSession): Promise<ReaderSession> {
    const body = {
      request_id: randomUUID(),
      reader: this.reader,
      session_epoch: session.sessionEpoch,
    };
    return wire.readerSession(await this.transport.post("/v1/readers/close", body));
  }
}

export class SubscriptionMemberClient {
  constructor(
    private readonly transport: Transport,
    private readonly subscription: string,
  ) {}

  private leaseBody(member: Joined, workId: string, leaseEpoch: number | null) {
    const body: Record<string, unknown> = {
      subscription: this.subscription,
      request_id: randomUUID(),
      member_id: member.memberId,
      member_epoch: member.memberEpoch,
      work_id: workId,
    };
    if (leaseEpoch !== null) body.lease_epoch = leaseEpoch;
    return body;
  }

  async join(memberId: string): Promise<Joined> {
    const body = {
      subscription: this.subscription,
      request_id: randomUUID(),
      member_id: memberId,
    };
    const response = (await this.transport.post("/v1/subscriptions/members/join", body)) as {
      member_epoch: number;
      ownership_epoch?: number;
      tick?: number;
    };
    return {
      memberId,
      memberEpoch: response.member_epoch,
      ownershipEpoch: response.ownership_epoch ?? 0,
      tick: response.tick ?? 0,
    };
  }

  async claim(member: Joined, workId: string, leaseTicks?: number): Promise<WorkLease> {
    return this.lease("/v1/subscriptions/members/claim", member, workId, null, leaseTicks);
  }

  async renew(member: Joined, lease: WorkLease, leaseTicks?: number): Promise<WorkLease> {
    return this.lease(
      "/v1/subscriptions/members/renew",
      member,
      lease.workId,
      lease.leaseEpoch,
      leaseTicks,
    );
  }

  private async lease(
    path: string,
    member: Joined,
    workId: string,
    leaseEpoch: number | null,
    leaseTicks?: number,
  ): Promise<WorkLease> {
    const body = this.leaseBody(member, workId, leaseEpoch);
    if (leaseTicks !== undefined) body.lease_ticks = leaseTicks;
    const response = (await this.transport.post(path, body)) as { lease?: Record<string, unknown> };
    const lease = response.lease;
    if (!lease) {
      throw new Conflict(409, "lease.refused", "work claim was not granted");
    }
    return {
      workId: String(lease.work_id),
      memberId: String(lease.member_id),
      memberEpoch: Number(lease.member_epoch),
      leaseEpoch: Number(lease.lease_epoch),
      expiresAtTick: Number(lease.expires_at_tick ?? 0),
    };
  }

  async release(member: Joined, lease: WorkLease): Promise<void> {
    await this.transport.post(
      "/v1/subscriptions/members/release",
      this.leaseBody(member, lease.workId, lease.leaseEpoch),
    );
  }

  async fetch(member: Joined, lease: WorkLease, limit?: number): Promise<MemberPage> {
    const body = this.leaseBody(member, lease.workId, lease.leaseEpoch);
    if (limit !== undefined) body.limit = limit;
    return wire.memberPage(await this.transport.post("/v1/subscriptions/members/fetch", body));
  }

  /** Commit the fetched frontier; overlapping members are fenced here. */
  async ack(member: Joined, lease: WorkLease, page: MemberPage): Promise<void> {
    const body = this.leaseBody(member, lease.workId, lease.leaseEpoch);
    if (page.cursor !== null) body.cursor = page.cursor;
    body.positions = { ...page.positions };
    await this.transport.post("/v1/subscriptions/members/ack", body);
  }
}

/**
 * Ad-hoc Feed operations: committed record reads, anonymous temporary
 * fetches, and single-shot appends with a caller-supplied writer
 * identity — no named session required.
 */
export class FeedClient {
  constructor(private readonly transport: Transport, private readonly compression = false) {}

  useCompression(enabled = true): FeedClient {
    return new FeedClient(this.transport, enabled);
  }

  /** Committed record history via GET /v1/feeds/records. */
  async records(feed: string, limit?: number, after?: Cursor | null): Promise<FeedRecord[]> {
    let path = "/v1/feeds/records?feed=" + encodeURIComponent(feed);
    if (limit != null) {
      path += "&limit=" + limit;
    }
    if (after != null) {
      path += "&after=" + encodeURIComponent(after);
    }
    return wire.records(await this.transport.get(path));
  }

  /**
   * Anonymous temporary Reader fetch: bounded page after a Cursor,
   * `newOnly` positioning, or `tail` + `waitMs` long-poll.
   */
  async temporaryFetch(
    feed: string,
    options: TemporaryFetchOptions = {},
  ): Promise<TemporaryFetchPage> {
    const body: Record<string, unknown> = { feed };
    if (options.after != null) body.after = options.after;
    if (options.limit != null) body.limit = options.limit;
    if (options.newOnly) body.new_only = true;
    if (options.tail) body.tail = true;
    if (options.waitMs != null) body.wait_ms = options.waitMs;
    if (options.afterEventTimeNs != null) {
      body.after_event_time_ns = options.afterEventTimeNs.toString();
    }
    return wire.temporaryFetchPage(
      await this.transport.post("/v1/readers/temporary/fetch", body),
    );
  }

  /** Ad-hoc append; identical `requestId` retries dedupe. */
  async append(feed: string, record: FeedAppend): Promise<AppendResult> {
    const body = wire.feedAppendRequest(feed, record);
    if (this.compression) body.use_compression = true;
    return wire.appendResult(await this.transport.post("/v1/feeds/append", body));
  }

  /**
   * Bulk-read committed Feed history once, from `start`, delivering each
   * page to `sink` until the current frontier is drained. Returns the
   * number of records delivered. Use {@link WhitewaterClient.subscribe}
   * when the sink should keep receiving.
   *
   * `ReaderStart.now()` loads nothing (the frontier is empty at call
   * time); `cursor`/`timestampNs` resume from that point.
   */
  async load(
    feed: string,
    start: ReaderStart,
    sink: (records: FeedRecord[]) => void | Promise<void>,
  ): Promise<number> {
    const deliver = sink;
    let options = loadFirstPage(start);
    let loaded = 0;
    for (;;) {
      const page = await this.temporaryFetch(feed, options);
      if (page.records.length === 0) return loaded;
      await deliver(page.records);
      loaded += page.records.length;
      options = { after: page.nextCursor, limit: LOAD_PAGE };
    }
  }
}

const LOAD_PAGE = 512;

function loadFirstPage(start: ReaderStart): TemporaryFetchOptions {
  switch (start.kind) {
    case "beginning":
      return { limit: LOAD_PAGE };
    case "now":
      return { newOnly: true };
    case "cursor":
      return { after: start.cursor, limit: LOAD_PAGE };
    case "timestamp":
      return { afterEventTimeNs: start.nanos, limit: LOAD_PAGE };
  }
}

/**
 * A running feed subscription: a named Reader session polled in the
 * background, invoking `onRecords` per fetched page and acknowledging
 * durable progress after each delivered batch.
 *
 * Error policy: transient refusals ({@link Unavailable},
 * {@link AmbiguousResult}) retry after a short pause. {@link StaleSession}
 * — another client opened the same Reader — terminates the subscription
 * rather than fencing back. Any other failure also terminates. Terminal
 * errors reach `onError` and surface on {@link failure}.
 */
export class Subscription {
  private stopped = false;
  private readonly done: Promise<void>;
  private _failure?: WhitewaterError;

  constructor(
    private readonly client: WhitewaterClient,
    private readonly readerName: string,
    private readonly feed: string,
    private readonly start: ReaderStart,
    private readonly capacity: number,
    private readonly onRecords: (records: FeedRecord[]) => void | Promise<void>,
    private readonly onError?: (error: WhitewaterError) => void,
  ) {
    this.done = this.run();
  }

  /** False once the loop has exited (closed or failed). */
  get running(): boolean {
    return !this.stopped;
  }

  /** The terminal error, when the loop stopped on a failure. */
  get failure(): WhitewaterError | undefined {
    return this._failure;
  }

  /** Resolves when the loop exits — the supervised-consumer seam. */
  get settled(): Promise<void> {
    return this.done;
  }

  /** Signal stop, release the Reader session, and await the loop. */
  async close(): Promise<void> {
    this.stopped = true;
    await this.done;
  }

  private async run(): Promise<void> {
    try {
      await this.ensureReader();
      const reader = this.client.reader(this.readerName);
      let session = await this.openWithRetry(reader);
      if (session === null) return;
      try {
        while (!this.stopped) {
          try {
            const page = await reader.fetch(session, this.capacity);
            if (page.records.length > 0) {
              await this.onRecords(page.records);
              if (page.deliveredCursor != null) {
                session = await reader.ack(session, page.deliveredCursor);
              }
            } else {
              await sleep(EMPTY_POLL_MS);
            }
          } catch (error) {
            if (!isRetryable(error)) throw error;
            await sleep(RETRY_MS);
          }
        }
      } finally {
        try {
          await reader.close(session);
        } catch {
          // Session may already be superseded; release is best-effort.
        }
      }
    } catch (error) {
      if (!this.stopped && error instanceof WhitewaterError) {
        this._failure = error;
        this.onError?.(error);
      }
    } finally {
      this.stopped = true;
    }
  }

  /** Declare the Reader when missing; an existing Reader is reused as-is. */
  private async ensureReader(): Promise<void> {
    try {
      await this.client.createReader(this.readerName, this.feed, this.start);
    } catch (error) {
      if (!(error instanceof WhitewaterError && error.message.includes("exists"))) {
        throw error;
      }
    }
  }

  private async openWithRetry(reader: ReaderSessionClient): Promise<ReaderSession | null> {
    while (!this.stopped) {
      try {
        return await reader.open(this.capacity);
      } catch (error) {
        if (!isRetryable(error)) throw error;
        await sleep(RETRY_MS);
      }
    }
    return null;
  }
}

const EMPTY_POLL_MS = 250;
const RETRY_MS = 1_000;

function isRetryable(error: unknown): boolean {
  return error instanceof Unavailable || error instanceof AmbiguousResult;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export interface WhitewaterClientOptions {
  apiKey: string;
  endpoints: string[];
  timeoutMs?: number;
}

export class WhitewaterClient {
  readonly feeds: FeedClient;
  /** Shared transport; exposed for low-level/operator calls not yet
   * covered by a typed facade. */
  readonly transport: Transport;

  readonly stateStores: StateStoresClient;

  constructor(options: WhitewaterClientOptions) {
    this.transport = new Transport(options.endpoints, options.apiKey, options.timeoutMs);
    this.feeds = new FeedClient(this.transport);
    this.stateStores = new StateStoresClient(this.transport);
  }

  /** Execute a WCL script; returns per-statement results. */
  async wcl(script: string, requestId = randomUUID()): Promise<ControlExecution> {
    const body = { request_id: requestId, script };
    return wire.controlExecution(await this.transport.post("/v1/admin/wcl", body));
  }

  /** Execute typed control commands, e.g. [{ command: "show", kind: "feeds" }]. */
  async commands(
    commands: Array<Record<string, unknown>>,
    requestId = randomUUID(),
  ): Promise<ControlExecution> {
    return wire.controlExecution(
      await this.transport.post("/v1/admin/commands", { request_id: requestId, commands }),
    );
  }

  /** Liveness of the contacted Node. */
  async healthy(): Promise<boolean> {
    const health = (await this.transport.get("/health")) as { status?: string };
    return health?.status === "ok";
  }

  // ---- Pipe lifecycle ---------------------------------------------------------

  /** Bind a declared Pipe for lifecycle operations:
   * describe/pause/resume/drop. */
  pipe(name: string): Pipe {
    return new Pipe(this, name);
  }

  /** `SHOW PIPES`: the catalog of declared Pipes. */
  async pipes(): Promise<PipeInfo[]> {
    const execution = await this.commands([{ command: "show", kind: "pipes" }]);
    return decodePipes(execution.results[0]?.data ?? []);
  }

  // ---- Pipe declaration --------------------------------------------------------

  /**
   * `DEFINE PIPE <name> FROM SUBSCRIPTION <subscription> TO FEED <feed>` —
   * the copy Pipe. `name`, `subscription`, and `outputFeed` are
   * same-Domain dotted names.
   */
  defineForwardPipe(
    name: string,
    subscription: string,
    outputFeed: string,
  ): Promise<ControlExecution> {
    return this.commands([
      {
        command: "define_pipe",
        name: dotted(name),
        subscription: dotted(subscription),
        output_feed: dotted(outputFeed),
        operation: PipeOperation.forward().toWire(),
      },
    ]);
  }

  /**
   * `DEFINE PIPE ... ENRICH STATESTORE <store>` — point-lookup
   * enrichment. `operation` must be {@link PipeOperation.enrich};
   * `quarantineFeed` resolves the miss-quarantine target when the
   * operation's miss disposition is `"quarantine"`.
   */
  defineEnrichPipe(
    name: string,
    subscription: string,
    outputFeed: string,
    store: string,
    operation: PipeOperation,
    quarantineFeed?: string,
  ): Promise<ControlExecution> {
    if (operation?.operation !== "enrich") {
      throw new Error("enrich pipes take PipeOperation.enrich(...)");
    }
    const command: Record<string, unknown> = {
      command: "define_pipe",
      name: dotted(name),
      subscription: dotted(subscription),
      output_feed: dotted(outputFeed),
      output_store: dotted(store),
      operation: operation.toWire(),
    };
    if (quarantineFeed !== undefined) {
      command.quarantine_feed = dotted(quarantineFeed);
    }
    return this.commands([command]);
  }

  /**
   * `DEFINE PIPE ... TO STATESTORE <store>` — rolling-window
   * aggregation. `operation` must be {@link PipeOperation.window};
   * `emitFeed` optionally publishes closed panes and requires the
   * operation declare a lateness bound (`maxLateNs` or `watermarkNs`).
   */
  defineWindowPipe(
    name: string,
    subscription: string,
    outputStore: string,
    operation: PipeOperation,
    emitFeed?: string,
  ): Promise<ControlExecution> {
    if (operation?.operation !== "window") {
      throw new Error("window pipes take PipeOperation.window(...)");
    }
    const command: Record<string, unknown> = {
      command: "define_pipe",
      name: dotted(name),
      subscription: dotted(subscription),
      output_store: dotted(outputStore),
      operation: operation.toWire(),
    };
    if (emitFeed !== undefined) {
      command.emit_feed = dotted(emitFeed);
    }
    return this.commands([command]);
  }

  // ---- Index queries ------------------------------------------------------------

  /** Query a replicated Index by predicate — exact, prefix, or range
   * over typed values. `strict` waits for the committed target rather
   * than answering stale. */
  async queryIndex(
    index: string,
    predicate: IndexPredicate,
    options: { limit?: number; strict?: boolean; waitMs?: number } = {},
  ): Promise<IndexPage> {
    const body: Record<string, unknown> = {
      index: dotted(index),
      ...predicateFields(predicate),
      strict: options.strict ?? false,
    };
    if (options.limit !== undefined) body.limit = options.limit;
    if (options.waitMs !== undefined) body.wait_ms = options.waitMs;
    return decodeIndexPage(await this.transport.post("/v1/indexes/query", body));
  }

  // ---- Demand observability -------------------------------------------------------

  private async demand(surface: string, resourceKey: string, nameField: string) {
    return decodeDemandReport(
      await this.transport.get(`/v1/admin/demand/${surface}`),
      resourceKey,
      nameField,
    );
  }

  /** `GET /v1/admin/demand/feeds` — per-Feed append/read attribution. */
  demandFeeds() {
    return this.demand("feeds", "feeds", "feed");
  }

  /** `GET /v1/admin/demand/readers` — per-Reader request/fetch/ack counters. */
  demandReaders() {
    return this.demand("readers", "readers", "reader");
  }

  /** `GET /v1/admin/demand/subscriptions` — per-Subscription counters. */
  demandSubscriptions() {
    return this.demand("subscriptions", "subscriptions", "subscription");
  }

  /** `GET /v1/admin/demand/indexes` — per-Index query counters. */
  demandIndexes() {
    return this.demand("indexes", "indexes", "index");
  }

  /** `GET /v1/admin/demand/pipes` — per-Pipe drive/apply/failure counters. */
  demandPipes() {
    return this.demand("pipes", "pipes", "pipe");
  }

  /** `GET /v1/admin/demand/hot-keys` — dominant-Key evidence across
   * Feeds. A strictly ordered hot Key is never parallelized: the
   * honest signal is to spread or shed load for the Key. */
  async hotKeys(): Promise<HotKey[]> {
    return decodeHotKeys(await this.transport.get("/v1/admin/demand/hot-keys"));
  }

  // ---- Resource management ---------------------------------------------------
  // Typed convenience over {@link wcl}: each method builds one WCL statement
  // and executes it. Names are validated before interpolation, so a bad value
  // fails locally rather than reaching the Control Plane.

  /** `CREATE DOMAIN <name>` */
  createDomain(name: string): Promise<ControlExecution> {
    return this.wcl(`CREATE DOMAIN ${dotted(name)}`);
  }

  /** `CREATE FEED <dotted.name>` */
  createFeed(name: string): Promise<ControlExecution> {
    return this.wcl(`CREATE FEED ${dotted(name)}`);
  }

  /** `CREATE WRITER <name> TO <feed>` */
  createWriter(name: string, feed: string): Promise<ControlExecution> {
    return this.wcl(`CREATE WRITER ${resource(name)} TO ${dotted(feed)}`);
  }

  /** `CREATE READER <name> FROM <feed> [START AT <start>]` (default BEGINNING). */
  createReader(
    name: string,
    feed: string,
    start: ReaderStart = readerStart.beginning(),
  ): Promise<ControlExecution> {
    return this.wcl(
      `CREATE READER ${resource(name)} FROM ${dotted(feed)} START AT ${readerStart.wcl(start)}`,
    );
  }

  /** `CREATE SUBSCRIPTION <name> FROM <feed> [START AT <start>]`. */
  createSubscription(
    name: string,
    feed: string,
    start: ReaderStart = readerStart.beginning(),
  ): Promise<ControlExecution> {
    return this.wcl(
      `CREATE SUBSCRIPTION ${resource(name)} FROM ${dotted(feed)} START AT ${readerStart.wcl(start)}`,
    );
  }

  /** `CREATE ROLE <name>` */
  createRole(name: string): Promise<ControlExecution> {
    return this.wcl(`CREATE ROLE ${resource(name)}`);
  }

  /** `OPEN WRITER SESSION <name>`; the returned epoch feeds {@link writer}. */
  openWriterSession(writer: string): Promise<ControlExecution> {
    return this.wcl(`OPEN WRITER SESSION ${resource(writer)}`);
  }

  /** `SEEK READER <name> TO <start>` — repositions a durable Reader. */
  seekReader(reader: string, start: ReaderStart): Promise<ControlExecution> {
    return this.wcl(`SEEK READER ${resource(reader)} TO ${readerStart.wcl(start)}`);
  }

  /** `DESCRIBE <kind> <name>` */
  describeDomain(name: string): Promise<ControlExecution> {
    return this.describe("DOMAIN", name, true);
  }
  describeFeed(name: string): Promise<ControlExecution> {
    return this.describe("FEED", name, true);
  }
  describeWriter(name: string): Promise<ControlExecution> {
    return this.describe("WRITER", name, false);
  }
  describeReader(name: string): Promise<ControlExecution> {
    return this.describe("READER", name, false);
  }
  describeSubscription(name: string): Promise<ControlExecution> {
    return this.describe("SUBSCRIPTION", name, false);
  }
  describeRole(name: string): Promise<ControlExecution> {
    return this.describe("ROLE", name, false);
  }

  /** `RENAME <kind> <current> TO <new>` — feed/domain names must be dotted. */
  renameDomain(current: string, renamed: string): Promise<ControlExecution> {
    return this.rename("DOMAIN", current, renamed, true);
  }
  renameFeed(current: string, renamed: string): Promise<ControlExecution> {
    return this.rename("FEED", current, renamed, true);
  }
  renameWriter(current: string, renamed: string): Promise<ControlExecution> {
    return this.rename("WRITER", current, renamed, false);
  }
  renameReader(current: string, renamed: string): Promise<ControlExecution> {
    return this.rename("READER", current, renamed, false);
  }
  renameSubscription(current: string, renamed: string): Promise<ControlExecution> {
    return this.rename("SUBSCRIPTION", current, renamed, false);
  }
  renameRole(current: string, renamed: string): Promise<ControlExecution> {
    return this.rename("ROLE", current, renamed, false);
  }

  /** `DROP <kind> <name>` */
  dropDomain(name: string): Promise<ControlExecution> {
    return this.drop("DOMAIN", name, true);
  }
  dropFeed(name: string): Promise<ControlExecution> {
    return this.drop("FEED", name, true);
  }
  dropWriter(name: string): Promise<ControlExecution> {
    return this.drop("WRITER", name, false);
  }
  dropReader(name: string): Promise<ControlExecution> {
    return this.drop("READER", name, false);
  }
  dropSubscription(name: string): Promise<ControlExecution> {
    return this.drop("SUBSCRIPTION", name, false);
  }
  dropRole(name: string): Promise<ControlExecution> {
    return this.drop("ROLE", name, false);
  }

  /** `SHOW <kind>` — rows live in `results[0].data`. */
  listDomains(): Promise<ControlExecution> {
    return this.wcl("SHOW DOMAINS");
  }
  listFeeds(): Promise<ControlExecution> {
    return this.wcl("SHOW FEEDS");
  }
  listWriters(): Promise<ControlExecution> {
    return this.wcl("SHOW WRITERS");
  }
  listReaders(): Promise<ControlExecution> {
    return this.wcl("SHOW READERS");
  }
  listSubscriptions(): Promise<ControlExecution> {
    return this.wcl("SHOW SUBSCRIPTIONS");
  }
  listRoles(): Promise<ControlExecution> {
    return this.wcl("SHOW ROLES");
  }
  listGrants(): Promise<ControlExecution> {
    return this.wcl("SHOW GRANTS");
  }

  private describe(kind: string, name: string, dottedName: boolean): Promise<ControlExecution> {
    return this.wcl(`DESCRIBE ${kind} ${dottedName ? dotted(name) : resource(name)}`);
  }

  private rename(
    kind: string,
    current: string,
    renamed: string,
    dottedName: boolean,
  ): Promise<ControlExecution> {
    const check = dottedName ? dotted : resource;
    return this.wcl(`RENAME ${kind} ${check(current)} TO ${check(renamed)}`);
  }

  private drop(kind: string, name: string, dottedName: boolean): Promise<ControlExecution> {
    return this.wcl(`DROP ${kind} ${dottedName ? dotted(name) : resource(name)}`);
  }

  writer(name: string, sessionEpoch: number): WriterSessionClient {
    return new WriterSessionClient(this.transport, name, sessionEpoch);
  }

  /**
   * A codec-bound Feed facade — serialization is bound once so every
   * write encodes and every read decodes automatically:
   *
   * ```ts
   * const orders = client.feed("orders.events", codecs.json<Order>());
   * const writer = await orders.writer("ingest");
   * await writer.append("order-1", order);        // JSON-encoded
   * const reader = await orders.reader("audit", 64);
   * const page = await reader.fetch();            // FeedMessage<string, Order>[]
   * ```
   *
   * `client.feed(name)` binds the default `Feed<string, Buffer>` —
   * UTF-8 keys and untouched payload bytes, the escape hatch to the
   * wire. `client.feed(name, valueCodec)` keeps UTF-8 keys (the common
   * case); the three-argument form binds both codecs explicitly —
   * protobuf/msgpack/custom via {@link codecs.custom}.
   */
  feed(name: string): Feed<string, Buffer>;
  feed<V>(name: string, valueCodec: Codec<V>): Feed<string, V>;
  feed<K, V>(name: string, keyCodec: Codec<K>, valueCodec: Codec<V>): Feed<K, V>;
  feed<K, V>(name: string, a?: Codec<K> | Codec<V>, b?: Codec<V>): Feed<K, V> | Feed<string, V> | Feed<string, Buffer> {
    const keyCodec = b !== undefined ? (a as Codec<K>) : (codecs.utf8() as unknown as Codec<K>);
    const valueCodec = (b ?? a ?? codecs.bytes()) as Codec<V>;
    return new Feed<K, V>(this, dotted(name), keyCodec, valueCodec);
  }

  /**
   * A codec-bound StateStore facade — every `get`/`put`/`iterate`
   * serializes through the bound codecs. `client.store(name)` binds
   * `Store<string, Buffer>`; a second argument types the payload
   * (UTF-8 keys); three arguments bind both codecs. The byte-level
   * {@link stateStore} facade remains for raw access and declaration.
   */
  store(name: string): Store<string, Buffer>;
  store<V>(name: string, valueCodec: Codec<V>): Store<string, V>;
  store<K, V>(name: string, keyCodec: Codec<K>, valueCodec: Codec<V>): Store<K, V>;
  store<K, V>(name: string, a?: Codec<K> | Codec<V>, b?: Codec<V>): Store<K, V> | Store<string, V> | Store<string, Buffer> {
    const bound = StateStore.bound(this.transport, name);
    const keyCodec = b !== undefined ? (a as Codec<K>) : (codecs.utf8() as unknown as Codec<K>);
    const valueCodec = (b ?? a ?? codecs.bytes()) as Codec<V>;
    return Store.bind(bound, keyCodec, valueCodec);
  }

  reader(name: string): ReaderSessionClient {
    return new ReaderSessionClient(this.transport, name);
  }

  subscriptionMember(subscription: string): SubscriptionMemberClient {
    return new SubscriptionMemberClient(this.transport, subscription);
  }

  /**
   * Subscribe durably to a Feed: ensures a Reader named `reader` exists
   * (creating it at `start` when missing), opens a session, and invokes
   * `onRecords` per fetched page in the background, acknowledging after
   * each batch. The returned {@link Subscription} runs until `close()`;
   * terminal errors reach `onError` and `subscription.failure`.
   *
   * ```ts
   * const sub = client.subscribe("hr-view", "hr.employees",
   *   ReaderStart.beginning(), (records) => handle(records));
   * // ...later
   * await sub.close();
   * ```
   */
  subscribe(
    reader: string,
    feed: string,
    start: ReaderStart,
    onRecords: (records: FeedRecord[]) => void | Promise<void>,
    onError?: (error: WhitewaterError) => void,
    capacity = SUBSCRIBE_CAPACITY,
  ): Subscription {
    return new Subscription(
      this,
      reader,
      feed,
      start,
      capacity,
      onRecords,
      onError,
    );
  }

  /**
   * A thin facade over the node-side StateStore `name` — rows live on
   * the Nodes (RF3 + Fjall); this client holds no state.
   */
  stateStore(name: string): StateStore {
    return StateStore.bound(this.transport, name);
  }
}

/**
 * A thin facade over one node-side StateStore — the durable RF3 + Fjall
 * store on the Nodes is the only store; this object holds no rows.
 * Declare it with named keys, then put/get/delete/iterate:
 *
 * ```ts
 * const spec = StateStore.named("accounts.employees")
 *   .addKey("passport", true).addKey("department", false);
 * const store = await client.stateStores.define(spec);
 *
 * await store.put(Buffer.from("user-1"), Buffer.from("profile-1"),
 *   { passport: "P111", department: "eng" });
 * const row = await store.get(Buffer.from("user-1"));       // StoreRow | null
 * const rows = await store.getBy("department", "eng");      // every match
 * for await (const r of store.iterate()) { }                // key-ordered pages
 * await store.clear();
 * ```
 *
 * `addKey` records declaration intent used by `define`; it does not
 * mutate an already-declared store (named keys are declared at
 * creation). Named values are typed scalars — string, number/bigint,
 * boolean, Buffer, or null; an array value carries one typed value per
 * posting component.
 */
export class StateStore {
  /** Named secondary keys per store; matches the server declaration budget. */
  static readonly MAX_NAMED_KEYS = MAX_NAMED_KEYS;

  private readonly declaredKeys: NamedKey[] = [];

  private constructor(
    private transport: Transport | null,
    readonly name: string,
  ) {}

  /** An unbound declaration spec for {@link StateStoresClient.define}. */
  static named(name: string): StateStore {
    return new StateStore(null, dotted(name));
  }

  /** @internal */
  static bound(transport: Transport, name: string): StateStore {
    return new StateStore(transport, dotted(name));
  }

  /** Declare a named secondary key for `define` (at most MAX_NAMED_KEYS).
   * `extractor` declares how replay projects each source-Feed record to
   * this key's values — required on feed-sourced stores, refused on
   * manual ones. */
  addKey(namedKey: string, unique = false, extractor?: StoreExtractor): this {
    if (!namedKey || !namedKey.trim()) throw new Error("named key is required");
    if (this.declaredKeys.some((k) => k.name === namedKey)) {
      throw new Error(`duplicate named key: ${namedKey}`);
    }
    if (this.declaredKeys.length >= StateStore.MAX_NAMED_KEYS) {
      throw new Error(`a StateStore declares at most ${StateStore.MAX_NAMED_KEYS} named keys`);
    }
    this.declaredKeys.push({ name: namedKey, unique, extractor });
    return this;
  }

  get namedKeys(): NamedKey[] {
    return [...this.declaredKeys];
  }

  private boundTransport(): Transport {
    if (!this.transport) {
      throw new Error(
        "unbound StateStore spec — pass it to client.stateStores.define() " +
          "or get a bound facade via client.stateStore(name)",
      );
    }
    return this.transport;
  }

  /** @internal */
  bind(transport: Transport): this {
    this.transport = transport;
    return this;
  }

  /** Insert or replace one row. `named` maps a declared named key to a
   * typed value (or an array for multi-component postings). */
  async put(
    key: Buffer | string,
    payload: Buffer | string,
    named?: Record<string, StoreValue | StoreValue[]>,
    mutationId = randomUUID(),
  ): Promise<MutationResult> {
    const op: Record<string, unknown> = {
      kind: "put_row",
      key_base64: keyBytes(key).toString("base64"),
      payload_base64: keyBytes(payload).toString("base64"),
    };
    if (named && Object.keys(named).length > 0) op.named = wire.namedValues(named);
    return wire.mutationResult(
      await this.boundTransport().post("/v1/state-stores/mutate", {
        store: this.name,
        mutation_id: mutationId,
        op,
      }),
    );
  }

  /** Primary-key read with freshness evidence on the returned row; null
   * when absent. `strict` bounded-waits for the serving replica to apply
   * its committed frontier rather than answering stale. */
  async get(
    key: Buffer | string,
    options: { strict?: boolean; waitMs?: number } = {},
  ): Promise<StoreRow | null> {
    const response = (await this.boundTransport().post("/v1/state-stores/lookup", {
      store: this.name,
      key_base64: keyBytes(key).toString("base64"),
      strict: options.strict ?? false,
      ...(options.waitMs != null ? { wait_ms: options.waitMs } : {}),
    })) as Record<string, unknown>;
    if (response.found !== true) return null;
    const row: StoreRow = {
      key: keyBytes(key),
      payload: Buffer.from(String(response.payload_base64 ?? ""), "base64"),
      freshness: wire.storeFreshness(response),
    };
    if (response.applied_cursor != null) row.appliedCursor = String(response.applied_cursor);
    return row;
  }

  /** Named-key lookup: every primary row the value points at, plus the
   * serving replica's freshness evidence. */
  async getBy(
    namedKey: string,
    value: StoreValue | StoreValue[],
    options: { limit?: number; strict?: boolean; waitMs?: number } = {},
  ): Promise<StoreRows> {
    const values = Array.isArray(value) ? value : [value];
    return wire.storeRows(
      await this.boundTransport().post("/v1/state-stores/get-by", {
        store: this.name,
        named_key: namedKey,
        values: values.map(wire.indexValue),
        strict: options.strict ?? false,
        ...(options.limit != null ? { limit: options.limit } : {}),
        ...(options.waitMs != null ? { wait_ms: options.waitMs } : {}),
      }),
    );
  }

  /** Remove the row and its named postings. */
  async delete(key: Buffer | string, mutationId = randomUUID()): Promise<MutationResult> {
    return wire.mutationResult(
      await this.boundTransport().post("/v1/state-stores/mutate", {
        store: this.name,
        mutation_id: mutationId,
        op: { kind: "delete_row", key_base64: keyBytes(key).toString("base64") },
      }),
    );
  }

  /** One bounded iteration page in primary-key order — the same row set
   * replica transfer serves, pinned to `pinnedSeq`. */
  async page(afterKey?: Buffer, limit?: number): Promise<StorePage> {
    return wire.storePage(
      await this.boundTransport().post("/v1/state-stores/iterate", {
        store: this.name,
        ...(afterKey ? { after_key_base64: afterKey.toString("base64") } : {}),
        ...(limit != null ? { limit } : {}),
      }),
    );
  }

  /** Async-iterate every row in primary-key order across bounded pages. */
  async *iterate(limit?: number): AsyncGenerator<StoreRow> {
    let after: Buffer | undefined;
    for (;;) {
      const page = await this.page(after, limit);
      for (const row of page.rows) yield row;
      if (!page.nextKey) return;
      after = page.nextKey;
    }
  }

  /** Empty the store as one journaled mutation — durable and idempotent
   * under `mutationId` retries. */
  async clear(mutationId = randomUUID()): Promise<MutationResult> {
    return wire.mutationResult(
      await this.boundTransport().post("/v1/state-stores/clear", {
        store: this.name,
        mutation_id: mutationId,
      }),
    );
  }

  /** Tombstone the store: the name cannot be recreated and every data
   * path refuses. Applied rows remain on replicas until reclaim. */
  async drop(requestId?: string): Promise<ControlExecution> {
    return wire.controlExecution(
      await this.boundTransport().post("/v1/admin/commands", {
        ...(requestId ? { request_id: requestId } : {}),
        commands: [{ command: "drop", kind: "state_store", name: this.name }],
      }),
    );
  }
}

function keyBytes(value: Buffer | string): Buffer {
  return typeof value === "string" ? Buffer.from(value, "utf8") : value;
}

/** Collection-level StateStore operations: declaration and drop. Row
 * operations live on the bound {@link StateStore} facade returned by
 * `client.stateStore(name)`. */
export class StateStoresClient {
  constructor(private readonly transport: Transport) {}

  /**
   * Declare `spec` on the Control Plane and return it bound to this
   * client. `source` names a same-Domain Feed the store materializes
   * from; omitted means a manual store. Feed-sourced named keys must
   * each declare an `extractor` so replay can project typed values;
   * extractors on a manual store are refused.
   */
  async define(spec: StateStore, source?: string, requestId?: string): Promise<StateStore> {
    if (!(spec instanceof StateStore)) {
      throw new Error("define() takes a StateStore spec — use StateStore.named(name)");
    }
    if (!source && spec.namedKeys.some((k) => k.extractor !== undefined)) {
      throw new Error(
        "named-key extractors apply to feed-sourced StateStores; " +
          "manual stores take named values on each put",
      );
    }
    if (source && spec.namedKeys.some((k) => k.extractor === undefined)) {
      throw new Error(
        "feed-sourced named keys require an extractor " +
          "(Extractor.recordKey()/metadata()/json())",
      );
    }
    await this.transport.post("/v1/admin/commands", {
      ...(requestId ? { request_id: requestId } : {}),
      commands: [
        {
          command: "define_state_store",
          name: spec.name,
          source: source ? { kind: "feed", feed: dotted(source) } : { kind: "manual" },
          named_keys: spec.namedKeys.map((k) => ({
            name: k.name,
            unique: k.unique,
            ...(k.extractor ? { extractor: k.extractor } : {}),
          })),
        },
      ],
    });
    return spec.bind(this.transport);
  }

  /** Tombstone the named store. */
  async drop(name: string, requestId?: string): Promise<ControlExecution> {
    return StateStore.bound(this.transport, name).drop(requestId);
  }
}

const SUBSCRIBE_CAPACITY = 128;

/** Lowercase dotted names: `^[a-z][a-z0-9]*(\.[a-z][a-z0-9]*)*$`, ≤512 bytes. */
function dotted(name: string): string {
  if (
    name.length > 512 ||
    name.startsWith(".") ||
    name.endsWith(".") ||
    !/^[a-z][a-z0-9]*(\.[a-z][a-z0-9]*)*$/.test(name)
  ) {
    throw new Error(`invalid dotted name: ${name}`);
  }
  return name;
}

/** Single-segment resource names (writers, readers, subscriptions, roles). */
function resource(name: string): string {
  if (!/^[A-Za-z][A-Za-z0-9_-]{0,127}$/.test(name)) {
    throw new Error(`invalid resource name: ${name}`);
  }
  return name;
}
