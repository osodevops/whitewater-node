/**
 * Whitewater client facades: Admin, Writer session, Reader session, and
 * Subscription member — mirroring the conformance fixture operation
 * vocabulary. Every call is async; endpoint round-robining and error
 * classification live in {@link Transport}.
 */

import { randomUUID } from "node:crypto";
import { Conflict } from "./errors.js";
import type {
  AppendBatchResult,
  AppendResult,
  ControlExecution,
  Cursor,
  FeedAppend,
  FeedRecord,
  Joined,
  MemberPage,
  ReaderPage,
  ReaderSession,
  TemporaryFetchOptions,
  TemporaryFetchPage,
  WorkLease,
} from "./models.js";
import { Transport } from "./transport.js";
import * as wire from "./wire.js";

export interface AppendInput {
  requestId?: string;
  key?: Buffer | null;
  payload: Buffer;
  eventTimeNs?: bigint | null;
  metadata?: Record<string, Buffer>;
}

export class AdminClient {
  constructor(private readonly transport: Transport) {}

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
}

export class WriterSessionClient {
  constructor(
    private readonly transport: Transport,
    private readonly writer: string,
    private readonly sessionEpoch: number,
  ) {}

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
  constructor(private readonly transport: Transport) {}

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
    return wire.appendResult(
      await this.transport.post("/v1/feeds/append", wire.feedAppendRequest(feed, record)),
    );
  }
}

export interface WhitewaterClientOptions {
  apiKey: string;
  endpoints: string[];
  timeoutMs?: number;
}

export class WhitewaterClient {
  readonly admin: AdminClient;
  readonly feeds: FeedClient;
  /** Shared transport; exposed for low-level/operator calls not yet
   * covered by a typed facade. */
  readonly transport: Transport;

  constructor(options: WhitewaterClientOptions) {
    this.transport = new Transport(options.endpoints, options.apiKey, options.timeoutMs);
    this.admin = new AdminClient(this.transport);
    this.feeds = new FeedClient(this.transport);
  }

  writer(name: string, sessionEpoch: number): WriterSessionClient {
    return new WriterSessionClient(this.transport, name, sessionEpoch);
  }

  reader(name: string): ReaderSessionClient {
    return new ReaderSessionClient(this.transport, name);
  }

  subscriptionMember(subscription: string): SubscriptionMemberClient {
    return new SubscriptionMemberClient(this.transport, subscription);
  }
}
