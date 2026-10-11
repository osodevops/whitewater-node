/**
 * Whitewater error hierarchy mapped from wire (status, code) pairs.
 *
 * `Unavailable` covers transient refusals the caller may retry after
 * `feedback.retryAfterMs` guidance; `AmbiguousResult` means the
 * operation may have committed — idempotent retry via the same
 * `request_id` is the safe path, never blind re-issue.
 */

export class WhitewaterError extends Error {
  constructor(
    readonly status: number,
    readonly code: string | null,
    message: string,
  ) {
    super(message);
    this.name = new.target.name;
  }
}

/** Non-retryable refusal (4xx without a more specific code). */
export class Refused extends WhitewaterError {}
/** 409 refusal — fencing, epoch, or sequencing conflict. */
export class Conflict extends WhitewaterError {}
/** `stale_epoch` — the Writer/Reader session was superseded. */
export class StaleSession extends Conflict {}
/** `lease.*` — subscription work lease contested, stale, or full. */
export class LeaseConflict extends Conflict {}
/** Transient: node unreachable, 503, or frozen-for-cutover. */
export class Unavailable extends WhitewaterError {}
/** The operation may or may not have committed (5xx, timeout). */
export class AmbiguousResult extends WhitewaterError {}

const LEASE_CODES = new Set(["lease.busy", "lease.stale_lease", "lease.capacity"]);

export function mapError(status: number, body: unknown): WhitewaterError {
  const obj = body && typeof body === "object" ? (body as Record<string, unknown>) : {};
  const message = typeof obj.error === "string" ? obj.error : `http ${status}`;
  const code = typeof obj.code === "string" ? obj.code : null;
  if (code === "stale_epoch") return new StaleSession(status, code, message);
  if (code !== null && LEASE_CODES.has(code)) return new LeaseConflict(status, code, message);
  if (status === 409) return new Conflict(status, code, message);
  if (status === 503 || status === 0) return new Unavailable(status, code, message);
  if (status >= 500) return new AmbiguousResult(status, code, message);
  return new Refused(status, code, message);
}
