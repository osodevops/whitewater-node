/**
 * Minimal JSON-over-HTTP transport: round-robins the configured Node
 * endpoints, attaches the bearer key, and maps (status, code) pairs to
 * typed errors via {@link mapError}. Wire serialization lives in
 * `wire.ts`; this file owns the HTTP round trip and nothing else.
 */

import { mapError, Unavailable } from "./errors.js";

export class Transport {
  private next = 0;

  constructor(
    private readonly endpoints: string[],
    private readonly apiKey: string,
    private readonly timeoutMs = 60_000,
  ) {
    if (endpoints.length === 0) {
      throw new Error("at least one endpoint is required");
    }
    this.endpoints = endpoints.map((e) => e.replace(/\/+$/, ""));
  }

  private endpoint(): string {
    const endpoint = this.endpoints[this.next % this.endpoints.length];
    this.next += 1;
    return endpoint;
  }

  async get(path: string): Promise<unknown> {
    return this.roundTrip(() =>
      fetch(this.endpoint() + path, {
        method: "GET",
        headers: this.headers(),
        signal: AbortSignal.timeout(this.timeoutMs),
      }),
    );
  }

  async post(path: string, body: unknown): Promise<unknown> {
    return this.roundTrip(() =>
      fetch(this.endpoint() + path, {
        method: "POST",
        headers: { ...this.headers(), "content-type": "application/json" },
        body: JSON.stringify(body),
        signal: AbortSignal.timeout(this.timeoutMs),
      }),
    );
  }

  private headers(): Record<string, string> {
    return { authorization: `Bearer ${this.apiKey}`, accept: "application/json" };
  }

  private async roundTrip(send: () => Promise<Response>): Promise<unknown> {
    let response: Response;
    try {
      response = await send();
    } catch (error) {
      throw new Unavailable(0, null, `Node unreachable: ${(error as Error).message}`);
    }
    const text = await response.text();
    let body: unknown = null;
    try {
      body = text ? JSON.parse(text) : null;
    } catch {
      body = null;
    }
    if (response.status >= 200 && response.status < 300) {
      return body;
    }
    throw mapError(response.status, body);
  }
}
