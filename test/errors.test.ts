import { describe, expect, it } from "vitest";
import {
  AmbiguousResult,
  Conflict,
  LeaseConflict,
  Refused,
  StaleSession,
  Unavailable,
  mapError,
} from "../src/errors.js";

describe("mapError", () => {
  it("maps wire codes to typed errors", () => {
    const stale = mapError(409, { error: "epoch superseded", code: "stale_epoch" });
    expect(stale).toBeInstanceOf(StaleSession);
    expect(stale.code).toBe("stale_epoch");

    for (const code of ["lease.busy", "lease.stale_lease", "lease.capacity"]) {
      const error = mapError(409, { error: "x", code });
      expect(error).toBeInstanceOf(LeaseConflict);
      expect(error.code).toBe(code);
    }
  });

  it("falls back to status class without a code", () => {
    expect(mapError(409, { error: "fencing" })).toBeInstanceOf(Conflict);
    expect(
      mapError(503, { error: "frozen for split cutover; retry safely" }),
    ).toBeInstanceOf(Unavailable);
    expect(mapError(500, { error: "internal" })).toBeInstanceOf(AmbiguousResult);
    expect(mapError(400, { error: "bad name" })).toBeInstanceOf(Refused);
  });

  it("keeps writer.stale_sequence a conflict for raw appends", () => {
    const error = mapError(409, { error: "seq hole", code: "writer.stale_sequence" });
    expect(error).toBeInstanceOf(Conflict);
    expect(error.code).toBe("writer.stale_sequence");
  });

  it("handles malformed bodies via status text", () => {
    const error = mapError(502, null);
    expect(error).toBeInstanceOf(AmbiguousResult);
    expect(error.message).toContain("502");
  });
});
