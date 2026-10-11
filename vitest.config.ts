import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    // Live three-Node conformance: quorum appends are ~200ms each, and a
    // scenario is a dozen round trips — well past the 5s default.
    testTimeout: 60_000,
  },
});
