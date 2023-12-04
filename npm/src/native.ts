// Copyright 2023 the Deno authors. All rights reserved. MIT license.

import { KvService } from "./kv_types.ts";

/**
 * Creates a new KvService instance that can be used to access Deno's native implementation (only works in the Deno runtime!)
 *
 * Requires the --unstable flag to `deno run` and any applicable --allow-read/allow-write/allow-net flags
 */
export function makeNativeService(): KvService {
  if ("Deno" in globalThis) {
    // deno-lint-ignore no-explicit-any
    const { openKv } = (globalThis as any).Deno;
    if (typeof openKv === "function") {
      return {
        // deno-lint-ignore no-explicit-any
        openKv: openKv as any,
      };
    }
  }
  throw new Error(`Global 'Deno.openKv' not found`);
}
