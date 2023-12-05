// Copyright 2023 the Deno authors. All rights reserved. MIT license.

import { KvEntryMaybe } from "./kv_types.ts";
import { defer } from "./proto/runtime/async/observer.ts";

// take an underlying raw kv watch stream, and create a deferred stream that dedups (by key+versionstamp) based on when it's pulled
export function makeUnrawWatchStream(
  rawWatchStream: ReadableStream<KvEntryMaybe<unknown>[]>,
  onCancel: () => void | Promise<void>,
): ReadableStream<KvEntryMaybe<unknown>[]> {
  let pulled: KvEntryMaybe<unknown>[] | undefined;
  let latest: KvEntryMaybe<unknown>[] | undefined;
  let signal = defer<void>();
  let cancelled = false;
  return new ReadableStream({
    start(controller) {
      (async () => {
        const reader = rawWatchStream.getReader();
        while (true) {
          const { done, value: entries } = await reader.read();
          if (done) break;
          if (cancelled) break;
          latest = entries;
          signal.resolve();
          signal = defer<void>();
        }
        await reader.cancel();
        signal.resolve();
        try {
          controller.close();
        } catch { /* noop */ }
      })();
    },
    async pull(controller) {
      if (!latest) await signal;
      if (!latest) return;
      while (true) {
        let changed = false;
        if (pulled) {
          for (let i = 0; i < latest.length; i++) {
            if (latest[i].versionstamp === pulled[i].versionstamp) continue;
            changed = true;
            break;
          }
        } else {
          pulled = latest;
          changed = pulled.some((v) => v.versionstamp !== null);
        }
        if (changed) {
          pulled = latest;
          controller.enqueue(pulled);
          return;
        } else {
          await signal;
        }
      }
    },
    async cancel() {
      cancelled = true;
      await onCancel();
    },
  }, {
    highWaterMark: 0, // ensure all pulls are user-initiated
  });
}
