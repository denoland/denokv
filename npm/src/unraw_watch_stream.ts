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
  let n = 1;
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
      if (n++ === 1) return; // assume the first pull is not user-initiated
      if (!latest) await signal;
      if (!latest) return;
      if (!pulled) {
        pulled = latest;
        controller.enqueue(pulled);
        return;
      }
      let changed = false;
      for (let i = 0; i < latest.length; i++) {
        if (latest[i].versionstamp === pulled[i].versionstamp) continue;
        changed = true;
        break;
      }
      if (changed) {
        pulled = latest;
        controller.enqueue(pulled);
      }
    },
    async cancel() {
      cancelled = true;
      await onCancel();
    },
  });
}
