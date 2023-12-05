// Copyright 2023 the Deno authors. All rights reserved. MIT license.

import { assertInstanceOf } from "https://deno.land/std@0.208.0/assert/assert_instance_of.ts";
import { RedBlackTree } from "https://deno.land/std@0.208.0/data_structures/red_black_tree.ts";
import { compareBytes, equalBytes } from "./bytes.ts";
import { packKey, unpackKey } from "./kv_key.ts";
import {
  AtomicCheck,
  KvCommitError,
  KvCommitResult,
  KvConsistencyLevel,
  KvEntry,
  KvEntryMaybe,
  KvKey,
  KvListOptions,
  KvListSelector,
  KvService,
} from "./kv_types.ts";
import { _KvU64 } from "./kv_u64.ts";
import {
  BaseKv,
  CursorHolder,
  Enqueue,
  Expirer,
  isValidVersionstamp,
  KvMutation,
  packCursor,
  packVersionstamp,
  QueueHandler,
  QueueWorker,
  replacer,
  unpackCursor,
} from "./kv_util.ts";
import { makeUnrawWatchStream } from "./unraw_watch_stream.ts";
import {
  checkOptionalBoolean,
  checkOptionalNumber,
  checkRecord,
} from "./check.ts";

export interface InMemoryServiceOptions {
  /** Enable some console logging */
  readonly debug?: boolean;

  /** Maximum number of attempts to deliver a failing queue message before giving up. Defaults to 10. */
  readonly maxQueueAttempts?: number;
}

/**
 * Return a new KvService that creates ephemeral in-memory KV instances.
 */
export function makeInMemoryService(
  opts: InMemoryServiceOptions = {},
): KvService {
  checkRecord("opts", opts);
  checkOptionalBoolean("opts.debug", opts.debug);
  checkOptionalNumber("opts.maxQueueAttempts", opts.maxQueueAttempts);
  const { debug = false, maxQueueAttempts = 10 } = opts;
  const instances = new Map<string, InMemoryKv>();
  return {
    openKv: (url = "") => {
      let kv = instances.get(url);
      if (!kv) {
        if (debug) console.log(`makeInMemoryService: new kv(${url})`);
        kv = new InMemoryKv(debug, maxQueueAttempts);
        instances.set(url, kv);
      }
      return Promise.resolve(kv);
    },
  };
}

//

type KVRow = [
  keyBytes: Uint8Array,
  value: unknown,
  versionstamp: string,
  expires: number | undefined,
];

const keyRow = (
  keyBytes: Uint8Array,
): KVRow => [keyBytes, undefined, "", undefined];

const copyValueIfNecessary = (v: unknown) =>
  v instanceof _KvU64 ? v : structuredClone(v);

type QueueItem = {
  id: number;
  value: unknown;
  enqueued: number;
  available: number;
  failures: number;
  keysIfUndelivered: KvKey[];
  locked: boolean;
};

type Watch = {
  keysAsBytes: Uint8Array[];
  onEntries: (entries: KvEntryMaybe<unknown>[]) => void;
  onFinalize: () => void;
};

class InMemoryKv extends BaseKv {
  private readonly rows = new RedBlackTree<KVRow>((a, b) =>
    compareBytes(a[0], b[0])
  ); // keep sorted by keyBytes
  private readonly queue = new Map<number, QueueItem>();
  private readonly watches = new Map<number, Watch>();
  private readonly expirer: Expirer;
  private readonly queueWorker: QueueWorker;
  private readonly maxQueueAttempts: number;

  private version = 0;
  private nextQueueItemId = 1;

  constructor(debug: boolean, maxQueueAttempts: number) {
    super({ debug });
    this.expirer = new Expirer(debug, () => this.expire());
    this.queueWorker = new QueueWorker((queueHandler) =>
      this.runWorker(queueHandler)
    );
    this.maxQueueAttempts = maxQueueAttempts;
  }

  protected get_<T = unknown>(
    key: KvKey,
    _consistency: KvConsistencyLevel | undefined,
  ): Promise<KvEntryMaybe<T>> {
    return Promise.resolve(this.getOne(key));
  }

  protected getMany_(
    keys: readonly KvKey[],
    _consistency: KvConsistencyLevel | undefined,
  ): Promise<KvEntryMaybe<unknown>[]> {
    return Promise.resolve(keys.map((v) => this.getOne(v)));
  }

  protected async *listStream<T>(
    outCursor: CursorHolder,
    selector: KvListSelector,
    { batchSize, consistency: _, cursor: cursorOpt, limit, reverse = false }:
      KvListOptions = {},
  ): AsyncGenerator<KvEntry<T>> {
    let yielded = 0;
    if (typeof limit === "number" && yielded >= limit) return;
    const cursor = typeof cursorOpt === "string"
      ? unpackCursor(cursorOpt)
      : undefined;
    let lastYieldedKeyBytes = cursor?.lastYieldedKeyBytes;
    let pass = 0;
    const prefixBytes = "prefix" in selector
      ? packKey(selector.prefix)
      : undefined;
    while (true) {
      pass++;
      let start: Uint8Array | undefined;
      let end: Uint8Array | undefined;
      if ("prefix" in selector) {
        start = "start" in selector ? packKey(selector.start) : prefixBytes;
        end = "end" in selector
          ? packKey(selector.end)
          : new Uint8Array([...prefixBytes!, 0xff]);
      } else {
        start = packKey(selector.start);
        end = packKey(selector.end);
      }
      if (reverse) {
        end = lastYieldedKeyBytes ?? end;
      } else {
        start = lastYieldedKeyBytes ?? start;
      }

      if (start === undefined || end === undefined) throw new Error();
      if (start.length === 0) start = new Uint8Array([0]);
      const batchLimit =
        Math.min(batchSize ?? 100, 500, limit ?? Number.MAX_SAFE_INTEGER) +
        (lastYieldedKeyBytes ? 1 : 0);

      const rows = this.listRows({ start, end, reverse, batchLimit });
      let entries = 0;
      for (const [keyBytes, value, versionstamp] of rows) {
        if (
          entries++ === 0 &&
          (lastYieldedKeyBytes && equalBytes(lastYieldedKeyBytes, keyBytes) ||
            prefixBytes && equalBytes(prefixBytes, keyBytes))
        ) continue;
        const key = unpackKey(keyBytes);
        lastYieldedKeyBytes = keyBytes;
        outCursor.set(packCursor({ lastYieldedKeyBytes })); // cursor needs to be set before yield
        yield { key, value: copyValueIfNecessary(value) as T, versionstamp };
        yielded++;
        // console.log({ yielded, entries, limit });
        if (typeof limit === "number" && yielded >= limit) return;
      }
      if (entries < batchLimit) return;
    }
  }

  protected listenQueue_(
    handler: (value: unknown) => void | Promise<void>,
  ): Promise<void> {
    return this.queueWorker.listen(handler);
  }

  protected async commit(
    checks: AtomicCheck[],
    mutations: KvMutation[],
    enqueues: Enqueue[],
    additionalWork?: (() => void) | undefined,
  ): Promise<KvCommitResult | KvCommitError> {
    const { rows, queue, watches } = this;
    await Promise.resolve();
    for (const { key, versionstamp } of checks) {
      if (
        !(versionstamp === null ||
          typeof versionstamp === "string" && isValidVersionstamp(versionstamp))
      ) throw new Error(`Bad 'versionstamp': ${versionstamp}`);
      const existing = rows.find(keyRow(packKey(key)));
      if (versionstamp === null && existing) return { ok: false };
      if (
        typeof versionstamp === "string" && (existing ?? [])[2] !== versionstamp
      ) return { ok: false };
    }
    let minExpires: number | undefined;
    let minEnqueued: number | undefined;
    const newVersionstamp = packVersionstamp(++this.version);
    for (const { value, opts = {} } of enqueues) {
      const { delay = 0, keysIfUndelivered = [] } = opts;
      const enqueued = Date.now();
      const available = enqueued + delay;
      const id = this.nextQueueItemId++;
      queue.set(id, {
        id,
        value: copyValueIfNecessary(value),
        enqueued,
        available,
        failures: 0,
        keysIfUndelivered,
        locked: false,
      });
      minEnqueued = Math.min(enqueued, minEnqueued ?? Number.MAX_SAFE_INTEGER);
    }
    const watchIds = new Set<number>();
    for (const mutation of mutations) {
      const { key } = mutation;
      const keyBytes = packKey(key);
      if (mutation.type === "set") {
        const { value, expireIn } = mutation;
        const expires = typeof expireIn === "number"
          ? Date.now() + Math.round(expireIn)
          : undefined;
        if (expires !== undefined) {
          minExpires = Math.min(expires, minExpires ?? Number.MAX_SAFE_INTEGER);
        }
        rows.remove(keyRow(keyBytes));
        rows.insert([
          keyBytes,
          copyValueIfNecessary(value),
          newVersionstamp,
          expires,
        ]);
      } else if (mutation.type === "delete") {
        rows.remove(keyRow(keyBytes));
      } else if (
        mutation.type === "sum" || mutation.type === "min" ||
        mutation.type === "max"
      ) {
        const existing = rows.find(keyRow(keyBytes));
        if (!existing) {
          rows.insert([keyBytes, mutation.value, newVersionstamp, undefined]);
        } else {
          const existingValue = existing[1];
          assertInstanceOf(
            existingValue,
            _KvU64,
            `Can only '${mutation.type}' on KvU64`,
          );
          const result = mutation.type === "min"
            ? existingValue.min(mutation.value)
            : mutation.type === "max"
            ? existingValue.max(mutation.value)
            : existingValue.sum(mutation.value);
          rows.remove(keyRow(keyBytes));
          rows.insert([keyBytes, result, newVersionstamp, undefined]);
        }
      } else {
        throw new Error(
          `commit(${
            JSON.stringify({ checks, mutations, enqueues }, replacer)
          }) not implemented`,
        );
      }
      for (const [watchId, watch] of watches) {
        if (watchIds.has(watchId)) continue;
        if (watch.keysAsBytes.some((v) => equalBytes(v, keyBytes))) {
          watchIds.add(watchId);
        }
      }
    }
    if (additionalWork) additionalWork();
    if (minExpires !== undefined) this.expirer.rescheduleExpirer(minExpires);
    if (minEnqueued !== undefined) this.queueWorker.rescheduleWorker();
    for (const watchId of watchIds) {
      const watch = watches.get(watchId);
      if (watch) {
        watch.onEntries(
          watch.keysAsBytes.map((v) => this.getOne(unpackKey(v))),
        );
      }
    }

    return { ok: true, versionstamp: newVersionstamp };
  }

  protected watch_(
    keys: readonly KvKey[],
    raw: boolean | undefined,
  ): ReadableStream<KvEntryMaybe<unknown>[]> {
    const { watches, debug } = this;
    const keysAsBytes = keys.map(packKey);
    const watchId = [...watches.keys()].reduce((a, b) => Math.max(a, b), 0) + 1;
    let onCancel: (() => void) | undefined;
    const rawStream = new ReadableStream({
      start(controller) {
        if (debug) console.log(`watch: ${watchId} start`);
        const tryClose = () => {
          try {
            controller.close();
          } catch { /* noop */ }
        };
        onCancel = tryClose;
        watches.set(watchId, {
          keysAsBytes,
          onEntries: (entries) => controller.enqueue(entries),
          onFinalize: tryClose,
        });
      },
      cancel() {
        watches.delete(watchId);
        if (debug) console.log(`watch: ${watchId} cancel`);
      },
    });
    return raw ? rawStream : makeUnrawWatchStream(rawStream, onCancel!);
  }

  protected close_(): void {
    this.expirer.finalize();
    this.queueWorker.finalize();
    this.watches.forEach((v) => v.onFinalize());
  }

  //

  private getOne<T>(key: KvKey): KvEntryMaybe<T> {
    const row = this.rows.find(keyRow(packKey(key)));
    return row
      ? { key, value: copyValueIfNecessary(row[1]) as T, versionstamp: row[2] }
      : { key, value: null, versionstamp: null };
  }

  private listRows(
    { start, end, reverse, batchLimit }: {
      start: Uint8Array;
      end: Uint8Array;
      reverse: boolean;
      batchLimit: number;
    },
  ): KVRow[] {
    const { rows } = this;
    const rt: KVRow[] = [];
    for (const row of reverse ? rows.rnlValues() : rows.lnrValues()) { // TODO use rbt to find start node faster
      const keyBytes = row[0];
      if (rt.length >= batchLimit) break;
      if (reverse) {
        if (compareBytes(keyBytes, start) < 0) break;
        if (compareBytes(keyBytes, end) < 0) rt.push(row);
      } else {
        if (compareBytes(keyBytes, end) >= 0) break;
        if (compareBytes(keyBytes, start) >= 0) rt.push(row);
      }
    }
    return rt;
  }

  private expire(): number | undefined {
    const { rows } = this;
    const now = Date.now();
    const remove: KVRow[] = [];
    let minExpires: number | undefined;
    for (const row of rows) {
      const expires = row[3];
      if (expires !== undefined && expires <= now) {
        remove.push(row);
      }
      minExpires = expires === undefined
        ? minExpires
        : Math.min(expires, minExpires ?? Number.MAX_SAFE_INTEGER);
    }
    remove.forEach((v) => rows.remove(v));
    return minExpires;
  }

  private async runWorker(queueHandler?: QueueHandler) {
    const { debug, queue } = this;
    if (!queueHandler) {
      if (debug) console.log(`runWorker: no queueHandler`);
      return;
    }
    const time = Date.now();
    const candidateIds = [...queue.values()].filter((v) =>
      v.available <= time && !v.locked
    ).map((v) => v.id);
    if (candidateIds.length === 0) {
      const nextAvailableItem = [...queue.values()].filter((v) =>
        !v.locked
      ).sort((a, b) => a.available - b.available)[0];
      if (nextAvailableItem) {
        const nextAvailableIn = nextAvailableItem.available - Date.now();
        if (debug) {
          console.log(
            `runWorker: no work (nextAvailableIn=${nextAvailableIn}ms)`,
          );
        }
        this.queueWorker.rescheduleWorker(nextAvailableIn);
      } else {
        if (debug) {
          console.log("runWorker: no work");
        }
      }
      return;
    }
    const id = Math.min(...candidateIds);
    const queueItem = queue.get(id)!;
    queueItem.locked = true;
    const { value, failures, keysIfUndelivered } = queueItem;
    const deleteQueueItem = () => queue.delete(id);
    try {
      if (debug) console.log(`runWorker: dispatching ${id}: ${value}`);
      await Promise.resolve(queueHandler(value));
      if (debug) console.log(`runWorker: ${id} succeeded, clearing`);
      deleteQueueItem();
    } catch (e) {
      const totalFailures = failures + 1;
      if (debug) {
        console.log(
          `runWorker: ${id} failed (totalFailures=${totalFailures}): ${
            e.stack || e
          }`,
        );
      }
      if (totalFailures >= this.maxQueueAttempts) {
        let atomic = this.atomic(deleteQueueItem);
        for (const key of keysIfUndelivered) atomic = atomic.set(key, value);
        await atomic.commit();
        if (debug) {
          console.log(
            `runWorker: give up on ${id}, keys=${keysIfUndelivered.length}`,
          );
        }
      } else {
        const available = Date.now() + 1000 * totalFailures;
        queueItem.failures = totalFailures;
        queueItem.available = available;
        queueItem.locked = false;
      }
    }
    this.queueWorker.rescheduleWorker();
  }
}
