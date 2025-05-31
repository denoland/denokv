// Copyright 2023 the Deno authors. All rights reserved. MIT license.

import { decodeHex, encodeHex, equalBytes } from "./bytes.ts";
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
  KvMutation,
} from "./kv_types.ts";
import {
  BaseKv,
  CursorHolder,
  DecodeV8,
  EncodeV8,
  Enqueue,
  packCursor,
  packKvValue,
  readValue,
  unpackCursor,
} from "./kv_util.ts";
import {
  AtomicWrite,
  AtomicWriteOutput,
  Check as KvCheckMessage,
  Enqueue as EnqueueMessage,
  Mutation as KvMutationMessage,
  ReadRange,
  SnapshotRead,
  SnapshotReadOutput,
  WatchKeyOutput,
} from "./proto/messages/com/deno/kv/datapath/index.ts";
export { UnknownV8 } from "./v8.ts";

export abstract class ProtoBasedKv extends BaseKv {
  protected readonly decodeV8: DecodeV8;
  protected readonly encodeV8: EncodeV8;

  constructor(debug: boolean, decodeV8: DecodeV8, encodeV8: EncodeV8) {
    super({ debug });
    this.decodeV8 = decodeV8;
    this.encodeV8 = encodeV8;
  }

  protected abstract snapshotRead(
    req: SnapshotRead,
    consistency: KvConsistencyLevel | undefined,
  ): Promise<SnapshotReadOutput>;
  protected abstract atomicWrite(req: AtomicWrite): Promise<AtomicWriteOutput>;

  protected async get_<T = unknown>(
    key: KvKey,
    consistency?: KvConsistencyLevel,
  ): Promise<KvEntryMaybe<T>> {
    const { decodeV8 } = this;
    const packedKey = packKey(key);
    const req: SnapshotRead = {
      ranges: [computeReadRangeForKey(packedKey)],
    };
    const res = await this.snapshotRead(req, consistency);
    for (const range of res.ranges) {
      for (const item of range.values) {
        if (equalBytes(item.key, packedKey)) {
          return {
            key,
            value: readValue(item.value, item.encoding, decodeV8) as T,
            versionstamp: encodeHex(item.versionstamp),
          };
        }
      }
    }
    return { key, value: null, versionstamp: null };
  }

  protected async getMany_(
    keys: readonly KvKey[],
    consistency?: KvConsistencyLevel,
  ): Promise<KvEntryMaybe<unknown>[]> {
    const { decodeV8 } = this;
    const packedKeys = keys.map(packKey);
    const packedKeysHex = packedKeys.map(encodeHex);
    const req: SnapshotRead = {
      ranges: packedKeys.map(computeReadRangeForKey),
    };
    const res = await this.snapshotRead(req, consistency);
    const rowMap = new Map<
      string,
      { key: KvKey; value: unknown; versionstamp: string }
    >();
    for (const range of res.ranges) {
      for (const { key, value, encoding, versionstamp } of range.values) {
        rowMap.set(encodeHex(key), {
          key: unpackKey(key),
          value: readValue(value, encoding, decodeV8),
          versionstamp: encodeHex(versionstamp),
        });
      }
    }
    return keys.map((key, i) => {
      const row = rowMap.get(packedKeysHex[i]);
      return row
        ? { key, value: row.value, versionstamp: row.versionstamp }
        : { key, value: null, versionstamp: null };
    });
  }

  protected async commit(
    checks: AtomicCheck[],
    mutations: KvMutation[],
    enqueues: Enqueue[],
  ): Promise<KvCommitResult | KvCommitError> {
    const write: AtomicWrite = {
      checks: checks.map(computeKvCheckMessage),
      mutations: mutations.map((v) =>
        computeKvMutationMessage(v, this.encodeV8)
      ),
      enqueues: enqueues.map(({ value, opts }) =>
        computeEnqueueMessage(value, this.encodeV8, opts)
      ),
    };
    const { status, versionstamp } = await this.atomicWrite(write);
    if (status === "AW_CHECK_FAILURE") return { ok: false };
    if (status !== "AW_SUCCESS") {
      throw new Error(`commit failed with status: ${status}`);
    }
    return { ok: true, versionstamp: encodeHex(versionstamp) };
  }

  protected async *listStream<T>(
    outCursor: CursorHolder,
    selector: KvListSelector,
    { batchSize, consistency, cursor: cursorOpt, limit, reverse = false }:
      KvListOptions = {},
  ): AsyncGenerator<KvEntry<T>> {
    const { decodeV8 } = this;
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
      // console.log({ pass });
      const req: SnapshotRead = { ranges: [] };
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
      const batchLimit =
        Math.min(batchSize ?? 100, 500, limit ?? Number.MAX_SAFE_INTEGER) +
        (lastYieldedKeyBytes ? 1 : 0);
      req.ranges.push({ start, end, limit: batchLimit, reverse });

      const res = await this.snapshotRead(req, consistency);
      let entries = 0;
      for (const range of res.ranges) {
        for (const entry of range.values) {
          if (
            entries++ === 0 &&
            (lastYieldedKeyBytes &&
                equalBytes(lastYieldedKeyBytes, entry.key) ||
              prefixBytes && equalBytes(prefixBytes, entry.key))
          ) continue;
          const key = unpackKey(entry.key);
          const value = readValue(entry.value, entry.encoding, decodeV8) as T;
          const versionstamp = encodeHex(entry.versionstamp);
          lastYieldedKeyBytes = entry.key;
          outCursor.set(packCursor({ lastYieldedKeyBytes })); // cursor needs to be set before yield
          yield { key, value, versionstamp };
          yielded++;
          // console.log({ yielded, entries, limit });
          if (typeof limit === "number" && yielded >= limit) return;
        }
      }
      if (entries < batchLimit) return;
    }
  }
}

export class WatchCache {
  private readonly decodeV8: DecodeV8;
  private readonly keys: readonly KvKey[];
  private readonly lastValues: ([unknown, string] | undefined)[] = [];

  constructor(decodeV8: DecodeV8, keys: readonly KvKey[]) {
    this.decodeV8 = decodeV8;
    this.keys = keys;
  }

  processOutputKeys(outputKeys: WatchKeyOutput[]): KvEntryMaybe<unknown>[] {
    const { lastValues, decodeV8, keys } = this;
    const initial = lastValues.length === 0;
    outputKeys.forEach((v, i) => {
      const { changed, entryIfChanged } = v;
      if (initial && !changed) {
        throw new Error(
          `watch: Expect all values in first message: ${
            JSON.stringify(outputKeys)
          }`,
        );
      }
      if (!changed) return;
      if (entryIfChanged) {
        const { value: bytes, encoding } = entryIfChanged;
        const value = readValue(bytes, encoding, decodeV8);
        const versionstamp = encodeHex(entryIfChanged.versionstamp);
        lastValues[i] = [value, versionstamp];
      } else {
        lastValues[i] = undefined; // deleted
      }
    });
    return keys.map((key, i) => {
      const lastValue = lastValues[i];
      if (lastValue === undefined) {
        return { key, value: null, versionstamp: null };
      }
      const [value, versionstamp] = lastValue;
      return { key, value, versionstamp };
    });
  }
}

function computeReadRangeForKey(packedKey: Uint8Array): ReadRange {
  return {
    start: packedKey,
    end: new Uint8Array([0xff]),
    limit: 1,
    reverse: false,
  };
}

function computeKvCheckMessage(
  { key, versionstamp }: AtomicCheck,
): KvCheckMessage {
  return {
    key: packKey(key),
    versionstamp: (versionstamp === null || versionstamp === undefined)
      /* in proto3 all fields are optional, but the generated types don't
       * like it, so we have to cast it manually */
      ? undefined as unknown as Uint8Array
      : decodeHex(versionstamp),
  };
}

function computeKvMutationMessage(
  mut: KvMutation,
  encodeV8: EncodeV8,
): KvMutationMessage {
  const { key, type } = mut;
  return {
    key: packKey(key),
    mutationType: type === "delete"
      ? "M_DELETE"
      : type === "max"
      ? "M_MAX"
      : type === "min"
      ? "M_MIN"
      : type == "set"
      ? "M_SET"
      : type === "sum"
      ? "M_SUM"
      : "M_UNSPECIFIED",
    value: mut.type === "delete" ? undefined : packKvValue(mut.value, encodeV8),
    expireAtMs: mut.type === "set" && typeof mut.expireIn === "number"
      ? (Date.now() + mut.expireIn).toString()
      : "0",
  };
}

function computeEnqueueMessage(
  value: unknown,
  encodeV8: EncodeV8,
  { delay = 0, keysIfUndelivered = [] }: {
    delay?: number;
    keysIfUndelivered?: KvKey[];
  } = {},
): EnqueueMessage {
  return {
    backoffSchedule: [100, 200, 400, 800],
    deadlineMs: `${Date.now() + delay}`,
    keysIfUndelivered: keysIfUndelivered.map(packKey),
    payload: encodeV8(value),
  };
}
