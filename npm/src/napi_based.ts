// Copyright 2023 the Deno authors. All rights reserved. MIT license.

import {
  checkOptionalBoolean,
  checkOptionalFunction,
  checkOptionalObject,
  checkOptionalString,
  checkRecord,
  isRecord,
} from "./check.ts";
import {
  decodeAtomicWriteOutput,
  decodeSnapshotReadOutput,
  encodeAtomicWrite,
  encodeSnapshotRead,
} from "./kv_connect_api.ts";
import { packKey } from "./kv_key.ts";
import {
  KvConsistencyLevel,
  KvEntryMaybe,
  KvKey,
  KvService,
} from "./kv_types.ts";
import { DecodeV8, EncodeV8 } from "./kv_util.ts";
import { encodeBinary as encodeWatch } from "./proto/messages/com/deno/kv/datapath/Watch.ts";
import { decodeBinary as decodeWatchOutput } from "./proto/messages/com/deno/kv/datapath/WatchOutput.ts";
import {
  AtomicWrite,
  AtomicWriteOutput,
  SnapshotRead,
  SnapshotReadOutput,
  Watch,
} from "./proto/messages/com/deno/kv/datapath/index.ts";
import { ProtoBasedKv, WatchCache } from "./proto_based.ts";
import { makeUnrawWatchStream } from "./unraw_watch_stream.ts";

export interface NapiBasedServiceOptions {
  /** Enable some console logging */
  readonly debug?: boolean;

  /** Underlying native napi interface */
  readonly napi?: NapiInterface;

  /** Custom serializer to use when serializing v8-encoded KV values.
   *
   * When you are running on Node 18+, pass the 'serialize' function in Node's 'v8' module. */
  readonly encodeV8: EncodeV8;

  /** Custom deserializer to use when deserializing v8-encoded KV values.
   *
   * When you are running on Node 18+, pass the 'deserialize' function in Node's 'v8' module. */
  readonly decodeV8: DecodeV8;

  /** The database will be opened as an in-memory database. */
  readonly inMemory?: boolean;
}

/**
 * Return a KVService that creates KV instances backed by a native Node NAPI interface.
 */
export function makeNapiBasedService(opts: NapiBasedServiceOptions): KvService {
  return {
    openKv: (v) => Promise.resolve(NapiBasedKv.of(v, opts)),
  };
}

export interface NapiInterface {
  open(
    path: string,
    inMemory: boolean | undefined | null,
    debug: boolean,
  ): number;
  close(db: number, debug: boolean): void;
  snapshotRead(
    dbId: number,
    snapshotReadBytes: Uint8Array,
    debug: boolean,
  ): Promise<Uint8Array>;
  atomicWrite(
    dbId: number,
    atomicWriteBytes: Uint8Array,
    debug: boolean,
  ): Promise<Uint8Array>;
  dequeueNextMessage(
    dbId: number,
    debug: boolean,
  ): Promise<{ bytes: Uint8Array; messageId: number } | undefined>;
  finishMessage(
    dbId: number,
    messageId: number,
    success: boolean,
    debug: boolean,
  ): Promise<void>;
  startWatch(
    dbId: number,
    watchBytes: Uint8Array,
    debug: boolean,
  ): Promise<number>;
  dequeueNextWatchMessage(
    dbId: number,
    watchId: number,
    debug: boolean,
  ): Promise<Uint8Array | undefined>;
  endWatch(dbId: number, watchId: number, debug: boolean): void;
}

export const NAPI_FUNCTIONS = [
  "open",
  "close",
  "snapshotRead",
  "atomicWrite",
  "dequeueNextMessage",
  "finishMessage",
  "startWatch",
  "dequeueNextWatchMessage",
  "endWatch",
];

export function isNapiInterface(obj: unknown): obj is NapiInterface {
  return isRecord(obj) &&
    NAPI_FUNCTIONS.every((v) => typeof obj[v] === "function");
}

//

// deno-lint-ignore no-explicit-any
const DEFAULT_NAPI_INTERFACE: any = undefined;

class NapiBasedKv extends ProtoBasedKv {
  private readonly napi: NapiInterface;
  private readonly dbId: number;

  constructor(
    debug: boolean,
    napi: NapiInterface,
    dbId: number,
    decodeV8: DecodeV8,
    encodeV8: EncodeV8,
  ) {
    super(debug, decodeV8, encodeV8);
    this.napi = napi;
    this.dbId = dbId;
  }

  static of(
    url: string | undefined,
    opts: NapiBasedServiceOptions,
  ): NapiBasedKv {
    checkOptionalString("url", url);
    checkRecord("opts", opts);
    checkOptionalBoolean("opts.debug", opts.debug);
    checkOptionalObject("opts.napi", opts.napi);
    checkOptionalFunction("opts.decodeV8", opts.decodeV8);
    checkOptionalFunction("opts.encodeV8", opts.encodeV8);
    checkOptionalBoolean("opts.inMemory", opts.inMemory);

    const {
      debug = false,
      napi = DEFAULT_NAPI_INTERFACE,
      decodeV8,
      encodeV8,
      inMemory,
    } = opts;
    if (typeof url !== "string" || /^https?:\/\//i.test(url)) {
      throw new Error(`Invalid path: ${url}`);
    }
    if (napi === undefined) {
      throw new Error(
        `No default napi interface, provide one via the 'napi' option.`,
      );
    }
    const dbId = napi.open(url, inMemory, debug);
    return new NapiBasedKv(debug, napi, dbId, decodeV8, encodeV8);
  }

  protected async listenQueue_(
    handler: (value: unknown) => void | Promise<void>,
  ): Promise<void> {
    const { napi, dbId, decodeV8, debug } = this;
    while (true) {
      if (debug) console.log(`listenQueue_: before dequeueNextMessage`);
      const result = await napi.dequeueNextMessage(dbId, debug);
      if (result === undefined) return;
      const { bytes, messageId } = result;
      const value = decodeV8(bytes);
      if (debug) console.log(`listenQueue_: after value ${value}`);

      try {
        await Promise.resolve(handler(value));
        await napi.finishMessage(dbId, messageId, true, debug);
      } catch (e) {
        if (debug) console.log(`listenQueue_: handler failed ${e.stack || e}`);
        await napi.finishMessage(dbId, messageId, false, debug);
      }
    }
  }

  protected close_(): void {
    const { napi, dbId, debug } = this;
    napi.close(dbId, debug);
  }

  protected async snapshotRead(
    req: SnapshotRead,
    _consistency?: KvConsistencyLevel,
  ): Promise<SnapshotReadOutput> {
    const { napi, dbId, debug } = this;
    const res = await napi.snapshotRead(dbId, encodeSnapshotRead(req), debug);
    return decodeSnapshotReadOutput(res);
  }

  protected async atomicWrite(req: AtomicWrite): Promise<AtomicWriteOutput> {
    const { napi, dbId, debug } = this;
    const res = await napi.atomicWrite(dbId, encodeAtomicWrite(req), debug);
    return decodeAtomicWriteOutput(res);
  }

  protected watch_(
    keys: readonly KvKey[],
    raw: boolean | undefined,
  ): ReadableStream<KvEntryMaybe<unknown>[]> {
    const { napi, dbId, debug, decodeV8 } = this;
    const { startWatch, dequeueNextWatchMessage, endWatch } = napi;
    if (
      startWatch === undefined || dequeueNextWatchMessage === undefined ||
      endWatch === undefined
    ) {
      throw new Error("watch: not implemented");
    }

    const watch: Watch = {
      keys: keys.map((v) => ({ key: packKey(v) })),
    };

    let watchId: number | undefined;
    let ended = false;
    const endWatchIfNecessary = () => {
      if (watchId !== undefined && !ended) endWatch(dbId, watchId, debug);
      ended = true;
    };
    const cache = new WatchCache(decodeV8, keys);
    const rawStream = new ReadableStream({
      async pull(controller) {
        if (watchId === undefined) {
          watchId = await startWatch(dbId, encodeWatch(watch), debug);
        }
        const watchOutputBytes = await dequeueNextWatchMessage(
          dbId,
          watchId,
          debug,
        );
        if (watchOutputBytes === undefined) {
          endWatchIfNecessary();
          controller.close();
          return;
        }
        const watchOutput = decodeWatchOutput(watchOutputBytes);
        const { status, keys: outputKeys } = watchOutput;
        if (status !== "SR_SUCCESS") {
          throw new Error(`Unexpected status: ${status}`);
        }
        const entries = cache.processOutputKeys(outputKeys);
        controller.enqueue(entries);
      },
      cancel() {
        endWatchIfNecessary();
      },
    });
    return raw
      ? rawStream
      : makeUnrawWatchStream(rawStream, endWatchIfNecessary);
  }
}
