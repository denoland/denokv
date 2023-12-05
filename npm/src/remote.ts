// Copyright 2023 the Deno authors. All rights reserved. MIT license.

import { ByteReader } from "./bytes.ts";
import {
  check,
  checkOptionalBoolean,
  checkOptionalFunction,
  checkOptionalNumber,
  checkOptionalString,
  checkRecord,
  checkString,
} from "./check.ts";
import {
  DatabaseMetadata,
  fetchAtomicWrite,
  fetchDatabaseMetadata,
  fetchSnapshotRead,
  fetchWatchStream,
  KvConnectProtocolVersion,
} from "./kv_connect_api.ts";
import { packKey } from "./kv_key.ts";
import {
  KvConsistencyLevel,
  KvEntryMaybe,
  KvKey,
  KvService,
} from "./kv_types.ts";
import { DecodeV8, EncodeV8 } from "./kv_util.ts";
import { encodeJson as encodeJsonAtomicWrite } from "./proto/messages/com/deno/kv/datapath/AtomicWrite.ts";
import { encodeJson as encodeJsonSnapshotRead } from "./proto/messages/com/deno/kv/datapath/SnapshotRead.ts";
import { encodeJson as encodeJsonWatch } from "./proto/messages/com/deno/kv/datapath/Watch.ts";
import { decodeBinary as decodeWatchOutput } from "./proto/messages/com/deno/kv/datapath/WatchOutput.ts";
import {
  AtomicWrite,
  AtomicWriteOutput,
  SnapshotRead,
  SnapshotReadOutput,
  Watch,
} from "./proto/messages/com/deno/kv/datapath/index.ts";
import { ProtoBasedKv, WatchCache } from "./proto_based.ts";
import { sleep } from "./sleep.ts";
import { makeUnrawWatchStream } from "./unraw_watch_stream.ts";
import { decodeV8 as _decodeV8, encodeV8 as _encodeV8 } from "./v8.ts";
import { _exponentialBackoffWithJitter } from "https://deno.land/std@0.208.0/async/_util.ts";

type Fetcher = typeof fetch;

export interface RemoteServiceOptions {
  /** Access token used to authenticate to the remote service */
  readonly accessToken: string;

  /** Wrap unsupported V8 payloads to instances of UnknownV8 instead of failing.
   *
   * Only applicable when using the default serializer. */
  readonly wrapUnknownValues?: boolean;

  /** Enable some console logging */
  readonly debug?: boolean;

  /** Custom serializer to use when serializing v8-encoded KV values.
   *
   * When you are running on Node 18+, pass the 'serialize' function in Node's 'v8' module. */
  readonly encodeV8?: EncodeV8;

  /** Custom deserializer to use when deserializing v8-encoded KV values.
   *
   * When you are running on Node 18+, pass the 'deserialize' function in Node's 'v8' module. */
  readonly decodeV8?: DecodeV8;

  /** Custom fetcher to use for the underlying http calls.
   *
   * Defaults to global 'fetch'`
   */
  readonly fetcher?: Fetcher;

  /** Max number of times to attempt to retry certain fetch errors (like 5xx) */
  readonly maxRetries?: number;

  /** Limit to specific KV Connect protocol versions */
  readonly supportedVersions?: KvConnectProtocolVersion[];
}

/**
 * Return a new KvService that can be used to open a remote KV database.
 */
export function makeRemoteService(opts: RemoteServiceOptions): KvService {
  return {
    openKv: async (url) => await RemoteKv.of(url, opts),
  };
}

//

function resolveEndpointUrl(url: string, responseUrl: string): string {
  const u = new URL(url, responseUrl);
  const str = u.toString();
  return u.pathname === "/" ? str.substring(0, str.length - 1) : str;
}

async function fetchNewDatabaseMetadata(
  url: string,
  accessToken: string,
  debug: boolean,
  fetcher: Fetcher,
  maxRetries: number,
  supportedVersions: KvConnectProtocolVersion[],
): Promise<DatabaseMetadata> {
  if (debug) console.log(`fetchNewDatabaseMetadata: Fetching ${url}...`);
  const { metadata, responseUrl } = await fetchDatabaseMetadata(
    url,
    accessToken,
    fetcher,
    maxRetries,
    supportedVersions,
  );
  const { version, endpoints, token } = metadata;
  if (
    version !== 1 && version !== 2 && version !== 3 ||
    !supportedVersions.includes(version)
  ) throw new Error(`Unsupported version: ${version}`);
  if (debug) {
    console.log(`fetchNewDatabaseMetadata: Using protocol version ${version}`);
  }
  if (typeof token !== "string" || token === "") {
    throw new Error(`Unsupported token: ${token}`);
  }
  if (endpoints.length === 0) throw new Error(`No endpoints`);
  const expiresMillis = computeExpiresInMillis(metadata);
  if (debug) {
    console.log(
      `fetchNewDatabaseMetadata: Expires in ${
        Math.round(expiresMillis / 1000 / 60)
      } minutes`,
    ); // expect 60 minutes
  }
  const responseEndpoints = endpoints.map(({ url, consistency }) => ({
    url: resolveEndpointUrl(url, responseUrl),
    consistency,
  })); // metadata url might have been redirected
  if (debug) {
    responseEndpoints.forEach(({ url, consistency }) =>
      console.log(`fetchNewDatabaseMetadata: ${url} (${consistency})`)
    );
  }
  return { ...metadata, endpoints: responseEndpoints };
}

function computeExpiresInMillis({ expiresAt }: DatabaseMetadata): number {
  const expiresTime = new Date(expiresAt).getTime();
  return expiresTime - Date.now();
}

function isValidHttpUrl(url: string): boolean {
  try {
    const { protocol } = new URL(url);
    return protocol === "http:" || protocol === "https:";
  } catch {
    return false;
  }
}

function snapshotReadToString(req: SnapshotRead): string {
  return JSON.stringify(encodeJsonSnapshotRead(req));
}

function atomicWriteToString(req: AtomicWrite): string {
  return JSON.stringify(encodeJsonAtomicWrite(req));
}

function watchToString(req: Watch): string {
  return JSON.stringify(encodeJsonWatch(req));
}

type WatchRecord = { onFinalize: () => void };

//

class RemoteKv extends ProtoBasedKv {
  private readonly url: string;
  private readonly accessToken: string;
  private readonly fetcher: Fetcher;
  private readonly maxRetries: number;
  private readonly supportedVersions: KvConnectProtocolVersion[];
  private readonly watches = new Map<number, WatchRecord>();

  private metadata: DatabaseMetadata;

  private constructor(
    url: string,
    accessToken: string,
    debug: boolean,
    encodeV8: EncodeV8,
    decodeV8: DecodeV8,
    fetcher: Fetcher,
    maxRetries: number,
    supportedVersions: KvConnectProtocolVersion[],
    metadata: DatabaseMetadata,
  ) {
    super(debug, decodeV8, encodeV8);
    this.url = url;
    this.accessToken = accessToken;
    this.fetcher = fetcher;
    this.maxRetries = maxRetries;
    this.supportedVersions = supportedVersions;
    this.metadata = metadata;
  }

  static async of(url: string | undefined, opts: RemoteServiceOptions) {
    checkOptionalString("url", url);
    checkRecord("opts", opts);
    checkString("opts.accessToken", opts.accessToken);
    checkOptionalBoolean("opts.wrapUnknownValues", opts.wrapUnknownValues);
    checkOptionalBoolean("opts.debug", opts.debug);
    checkOptionalFunction("opts.fetcher", opts.fetcher);
    checkOptionalNumber("opts.maxRetries", opts.maxRetries);
    check(
      "opts.supportedVersions",
      opts.supportedVersions,
      opts.supportedVersions === undefined ||
        Array.isArray(opts.supportedVersions) &&
          opts.supportedVersions.every((v) =>
            typeof v === "number" && Number.isSafeInteger(v) && v > 0
          ),
    );
    const {
      accessToken,
      wrapUnknownValues = false,
      debug = false,
      fetcher = fetch,
      maxRetries = 10,
      supportedVersions = [1, 2, 3],
    } = opts;
    if (url === undefined || !isValidHttpUrl(url)) {
      throw new Error(`Bad 'path': must be an http(s) url, found ${url}`);
    }
    const metadata = await fetchNewDatabaseMetadata(
      url,
      accessToken,
      debug,
      fetcher,
      maxRetries,
      supportedVersions,
    );

    const encodeV8: EncodeV8 = opts.encodeV8 ?? _encodeV8;
    const decodeV8: DecodeV8 = opts.decodeV8 ??
      ((v) => _decodeV8(v, { wrapUnknownValues }));

    return new RemoteKv(
      url,
      accessToken,
      debug,
      encodeV8,
      decodeV8,
      fetcher,
      maxRetries,
      supportedVersions,
      metadata,
    );
  }

  protected listenQueue_(
    _handler: (value: unknown) => void | Promise<void>,
  ): Promise<void> {
    throw new Error(`'listenQueue' is not possible over KV Connect`);
  }

  protected watch_(
    keys: readonly KvKey[],
    raw: boolean | undefined,
  ): ReadableStream<KvEntryMaybe<unknown>[]> {
    const { watches, debug } = this;
    const watchId = [...watches.keys()].reduce((a, b) => Math.max(a, b), 0) + 1;
    let readDisabled = false;
    let endOfStreamReached = false;
    let readerCancelled = false;
    let attempt = 1;
    let readStarted = -1;
    let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
    async function* yieldResults(kv: RemoteKv) {
      const { metadata, fetcher, maxRetries, decodeV8 } = kv;
      if (metadata.version < 3) {
        throw new Error(
          `watch: Only supported in version 3 of the protocol or higher`,
        );
      }

      const endpointUrl = await kv.locateEndpointUrl("eventual", readDisabled); // force refetch if retrying after receiving read disabled
      const watchUrl = `${endpointUrl}/watch`;
      const accessToken = metadata.token;
      const req: Watch = {
        keys: keys.map((v) => ({ key: packKey(v) })),
      };
      if (debug) console.log(`watch: ${watchToString(req)}`);
      const stream = await fetchWatchStream(
        watchUrl,
        accessToken,
        metadata.databaseId,
        req,
        fetcher,
        maxRetries,
        metadata.version,
      );
      reader = stream.getReader(); // can't use byob for node compat (fetch() response body streams are ReadableStream { locked: false, state: 'readable', supportsBYOB: false }), see https://github.com/nodejs/undici/issues/1873
      const byteReader = new ByteReader(reader); // use our own buffered reader
      endOfStreamReached = false;
      readerCancelled = false;
      readStarted = Date.now();
      try {
        const cache = new WatchCache(decodeV8, keys);
        while (true) {
          const { done, value } = await byteReader.read(4);
          if (done) {
            if (debug) console.log(`watch: done! returning`);
            endOfStreamReached = true;
            return;
          }
          const n = new DataView(value.buffer).getInt32(0, true);
          if (debug) console.log(`watch: ${n}-byte message`);
          if (n > 0) {
            const { done, value } = await byteReader.read(n);
            if (done) {
              if (debug) console.log(`watch: done before message! returning`);
              endOfStreamReached = true;
              return;
            }
            const output = decodeWatchOutput(value);
            const { status, keys: outputKeys } = output;
            if (status === "SR_READ_DISABLED") {
              if (!readDisabled) {
                readDisabled = true; // retry in the next go-around
                if (debug) {
                  console.log(
                    `watch: received SR_READ_DISABLED, retry after refreshing metadata`,
                  );
                }
                return;
              } else {
                throw new Error(`watch: Read disabled after retry`);
              }
            }
            if (status !== "SR_SUCCESS") {
              throw new Error(`Unexpected status: ${status}`);
            }
            const entries = cache.processOutputKeys(outputKeys);
            yield entries;
          }
        }
      } finally {
        await reader.cancel();
        reader = undefined;
      }
    }

    async function* yieldResultsLoop(kv: RemoteKv) {
      while (true) {
        for await (const entries of yieldResults(kv)) {
          yield entries;
        }
        if (readDisabled) {
          if (debug) {
            console.log(`watch: readDisabled, retry and refresh metadata`);
          }
        } else if (endOfStreamReached && !readerCancelled) {
          const readDuration = readStarted > -1
            ? (Date.now() - readStarted)
            : 0;
          if (readDuration > 60000) attempt = 1; // we read for at least a minute, reset attempt counter to avoid missing updates
          const timeout = Math.round(_exponentialBackoffWithJitter(
            60000, // max timeout
            1000, // min timeout
            attempt,
            2, // multiplier
            1, // full jitter
          ));
          if (debug) {
            console.log(
              `watch: endOfStreamReached, retry after ${timeout}ms, attempt=${attempt}`,
            );
          }
          await sleep(timeout);
          attempt++;
        } else {
          if (debug) console.log(`watch: end of retry loop`);
          return;
        }
      }
    }
    // return ReadableStream.from(yieldResultsLoop(this)); // not supported by dnt/node
    const generator = yieldResultsLoop(this);
    const cancelReaderIfNecessary = async () => {
      readerCancelled = true;
      await reader?.cancel();
      reader = undefined;
    };
    watches.set(watchId, { onFinalize: cancelReaderIfNecessary });
    const rawStream = new ReadableStream({
      async pull(controller) {
        const { done, value } = await generator.next();
        if (done || value === undefined) return;
        controller.enqueue(value);
      },
      async cancel() {
        await cancelReaderIfNecessary();
      },
    });
    return raw
      ? rawStream
      : makeUnrawWatchStream(rawStream, async () =>
        await cancelReaderIfNecessary());
  }

  protected close_(): void {
    [...this.watches.values()].forEach((v) => v.onFinalize());
  }

  protected async snapshotRead(
    req: SnapshotRead,
    consistency: KvConsistencyLevel = "strong",
  ): Promise<SnapshotReadOutput> {
    const {
      url,
      accessToken,
      metadata,
      debug,
      fetcher,
      maxRetries,
      supportedVersions,
    } = this;
    const read = async () => {
      const endpointUrl = await this.locateEndpointUrl(consistency);
      const snapshotReadUrl = `${endpointUrl}/snapshot_read`;
      const accessToken = metadata.token;
      if (debug) console.log(`snapshotRead: ${snapshotReadToString(req)}`);
      return await fetchSnapshotRead(
        snapshotReadUrl,
        accessToken,
        metadata.databaseId,
        req,
        fetcher,
        maxRetries,
        metadata.version,
      );
    };
    const responseCheck = (res: SnapshotReadOutput) =>
      !(this.metadata.version >= 3 && res.status === "SR_READ_DISABLED" ||
        res.readDisabled ||
        consistency === "strong" && !res.readIsStronglyConsistent);
    const res = await read();
    if (!responseCheck(res)) {
      if (debug) {
        if (debug) {
          console.log(
            `snapshotRead: response checks failed, refresh metadata and retry`,
          );
        }
      }
      this.metadata = await fetchNewDatabaseMetadata(
        url,
        accessToken,
        debug,
        fetcher,
        maxRetries,
        supportedVersions,
      );
      const res = await read();
      if (!responseCheck(res)) {
        const { readDisabled, readIsStronglyConsistent, status } = res;
        throw new Error(
          `snapshotRead: response checks failed after retry: ${
            JSON.stringify({ readDisabled, readIsStronglyConsistent, status })
          }`,
        );
      }
      return res;
    } else {
      return res;
    }
  }

  protected async atomicWrite(req: AtomicWrite): Promise<AtomicWriteOutput> {
    const { metadata, debug, fetcher, maxRetries } = this;
    const endpointUrl = await this.locateEndpointUrl("strong");
    const atomicWriteUrl = `${endpointUrl}/atomic_write`;
    const accessToken = metadata.token;
    if (debug) console.log(`fetchAtomicWrite: ${atomicWriteToString(req)}`);
    return await fetchAtomicWrite(
      atomicWriteUrl,
      accessToken,
      metadata.databaseId,
      req,
      fetcher,
      maxRetries,
      metadata.version,
    );
  }

  //

  private async locateEndpointUrl(
    consistency: KvConsistencyLevel,
    forceRefetch = false,
  ): Promise<string> {
    const { url, accessToken, debug, fetcher, maxRetries, supportedVersions } =
      this;
    if (forceRefetch || computeExpiresInMillis(this.metadata) < 1000 * 60 * 5) {
      this.metadata = await fetchNewDatabaseMetadata(
        url,
        accessToken,
        debug,
        fetcher,
        maxRetries,
        supportedVersions,
      );
    }
    const { metadata } = this;
    const firstStrong =
      metadata.endpoints.filter((v) => v.consistency === "strong")[0];
    const firstNonStrong =
      metadata.endpoints.filter((v) => v.consistency !== "strong")[0];
    const endpoint = consistency === "strong"
      ? firstStrong
      : (firstNonStrong ?? firstStrong);
    if (endpoint === undefined) {
      throw new Error(`Unable to find endpoint for: ${consistency}`);
    }
    return endpoint.url; // guaranteed not to end in "/"
  }
}
