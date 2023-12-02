import { decodeHex, encodeHex } from "./bytes.ts";
import {
  checkExpireIn,
  checkKeyNotEmpty,
  checkMatches,
  isRecord,
} from "./check.ts";
import {
  AtomicCheck,
  AtomicOperation,
  Kv,
  KvCommitError,
  KvCommitResult,
  KvConsistencyLevel,
  KvEntry,
  KvEntryMaybe,
  KvKey,
  KvListIterator,
  KvListOptions,
  KvListSelector,
} from "./kv_types.ts";
import { _KvU64 } from "./kv_u64.ts";
import { defer, Deferred } from "./proto/runtime/async/observer.ts";
import {
  decode as decodeBase64,
  encode as encodeBase64,
} from "./proto/runtime/base64.ts";

export type EncodeV8 = (value: unknown) => Uint8Array;
export type DecodeV8 = (bytes: Uint8Array) => unknown;

export type KvValueEncoding =
  | "VE_UNSPECIFIED"
  | "VE_V8"
  | "VE_LE64"
  | "VE_BYTES";

export type KvValue = {
  data: Uint8Array;
  encoding: KvValueEncoding;
};

export type KvMutation =
  & { key: KvKey }
  & (
    | { type: "set"; value: unknown; expireIn?: number }
    | { type: "delete" }
    | { type: "sum"; value: { readonly value: bigint } }
    | { type: "max"; value: { readonly value: bigint } }
    | { type: "min"; value: { readonly value: bigint } }
  );

export function unpackKvu(bytes: Uint8Array): _KvU64 {
  if (bytes.length !== 8) throw new Error();
  if (bytes.buffer.byteLength !== 8) bytes = new Uint8Array(bytes);
  const rt = new DataView(bytes.buffer).getBigUint64(0, true);
  return new _KvU64(rt);
}

export function packKvu(value: _KvU64): Uint8Array {
  const rt = new Uint8Array(8);
  new DataView(rt.buffer).setBigUint64(0, value.value, true);
  return rt;
}

export function readValue(
  bytes: Uint8Array,
  encoding: KvValueEncoding,
  decodeV8: DecodeV8,
) {
  if (encoding === "VE_V8") return decodeV8(bytes);
  if (encoding === "VE_LE64") return unpackKvu(bytes);
  if (encoding === "VE_BYTES") return bytes;
  throw new Error(
    `Unsupported encoding: ${encoding} [${[...bytes].join(", ")}]`,
  );
}

export function packKvValue(value: unknown, encodeV8: EncodeV8): KvValue {
  if (value instanceof _KvU64) {
    return { encoding: "VE_LE64", data: packKvu(value) };
  }
  if (value instanceof Uint8Array) return { encoding: "VE_BYTES", data: value };
  return { encoding: "VE_V8", data: encodeV8(value) };
}

export type Cursor = { lastYieldedKeyBytes: Uint8Array };

export function packCursor({ lastYieldedKeyBytes }: Cursor): string {
  return encodeBase64(
    JSON.stringify({ lastYieldedKeyBytes: encodeHex(lastYieldedKeyBytes) }),
  );
}

export function unpackCursor(str: string): Cursor {
  try {
    const { lastYieldedKeyBytes } = JSON.parse(
      new TextDecoder().decode(decodeBase64(str)),
    );
    if (typeof lastYieldedKeyBytes === "string") {
      return { lastYieldedKeyBytes: decodeHex(lastYieldedKeyBytes) };
    }
  } catch {
    // noop
  }
  throw new Error(`Invalid cursor`);
}

export function checkListSelector(selector: KvListSelector) {
  if (!isRecord(selector)) {
    throw new TypeError(`Bad selector: ${JSON.stringify(selector)}`);
  }
  if ("prefix" in selector && "start" in selector && "end" in selector) {
    throw new TypeError(
      `Selector can not specify both 'start' and 'end' key when specifying 'prefix'`,
    );
  }
}

export function checkListOptions(options: KvListOptions): KvListOptions {
  if (!isRecord(options)) {
    throw new TypeError(`Bad options: ${JSON.stringify(options)}`);
  }
  const { limit, cursor, consistency, batchSize } = options;
  if (
    !(limit === undefined ||
      typeof limit === "number" && limit > 0 && Number.isSafeInteger(limit))
  ) throw new TypeError(`Bad 'limit': ${limit}`);
  if (!(cursor === undefined || typeof cursor === "string")) {
    throw new TypeError(`Bad 'cursor': ${limit}`);
  }
  const reverse = options.reverse === true; // follow native logic
  if (
    !(consistency === undefined || consistency === "strong" ||
      consistency === "eventual")
  ) throw new TypeError(`Bad 'consistency': ${consistency}`);
  if (
    !(batchSize === undefined ||
      typeof batchSize === "number" && batchSize > 0 &&
        Number.isSafeInteger(batchSize) && batchSize <= 1000)
  ) throw new TypeError(`Bad 'batchSize': ${batchSize}`);
  return { limit, cursor, reverse, consistency, batchSize };
}

export const packVersionstamp = (version: number) =>
  `${version.toString().padStart(16, "0")}0000`;

export const unpackVersionstamp = (versionstamp: string) =>
  parseInt(checkMatches("versionstamp", versionstamp, /^(\d{16})0000$/)[1]);

export const isValidVersionstamp = (versionstamp: string) =>
  /^(\d{16})0000$/.test(versionstamp);

export const replacer = (_this: unknown, v: unknown) =>
  typeof v === "bigint" ? v.toString() : v;

export class CursorHolder {
  private cursor: string | undefined;

  get(): string {
    const { cursor } = this;
    if (cursor === undefined) {
      throw new Error(`Cannot get cursor before first iteration`);
    }
    return cursor;
  }

  set(cursor: string) {
    this.cursor = cursor;
  }
}

export class GenericKvListIterator<T> implements KvListIterator<T> {
  private readonly generator: AsyncGenerator<KvEntry<T>>;
  private readonly _cursor: () => string;

  constructor(generator: AsyncGenerator<KvEntry<T>>, cursor: () => string) {
    this.generator = generator;
    this._cursor = cursor;
  }

  get cursor(): string {
    return this._cursor();
  }

  next(): Promise<IteratorResult<KvEntry<T>, undefined>> {
    return this.generator.next();
  }

  [Symbol.asyncIterator](): AsyncIterableIterator<KvEntry<T>> {
    return this.generator[Symbol.asyncIterator]();
  }

  // deno-lint-ignore no-explicit-any
  return?(value?: any): Promise<IteratorResult<KvEntry<T>, any>> {
    return this.generator.return(value);
  }

  // deno-lint-ignore no-explicit-any
  throw?(e?: any): Promise<IteratorResult<KvEntry<T>, any>> {
    return this.generator.throw(e);
  }
}

export type Enqueue = {
  value: unknown;
  opts?: { delay?: number; keysIfUndelivered?: KvKey[] };
};
type CommitFn = (
  checks: AtomicCheck[],
  mutations: KvMutation[],
  enqueues: Enqueue[],
) => Promise<KvCommitResult | KvCommitError>;

export class GenericAtomicOperation implements AtomicOperation {
  private readonly commitFn: CommitFn;

  private readonly checks: AtomicCheck[] = [];
  private readonly mutations: KvMutation[] = [];
  private readonly enqueues: Enqueue[] = [];

  constructor(commit: CommitFn) {
    this.commitFn = commit;
  }

  check(...checks: AtomicCheck[]): this {
    this.checks.push(...checks);
    return this;
  }

  mutate(...mutations: KvMutation[]): this {
    mutations.map((v) => v.key).forEach(checkKeyNotEmpty);
    mutations.forEach((v) => v.type === "set" && checkExpireIn(v.expireIn));
    this.mutations.push(...mutations);
    return this;
  }

  sum(key: KvKey, n: bigint): this {
    checkKeyNotEmpty(key);
    return this.mutate({ type: "sum", key, value: new _KvU64(n) });
  }

  min(key: KvKey, n: bigint): this {
    checkKeyNotEmpty(key);
    return this.mutate({ type: "min", key, value: new _KvU64(n) });
  }

  max(key: KvKey, n: bigint): this {
    checkKeyNotEmpty(key);
    return this.mutate({ type: "max", key, value: new _KvU64(n) });
  }

  set(
    key: KvKey,
    value: unknown,
    { expireIn }: { expireIn?: number } = {},
  ): this {
    checkExpireIn(expireIn);
    checkKeyNotEmpty(key);
    return this.mutate({ type: "set", key, value, expireIn });
  }

  delete(key: KvKey): this {
    checkKeyNotEmpty(key);
    return this.mutate({ type: "delete", key });
  }

  enqueue(
    value: unknown,
    opts?: { delay?: number; keysIfUndelivered?: KvKey[] },
  ): this {
    this.enqueues.push({ value, opts });
    return this;
  }

  async commit(): Promise<KvCommitResult | KvCommitError> {
    return await this.commitFn(this.checks, this.mutations, this.enqueues);
  }
}

export abstract class BaseKv implements Kv {
  protected readonly debug: boolean;
  private closed = false;

  protected constructor({ debug }: { debug: boolean }) {
    this.debug = debug;
  }

  async get<T = unknown>(
    key: KvKey,
    { consistency }: { consistency?: KvConsistencyLevel } = {},
  ): Promise<KvEntryMaybe<T>> {
    this.checkOpen("get");
    checkKeyNotEmpty(key);
    return await this.get_(key, consistency);
  }

  // deno-lint-ignore no-explicit-any
  async getMany<T>(
    keys: readonly KvKey[],
    { consistency }: { consistency?: KvConsistencyLevel } = {},
  ): Promise<any> {
    this.checkOpen("getMany");
    keys.forEach(checkKeyNotEmpty);
    if (keys.length === 0) return [];
    return await this.getMany_(keys, consistency);
  }

  async set(
    key: KvKey,
    value: unknown,
    { expireIn }: { expireIn?: number } = {},
  ): Promise<KvCommitResult> {
    this.checkOpen("set");
    const result = await this.atomic().set(key, value, { expireIn }).commit();
    if (!result.ok) throw new Error(`set failed`); // should never happen, there are no checks
    return result;
  }

  async delete(key: KvKey): Promise<void> {
    this.checkOpen("delete");
    const result = await this.atomic().delete(key).commit();
    if (!result.ok) throw new Error(`delete failed`); // should never happen, there are no checks
  }

  async enqueue(
    value: unknown,
    opts?: { delay?: number; keysIfUndelivered?: KvKey[] },
  ): Promise<KvCommitResult> {
    this.checkOpen("enqueue");
    const result = await this.atomic().enqueue(value, opts).commit();
    if (!result.ok) throw new Error(`enqueue failed`); // should never happen, there are no checks
    return result;
  }

  list<T = unknown>(
    selector: KvListSelector,
    options: KvListOptions = {},
  ): KvListIterator<T> {
    this.checkOpen("list");
    checkListSelector(selector);
    options = checkListOptions(options);
    const outCursor = new CursorHolder();
    const generator: AsyncGenerator<KvEntry<T>> = this.listStream(
      outCursor,
      selector,
      options,
    );
    return new GenericKvListIterator<T>(generator, () => outCursor.get());
  }

  async listenQueue(
    handler: (value: unknown) => void | Promise<void>,
  ): Promise<void> {
    this.checkOpen("listenQueue");
    return await this.listenQueue_(handler);
  }

  atomic(additionalWork?: () => void): AtomicOperation {
    return new GenericAtomicOperation(async (checks, mutations, enqueues) => {
      this.checkOpen("commit");
      return await this.commit(checks, mutations, enqueues, additionalWork);
    });
  }

  // deno-lint-ignore no-explicit-any
  watch<T>(
    keys: readonly KvKey[],
    options?: { raw?: boolean },
  ): ReadableStream<any> {
    this.checkOpen("watch");
    if (keys.length === 0) throw new Error("Provide at least one key to watch");
    keys.forEach(checkKeyNotEmpty);
    if (
      !(options === undefined ||
        isRecord(options) &&
          (options.raw === undefined || typeof options.raw === "boolean"))
    ) throw new Error(`Unexpected options: ${JSON.stringify(options)}`);
    const { raw } = options ?? {};
    return this.watch_(keys, raw);
  }

  close(): void {
    this.checkOpen("close");
    this.closed = true;
    this.close_();
  }

  [Symbol.dispose](): void {
    this.close();
  }

  protected abstract get_<T = unknown>(
    key: KvKey,
    consistency: KvConsistencyLevel | undefined,
  ): Promise<KvEntryMaybe<T>>;
  protected abstract getMany_(
    keys: readonly KvKey[],
    consistency: KvConsistencyLevel | undefined,
  ): Promise<KvEntryMaybe<unknown>[]>;
  protected abstract listStream<T>(
    outCursor: CursorHolder,
    selector: KvListSelector,
    opts: KvListOptions,
  ): AsyncGenerator<KvEntry<T>>;
  protected abstract listenQueue_(
    handler: (value: unknown) => void | Promise<void>,
  ): Promise<void>;
  protected abstract commit(
    checks: AtomicCheck[],
    mutations: KvMutation[],
    enqueues: Enqueue[],
    additionalWork?: () => void,
  ): Promise<KvCommitResult | KvCommitError>;
  protected abstract watch_(
    keys: readonly KvKey[],
    raw: boolean | undefined,
  ): ReadableStream<KvEntryMaybe<unknown>[]>;
  protected abstract close_(): void;

  //

  private checkOpen(method: string) {
    if (this.closed) {
      throw new Error(`Cannot call '.${method}' after '.close' is called`);
    }
  }
}

export class Expirer {
  private readonly debug: boolean;
  private readonly expireFn: () => number | undefined;

  private minExpires: number | undefined;
  private expirerTimeout = 0;

  constructor(debug: boolean, expireFn: () => number | undefined) {
    this.debug = debug;
    this.expireFn = expireFn;
  }

  init(minExpires: number | undefined) {
    this.minExpires = minExpires;
    if (this.minExpires !== undefined) this.rescheduleExpirer(this.minExpires);
  }

  rescheduleExpirer(expires: number) {
    const { minExpires, debug, expirerTimeout } = this;
    if (minExpires !== undefined && minExpires < expires) return;
    this.minExpires = expires;
    clearTimeout(expirerTimeout);
    const delay = expires - Date.now();
    if (debug) console.log(`rescheduleExpirer: run in ${delay}ms`);
    this.expirerTimeout = setTimeout(() => this.runExpirer(), delay);
  }

  finalize() {
    clearTimeout(this.expirerTimeout);
  }

  //

  private runExpirer() {
    const { expireFn } = this;
    const newMinExpires = expireFn();
    this.minExpires = newMinExpires;
    if (newMinExpires !== undefined) {
      this.rescheduleExpirer(newMinExpires);
    } else {
      clearTimeout(this.expirerTimeout);
    }
  }
}

export type QueueHandler = (value: unknown) => void | Promise<void>;

export class QueueWorker {
  private readonly workerFn: (queueHandler?: QueueHandler) => void;

  private workerTimeout = 0;
  private queueHandler?: QueueHandler;
  private queueHandlerPromise?: Deferred<void>;

  constructor(workerFn: (queueHandler?: QueueHandler) => void) {
    this.workerFn = workerFn;
  }

  listen(handler: QueueHandler): Promise<void> {
    if (this.queueHandler) throw new Error(`Already called 'listenQueue'`); // for now
    this.queueHandler = handler;
    const rt = defer<void>();
    this.queueHandlerPromise = rt;
    this.rescheduleWorker();
    return rt;
  }

  rescheduleWorker(delay = 0) {
    clearTimeout(this.workerTimeout);
    if (this.queueHandler) {
      this.workerTimeout = setTimeout(
        () => this.workerFn(this.queueHandler),
        delay,
      );
    }
  }

  finalize() {
    clearTimeout(this.workerTimeout);
    this.queueHandlerPromise?.resolve();
  }
}
