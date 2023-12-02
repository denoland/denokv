import {
  check,
  checkOptionalBoolean,
  checkOptionalString,
  checkRecord,
} from "./check.ts";
import { makeInMemoryService } from "./in_memory.ts";
import { Kv } from "./kv_types.ts";
import { DecodeV8, EncodeV8 } from "./kv_util.ts";
import { isNapiInterface, makeNapiBasedService } from "./napi_based.ts";
import { makeNativeService } from "./native.ts";
import { makeRemoteService } from "./remote.ts";

export * from "./napi_based.ts";
export * from "./remote.ts";
export * from "./in_memory.ts";
export * from "./kv_types.ts";
export { UnknownV8 } from "./v8.ts";

export type KvImplementation = "in-memory" | "sqlite" | "remote";

/**
 * Open a new {@linkcode Kv} connection to persist data.
 *
 * When an url is provided, this will connect to a remote [Deno Deploy](https://deno.com/deploy) database
 * or any other endpoint that supports the open [KV Connect](https://github.com/denoland/denokv/blob/main/proto/kv-connect.md) protocol.
 *
 * When a local path is provided, this will use a sqlite database on disk. Read and write access to the file is required.
 *
 * When no path is provided, this will use an ephemeral in-memory implementation.
 */
export async function openKv(
  path?: string,
  opts: Record<string, unknown> & {
    debug?: boolean;
    implementation?: KvImplementation;
  } = {},
): Promise<Kv> {
  checkOptionalString("path", path);
  checkRecord("opts", opts);
  checkOptionalBoolean("opts.debug", opts.debug);
  check(
    "opts.implementation",
    opts.implementation,
    opts.implementation === undefined ||
      ["in-memory", "sqlite", "remote"].includes(opts.implementation),
  );

  const { debug, implementation } = opts;

  // use built-in native implementation if available when running on Deno
  if ("Deno" in globalThis && !implementation) {
    // deno-lint-ignore no-explicit-any
    const { openKv } = (globalThis as any).Deno;
    if (typeof openKv === "function") return makeNativeService().openKv(path);
  }

  // use in-memory implementation if no path provided
  if (path === undefined || path === "" || implementation === "in-memory") {
    const maxQueueAttempts = typeof opts.maxQueueAttempts === "number"
      ? opts.maxQueueAttempts
      : undefined;
    return await makeInMemoryService({ debug, maxQueueAttempts }).openKv(path);
  }

  const { encodeV8, decodeV8 } = await (async () => {
    const { encodeV8, decodeV8 } = opts;
    const defined = [encodeV8, decodeV8].filter((v) => v !== undefined).length;
    if (defined === 1) {
      throw new Error(`Provide both 'encodeV8' or 'decodeV8', or neither`);
    }
    if (defined > 0) {
      if (typeof encodeV8 !== "function") {
        throw new Error(`Unexpected 'encodeV8': ${encodeV8}`);
      }
      if (typeof decodeV8 !== "function") {
        throw new Error(`Unexpected 'decodeV8': ${decodeV8}`);
      }
      return { encodeV8: encodeV8 as EncodeV8, decodeV8: decodeV8 as DecodeV8 };
    }
    if ("Bun" in globalThis) {
      throw new Error(
        `Bun provides v8.serialize/deserialize, but it uses an incompatible format (JavaScriptCore). Provide explicit 'encodeV8' and 'decodeV8' functions via options.`,
      ); // https://discord.com/channels/876711213126520882/888937948345684008/1150135641137487892
    }
    const v8 = await import(`${"v8"}`);
    if (!v8) throw new Error(`Unable to import the v8 module`);
    const { serialize, deserialize } = v8;
    if (typeof serialize !== "function") {
      throw new Error(`Unexpected 'serialize': ${serialize}`);
    }
    if (typeof deserialize !== "function") {
      throw new Error(`Unexpected 'deserialize': ${deserialize}`);
    }
    return {
      encodeV8: serialize as EncodeV8,
      decodeV8: deserialize as DecodeV8,
    };
  })();

  // use remote implementation if path looks like a url
  if (/^https?:\/\//i.test(path) || implementation === "remote") {
    let accessToken =
      typeof opts.accessToken === "string" && opts.accessToken !== ""
        ? opts.accessToken
        : undefined;
    if (accessToken === undefined) {
      // deno-lint-ignore no-explicit-any
      accessToken = (globalThis as any)?.process?.env?.DENO_KV_ACCESS_TOKEN;
      if (typeof accessToken !== "string") {
        throw new Error(`Set the DENO_KV_ACCESS_TOKEN to your access token`);
      }
    }
    const fetcher = typeof opts.fetcher === "function"
      ? opts.fetcher as typeof fetch
      : undefined;
    const maxRetries = typeof opts.maxRetries === "number"
      ? opts.maxRetries
      : undefined;
    const wrapUnknownValues = typeof opts.wrapUnknownValues === "boolean"
      ? opts.wrapUnknownValues
      : undefined;
    const supportedVersions = Array.isArray(opts.supportedVersions) &&
        opts.supportedVersions.every((v) => typeof v === "number")
      ? opts.supportedVersions
      : undefined;
    return await makeRemoteService({
      debug,
      accessToken,
      encodeV8,
      decodeV8,
      fetcher,
      maxRetries,
      supportedVersions,
      wrapUnknownValues,
    }).openKv(path);
  }

  // else use the sqlite napi implementation
  const { napi } = opts;
  if (napi !== undefined && !isNapiInterface(napi)) {
    throw new Error(`Unexpected napi interface for sqlite`);
  }
  const inMemory = typeof opts.inMemory === "boolean"
    ? opts.inMemory
    : undefined;
  return await makeNapiBasedService({
    debug,
    encodeV8,
    decodeV8,
    napi,
    inMemory,
  }).openKv(path);
}
