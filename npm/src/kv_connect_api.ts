import { encodeBinary as encodeWatch } from "./proto/messages/com/deno/kv/datapath/Watch.ts";
import { encodeBinary as encodeAtomicWrite } from "./proto/messages/com/deno/kv/datapath/AtomicWrite.ts";
import { encodeBinary as encodeSnapshotRead } from "./proto/messages/com/deno/kv/datapath/SnapshotRead.ts";
import { decodeBinary as decodeSnapshotReadOutput } from "./proto/messages/com/deno/kv/datapath/SnapshotReadOutput.ts";
import { decodeBinary as decodeAtomicWriteOutput } from "./proto/messages/com/deno/kv/datapath/AtomicWriteOutput.ts";
import {
  AtomicWrite,
  AtomicWriteOutput,
  SnapshotRead,
  SnapshotReadOutput,
  Watch,
} from "./proto/messages/com/deno/kv/datapath/index.ts";
import { isDateTime, isRecord } from "./check.ts";
import { executeWithRetries, RetryableError } from "./sleep.ts";
export {
  decodeAtomicWriteOutput,
  decodeSnapshotReadOutput,
  encodeAtomicWrite,
  encodeSnapshotRead,
};

// VERSION 1
// https://github.com/denoland/deno/tree/092555c611ebab87ad570b4dcb73d54288dccdd9/ext/kv#kv-connect
// https://github.com/denoland/deno/blob/092555c611ebab87ad570b4dcb73d54288dccdd9/cli/schemas/kv-metadata-exchange-response.v1.json
// https://github.com/denoland/deno/blob/092555c611ebab87ad570b4dcb73d54288dccdd9/ext/kv/proto/datapath.proto

// VERSION 2
// https://github.com/denoland/denokv/blob/main/proto/kv-connect.md
// https://github.com/denoland/denokv/blob/main/proto/schema/datapath.proto
// https://github.com/denoland/denokv/blob/main/proto/schema/kv-metadata-exchange-response.v2.json

// VERSION 3
// https://github.com/denoland/denokv/blob/b27fdc1a1a148ab9590c5eff4d2430aa6ee075b4/proto/kv-connect.md
// https://github.com/denoland/denokv/blob/b27fdc1a1a148ab9590c5eff4d2430aa6ee075b4/proto/schema/datapath.proto

export type KvConnectProtocolVersion = 1 | 2 | 3;

export async function fetchDatabaseMetadata(
  url: string,
  accessToken: string,
  fetcher: typeof fetch,
  maxRetries: number,
  supportedVersions: KvConnectProtocolVersion[],
): Promise<{ metadata: DatabaseMetadata; responseUrl: string }> {
  return await executeWithRetries("fetchDatabaseMetadata", async () => {
    const res = await fetcher(url, {
      method: "POST",
      headers: {
        authorization: `Bearer ${accessToken}`,
        "content-type": "application/json",
      },
      body: JSON.stringify({ supportedVersions }),
    });
    if (res.status !== 200) {
      throw new (res.status >= 500 && res.status < 600
        ? RetryableError
        : Error)(
        `Unexpected response status: ${res.status} ${await res.text()}`,
      );
    }
    const contentType = res.headers.get("content-type") ?? undefined;
    if (contentType !== "application/json") {
      throw new Error(
        `Unexpected response content-type: ${contentType} ${await res.text()}`,
      );
    }
    const metadata = await res.json();
    if (!isDatabaseMetadata(metadata)) {
      throw new Error(`Bad DatabaseMetadata: ${JSON.stringify(metadata)}`);
    }
    return { metadata, responseUrl: res.url };
  }, { maxRetries });
}

export async function fetchSnapshotRead(
  url: string,
  accessToken: string,
  databaseId: string,
  req: SnapshotRead,
  fetcher: typeof fetch,
  maxRetries: number,
  version: KvConnectProtocolVersion,
): Promise<SnapshotReadOutput> {
  return decodeSnapshotReadOutput(
    await fetchProtobuf(
      url,
      accessToken,
      databaseId,
      encodeSnapshotRead(req),
      fetcher,
      maxRetries,
      version,
      false,
    ),
  );
}

export async function fetchAtomicWrite(
  url: string,
  accessToken: string,
  databaseId: string,
  write: AtomicWrite,
  fetcher: typeof fetch,
  maxRetries: number,
  version: KvConnectProtocolVersion,
): Promise<AtomicWriteOutput> {
  return decodeAtomicWriteOutput(
    await fetchProtobuf(
      url,
      accessToken,
      databaseId,
      encodeAtomicWrite(write),
      fetcher,
      maxRetries,
      version,
      false,
    ),
  );
}

export async function fetchWatchStream(
  url: string,
  accessToken: string,
  databaseId: string,
  watch: Watch,
  fetcher: typeof fetch,
  maxRetries: number,
  version: KvConnectProtocolVersion,
): Promise<ReadableStream<Uint8Array>> {
  return await fetchProtobuf(
    url,
    accessToken,
    databaseId,
    encodeWatch(watch),
    fetcher,
    maxRetries,
    version,
    true,
  );
}

//

async function fetchProtobuf(
  url: string,
  accessToken: string,
  databaseId: string,
  body: Uint8Array,
  fetcher: typeof fetch,
  maxRetries: number,
  version: KvConnectProtocolVersion,
  stream: false,
): Promise<Uint8Array>;
async function fetchProtobuf(
  url: string,
  accessToken: string,
  databaseId: string,
  body: Uint8Array,
  fetcher: typeof fetch,
  maxRetries: number,
  version: KvConnectProtocolVersion,
  stream: true,
): Promise<ReadableStream<Uint8Array>>;
async function fetchProtobuf(
  url: string,
  accessToken: string,
  databaseId: string,
  body: Uint8Array,
  fetcher: typeof fetch,
  maxRetries: number,
  version: KvConnectProtocolVersion,
  stream: boolean,
): Promise<Uint8Array | ReadableStream<Uint8Array>> {
  const headers = {
    authorization: `Bearer ${accessToken}`,
    ...(version === 1 ? { "x-transaction-domain-id": databaseId } : {
      "x-denokv-version": version.toString(),
      "x-denokv-database-id": databaseId,
    }),
  };
  return await executeWithRetries("fetchProtobuf", async () => {
    const res = await fetcher(url, { method: "POST", body, headers });
    if (res.status !== 200) {
      throw new (res.status >= 500 && res.status < 600
        ? RetryableError
        : Error)(
        `Unexpected response status: ${res.status} ${await res.text()}`,
      );
    }
    const contentType = res.headers.get("content-type") ?? undefined;
    const expectedContentTypes = stream
      ? ["application/octet-stream", "" /* TODO remove once fixed upstream */]
      : [
        "application/x-protobuf",
        "application/protobuf", /* allow nonspec, was returned by denokv release 0.2.0 */
      ];
    if (!expectedContentTypes.includes(contentType ?? "")) {
      throw new Error(
        `Unexpected response content-type: ${contentType} ${await res.text()}`,
      );
    }
    if (stream) {
      if (res.body === null) {
        throw new Error(`No response body for stream request`);
      }
      return res.body;
    } else {
      return new Uint8Array(await res.arrayBuffer());
    }
  }, { maxRetries });
}

function isValidEndpointUrl(url: string): boolean {
  try {
    const { protocol, pathname, search, hash } = new URL(
      url,
      "https://example.com",
    );
    return /^https?:$/.test(protocol) &&
      (pathname === "/" ||
        !pathname.endsWith("/") && search === "" && hash === ""); // must not end in "/" (except no path), no qp/hash implied since the spec simply appends "/action"
  } catch {
    return false;
  }
}

function isEndpointInfo(obj: unknown): obj is EndpointInfo {
  if (!isRecord(obj)) return false;
  const { url, consistency, ...rest } = obj;
  return typeof url === "string" && isValidEndpointUrl(url) &&
    (consistency === "strong" || consistency === "eventual") &&
    Object.keys(rest).length === 0;
}

function isDatabaseMetadata(obj: unknown): obj is DatabaseMetadata {
  if (!isRecord(obj)) return false;
  const { version, databaseId, endpoints, token, expiresAt, ...rest } = obj;
  return (version === 1 || version === 2 || version === 3) &&
    typeof databaseId === "string" &&
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(
      databaseId,
    ) &&
    Array.isArray(endpoints) && endpoints.every(isEndpointInfo) &&
    typeof token === "string" &&
    typeof expiresAt === "string" && isDateTime(expiresAt) &&
    Object.keys(rest).length === 0;
}

//

export interface DatabaseMetadata {
  readonly version: 1 | 2;
  readonly databaseId: string; // uuid
  readonly endpoints: EndpointInfo[];
  readonly token: string;
  readonly expiresAt: string; // 2023-09-17T16:39:10Z
}

export interface EndpointInfo {
  /** A fully qualified URL, or a URL relative to the metadata URL. The path of the URL must not end with a slash.
   * e.g. https://data.example.com/v1, /v1, ./v1 */
  readonly url: string; // https://us-east4.txnproxy.deno-gcp.net

  readonly consistency: "strong" | "eventual";
}
