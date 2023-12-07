# `@deno/kv`

![https://github.com/denoland/denokv/actions](https://github.com/denoland/denokv/workflows/npm/badge.svg)

A [Deno KV](https://deno.com/kv) client library optimized for Node.js.

- Access [Deno Deploy](https://deno.com/deploy) remote databases (or any
  endpoint implementing the open
  [KV Connect](https://github.com/denoland/denokv/blob/main/proto/kv-connect.md)
  protocol) on Node 18+.
- Create local KV databases backed by
  [SQLite](https://www.sqlite.org/index.html), using optimized native
  [NAPI](https://nodejs.org/docs/latest-v18.x/api/n-api.html) packages for
  Node - compatible with databases created by Deno itself.
- Create ephemeral in-memory KV instances backed by SQLite memory files or by a
  lightweight JS-only implementation for testing.
- Zero JS dependencies, architecture-specific native code for SQLite backend
  (see below).
- Simply call the exported `openKv` function (equiv to
  [`Deno.openKv`](https://deno.land/api?s=Deno.openKv&unstable)) with a url or
  local path to get started!

### Quick start - Node 18+

_Install the [npm package](https://www.npmjs.com/package/@deno/kv)_ 👉
`npm install @deno/kv`

_Remote KV Database_

```js
import { openKv } from "@deno/kv";

// to open a database connection to an existing Deno Deploy KV database,
// first obtain the database ID from your project dashboard:
// https://dash.deno.com/projects/YOUR_PROJECT/kv

// connect to the remote database
const kv = await openKv(
  "https://api.deno.com/databases/YOUR_DATABASE_ID/connect",
);
// access token for auth is the value of environment variable DENO_KV_ACCESS_TOKEN by default

// do anything using the KV api: https://deno.land/api?s=Deno.Kv&unstable
const result = await kv.set(["from-client"], "hello!");
console.log(result);

// close the database connection
kv.close();

// to provide an explicit access token, pass it as an additional option
const kv2 = await openKv(
  "https://api.deno.com/databases/YOUR_DATABASE_ID/connect",
  { accessToken: mySecretAccessToken },
);
```

_Local KV Database_

```js
import { openKv } from "@deno/kv";

// create a local KV database instance backed by SQLite
const kv = await openKv("kv.db");

// do anything using the KV api: https://deno.land/api?s=Deno.Kv&unstable
const result = await kv.set(["from-client"], "hello!");
console.log(result);

// close the database connection
kv.close();
```

_In-Memory KV Database (no native code)_

```js
import { openKv } from "@deno/kv";

// create an ephemeral KV instance for testing
const kv = await openKv("");

// do anything using the KV api: https://deno.land/api?s=Deno.Kv&unstable
const result = await kv.set(["from-client"], "hello!");
console.log(result);

// close the database connection
kv.close();
```

> Examples use ESM syntax, but this package supports CJS `require`-based usage
> as well

### Local KV Databases

Local disk-based databases are backed by
[SQLite](https://www.sqlite.org/index.html), and are compatible with databases created
with Deno itself:

- leverages shared Rust code from
  [denoland/denokv](https://github.com/denoland/denokv) with a small shim
  compiled for Node's native interface via the [NAPI-RS](https://napi.rs/)
  framework
- an architecture-specific native package dependency is selected and installed
  at `npm install` time via the standard npm
  [peer dependency mechanism](https://docs.npmjs.com/cli/v10/configuring-npm/package-json#peerdependencies)

The following native architectures are supported:

|               | node18 | node20 |
| ------------- | ------ | ------ |
| Windows x64   | ✓      | ✓      |
| macOS x64     | ✓      | ✓      |
| macOS arm64   | ✓      | ✓      |
| Linux x64 gnu | ✓      | ✓      |

### Credits

- Protobuf code generated with [pb](https://deno.land/x/pbkit/cli/pb/README.md)
- npm package generated with [dnt](https://github.com/denoland/dnt)
- Initial code contributed by
  [skymethod/kv-connect-kit](https://github.com/skymethod/kv-connect-kit)

---

### API

This package exports a single convenience function `openKv`, taking an optional
string `path`, with optional `opts`. Depending on the `path` provided, one of
three different implementations are used:

- if `path` is omitted or blank, an ephemeral `in-memory` db implementation is
  used, useful for testing
- if `path` is an http or https url, the `remote` client implementation is used
  to connect to [Deno Deploy](https://deno.com/deploy) or self-hosted
  [denokv](https://github.com/denoland/denokv) instances
- otherwise the `path` is passed to the native `sqlite` implementation - can
  specify local paths or `:memory:` for SQLite's
  [in-memory mode](https://www.sqlite.org/inmemorydb.html)

You can override the implementation used via the `implementation` option:

```js
const kv = await openKv("https://example.com/not-really-remote", {
  implementation: "in-memory",
});
```

Pass the `debug` option to `console.log` additional debugging info:

```js
const kv = await openKv("http://localhost:4512/", { 
  debug: true
});
```

### Backend-specific options

Each implementation supports different additional options,
via the second parameter to `openKv`:

**Remote backend**

```ts
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
```

**Native SQLite backend**

```ts
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
```

**Lightweight In-Memory backend**

```ts
export interface InMemoryServiceOptions {
  /** Enable some console logging */
  readonly debug?: boolean;

  /** Maximum number of attempts to deliver a failing queue message before giving up. Defaults to 10. */
  readonly maxQueueAttempts?: number;
}
```

### Other runtimes

This package is targeted for Node.js, but may work on other runtimes with the following caveats:

**V8 Serialization**

Deno KV uses [V8](https://v8.dev/docs)'s serialization format to serialize values, provided natively by Deno and when using this
package on Node automatically via the [built-in `v8` module](https://nodejs.org/api/v8.html#serialization-api).

[Bun](https://bun.sh/) also provides a `v8` module, but uses a different serialization format under the hood (JavaScriptCore).

This can present data corruption problems if you want to use databases on Deno Deploy or other databases shared
with Node/Deno that use actual V8 serialization: you might create data you cannot read, or vice versa.

For this reason, `openKV` on Bun will throw by default to avoid unexpected data corruption.

If you are only going to read and write to your local databases in Bun, you can force the use of Bun's serializers by providing
a custom `encodeV8`, `decodeV8` explicitly as the second parameter to `openKv`. Note any data will be unreadable by
Node (using @deno/kv), or Deno - since they use the actual V8 format.

```js
import { openKv } from "@deno/kv";
import { serialize as encodeV8, deserialize as decodeV8 } from "v8"; // actually JavaScriptCore format on Bun!

const kv = await openKv("kv.db", { encodeV8, decodeV8 });
```

If a native `v8` module is not available on your runtime, you can use a limited JS-based V8 serializer provided by this package.
It only supports a limited number of value types (`string`, `boolean`, `null`, `undefined`), so consider using `JSON.parse`/`JSON.stringify`
to marshall values to and from strings for storage.

```js
import { openKv, makeLimitedV8Serializer } from "@deno/kv";

const { encodeV8, decodeV8 } = makeLimitedV8Serializer();

const kv = await openKv("kv.db", { encodeV8, decodeV8 });
```

V8 serialization is only necessary for SQLite and remote databases, the in-memory implementation
used when calling `openKv()` or `openKv('')` uses in-process values and [structuredClone](https://developer.mozilla.org/en-US/docs/Web/API/structuredClone)
instead.
