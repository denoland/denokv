// Copyright 2023 the Deno authors. All rights reserved. MIT license.

import { chunk } from "https://deno.land/std@0.208.0/collections/chunk.ts";
import { parseArgs as parseFlags } from "https://deno.land/std@0.208.0/cli/parse_args.ts";
import {
  compare as compareVersion,
  parse as parseVersion,
} from "https://deno.land/std@0.208.0/semver/mod.ts";
import { makeRemoteService } from "./remote.ts";
import { makeNativeService } from "./native.ts";
import { endToEnd } from "./e2e.ts";
import { makeInMemoryService } from "./in_memory.ts";
import { KvKey, KvService } from "./kv_types.ts";

const flags = parseFlags(Deno.args);
const debug = !!flags.debug;

Deno.test({
  name: "e2e-userland-in-memory",
  only: false,
  fn: async () => {
    await endToEnd(makeInMemoryService({ debug, maxQueueAttempts: 1 }), {
      type: "userland",
      subtype: "in-memory",
      path: ":memory:",
    });
  },
});

Deno.test({
  name: "e2e-deno-memory",
  only: false,
  fn: async () => {
    await endToEnd(makeNativeService(), { type: "deno", path: ":memory:" });
  },
});

Deno.test({
  name: "e2e-deno-disk",
  only: false,
  fn: async () => {
    const path = await Deno.makeTempFile({
      prefix: "userland-e2e-tests-",
      suffix: ".db",
    });
    try {
      await endToEnd(makeNativeService(), { type: "deno", path });
    } finally {
      await Deno.remove(path);
    }
  },
});

//

async function clear(service: KvService, path: string) {
  const kv = await service.openKv(path);
  const keys: KvKey[] = [];
  for await (const { key } of kv.list({ prefix: [] })) {
    keys.push(key);
  }
  for (const batch of chunk(keys, 1000)) {
    let tx = kv.atomic();
    for (const key of batch) {
      tx = tx.delete(key);
    }
    await tx.commit();
  }
  kv.close();
}

const denoKvAccessToken = (await Deno.permissions.query({
      name: "env",
      variable: "DENO_KV_ACCESS_TOKEN",
    })).state === "granted" && Deno.env.get("DENO_KV_ACCESS_TOKEN");
const denoKvDatabaseId = (await Deno.permissions.query({
      name: "env",
      variable: "DENO_KV_DATABASE_ID",
    })).state === "granted" && Deno.env.get("DENO_KV_DATABASE_ID");

Deno.test({
  name: "e2e-deno-remote",
  only: false,
  ignore: !(typeof denoKvAccessToken === "string" && denoKvDatabaseId),
  fn: async () => {
    const path = `https://api.deno.com/databases/${denoKvDatabaseId}/connect`;
    const service = makeNativeService();
    await clear(service, path);
    try {
      await endToEnd(service, { type: "deno", path });
    } finally {
      await clear(service, path);
    }
  },
});

Deno.test({
  name: "e2e-userland-remote",
  only: false,
  ignore: !(typeof denoKvAccessToken === "string" && denoKvDatabaseId),
  fn: async () => {
    const path = `https://api.deno.com/databases/${denoKvDatabaseId}/connect`;
    const service = makeRemoteService({
      accessToken: denoKvAccessToken as string,
      debug,
    });
    await clear(service, path);
    try {
      await endToEnd(service, { type: "userland", subtype: "remote", path });
    } finally {
      await clear(service, path);
    }
  },
});

const localKvUrl =
  (await Deno.permissions.query({ name: "env", variable: "LOCAL_KV_URL" }))
      .state === "granted" && Deno.env.get("LOCAL_KV_URL");

Deno.test({
  only: false,
  ignore:
    compareVersion(parseVersion(Deno.version.deno), parseVersion("1.38.0")) <
      0 || !(typeof denoKvAccessToken === "string" && localKvUrl),
  name: "e2e-deno-localkv",
  fn: async () => {
    const path = localKvUrl as string;
    const service = makeNativeService();
    await clear(service, path);
    try {
      await endToEnd(service, { type: "deno", path });
    } finally {
      await clear(service, path);
    }
  },
});

Deno.test({
  only: false,
  ignore: !(typeof denoKvAccessToken === "string" && localKvUrl),
  name: "e2e-userland-localkv",
  fn: async () => {
    const path = localKvUrl as string;
    const service = makeRemoteService({
      accessToken: denoKvAccessToken as string,
      debug,
      maxRetries: 0,
    });
    await clear(service, path);
    try {
      await endToEnd(service, { type: "userland", subtype: "remote", path });
    } finally {
      await clear(service, path);
    }
  },
});
