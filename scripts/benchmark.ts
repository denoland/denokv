/// <reference lib="deno.unstable" />

import { parse } from "https://deno.land/std@0.191.0/flags/mod.ts";
import { Semaphore } from "https://deno.land/x/semaphore@v1.1.2/semaphore.ts";

const flags = parse(Deno.args, {
  string: ["path", "read-concurrency", "write-concurrency", "size"],
  default: {},
});

if (!flags["path"]) {
  console.error("Missing --path");
  Deno.exit(1);
}

const readConcurrency = parseInt(flags["read-concurrency"] ?? "");
const writeConcurrency = parseInt(flags["write-concurrency"] ?? "");
const size = parseInt(flags["size"] ?? "");

if (!Number.isSafeInteger(readConcurrency) || readConcurrency <= 0) {
  console.error("Invalid --read-concurrency");
  Deno.exit(1);
}

if (!Number.isSafeInteger(writeConcurrency) || writeConcurrency <= 0) {
  console.error("Invalid --write-concurrency");
  Deno.exit(1);
}

if (!Number.isSafeInteger(size) || size <= 0) {
  console.error("Invalid --size");
  Deno.exit(1);
}

const initConcurrency = 512;

const kv = await Deno.openKv(flags.path);

{
  const startTime = Date.now();
  const initSemaphore = new Semaphore(initConcurrency);
  let count = 0;
  for await (const entry of kv.list({ prefix: [] })) {
    const release = await initSemaphore.acquire();
    kv.delete(entry.key).finally(() => release());
    count++;
  }
  for (let i = 0; i < initConcurrency; i++) {
    await initSemaphore.acquire();
  }

  console.log(`Deleted ${count} entries in ${Date.now() - startTime}ms`);
}

{
  const startTime = Date.now();
  const initSemaphore = new Semaphore(initConcurrency);

  for (let i = 0; i < size; i++) {
    const release = await initSemaphore.acquire();
    kv.set(["accounts", i], 0n).finally(() => release());
  }
  for (let i = 0; i < initConcurrency; i++) {
    await initSemaphore.acquire();
  }

  console.log(`Inserted ${size} entries in ${Date.now() - startTime}ms`);
}

const metrics = { reads: 0, writes: 0, conflicts: 0 };

for (let i = 0; i < writeConcurrency; i++) {
  (async () => {
    while (true) {
      const a = Math.floor(Math.random() * size);
      const b = Math.floor(Math.random() * size);
      if (a === b) continue;

      const amount = BigInt(Math.floor(Math.random() * 1000));

      const [aEntry, bEntry] = await kv.getMany([["accounts", a], [
        "accounts",
        b,
      ]]);
      let [aBal, bBal] = [aEntry, bEntry].map((x) => x.value as bigint);
      aBal -= amount;
      bBal += amount;

      const { ok } = await kv.atomic().check(aEntry, bEntry).set(
        ["accounts", a],
        aBal,
      )
        .set(
          [
            "accounts",
            b,
          ],
          bBal,
        ).commit();
      metrics.writes++;
      if (!ok) metrics.conflicts++;
    }
  })();
}

for (let i = 0; i < readConcurrency; i++) {
  (async () => {
    while (true) {
      const id = Math.floor(Math.random() * size);
      const { value } = await kv.get<bigint>(["accounts", id]);
      if (typeof value !== "bigint") {
        throw new Error("Invalid value");
      }
      metrics.reads++;
    }
  })();
}

let lastMetrics = { ...metrics };

while (true) {
  await new Promise((resolve) => setTimeout(resolve, 1000));
  const [newReads, newWrites, newConflicts] = [
    metrics.reads - lastMetrics.reads,
    metrics.writes - lastMetrics.writes,
    metrics.conflicts - lastMetrics.conflicts,
  ];
  lastMetrics = { ...metrics };

  console.log(
    `NewRds: ${newReads.toString().padStart(8, " ")}/s, NewWrs: ${
      newWrites.toString().padStart(8, " ")
    }/s, NewCfs: ${newConflicts.toString().padStart(8, " ")}/s, AllRds: ${
      metrics.reads.toString().padStart(8, " ")
    }, AllWrs: ${metrics.writes.toString().padStart(8, " ")}, AllCfs: ${
      metrics.conflicts.toString().padStart(8, " ")
    }`,
  );
}
