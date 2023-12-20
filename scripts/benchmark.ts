/// <reference lib="deno.unstable" />

// Run with: deno run -A --unstable ./scripts/benchmark.ts --path http://127.0.0.1:4512 --read-concurrency 128 --write-concurrency 32 --size 10000 --consistency-check-interval 10

import { parse } from "https://deno.land/std@0.191.0/flags/mod.ts";
import { Semaphore } from "https://deno.land/x/semaphore@v1.1.2/semaphore.ts";

const flags = parse(Deno.args, {
  string: [
    "path",
    "read-concurrency",
    "write-concurrency",
    "consistency-check-interval",
    "size",
  ],
  default: {},
});

if (!flags["path"]) {
  console.error("Missing --path");
  Deno.exit(1);
}

const readConcurrency = parseInt(flags["read-concurrency"] ?? "");
const writeConcurrency = parseInt(flags["write-concurrency"] ?? "");
const consistencyCheckInterval = parseInt(
  flags["consistency-check-interval"] ?? "0",
);
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

if (
  !Number.isSafeInteger(consistencyCheckInterval) ||
  consistencyCheckInterval < 0
) {
  console.error("Invalid --consistency-check-interval");
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
const readSemaphore = new Semaphore(readConcurrency);
const writeSemaphore = new Semaphore(writeConcurrency);

for (let i = 0; i < writeConcurrency; i++) {
  (async () => {
    while (true) {
      const a = Math.floor(
        Math.random() * Math.random() * Math.random() * size,
      );
      const b = Math.floor(
        Math.random() * Math.random() * Math.random() * size,
      );
      if (a === b) continue;

      const release = await writeSemaphore.acquire();

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
      release();
    }
  })();
}

for (let i = 0; i < readConcurrency; i++) {
  (async () => {
    while (true) {
      const release = await readSemaphore.acquire();
      const id = Math.floor(Math.random() * size);
      const { value } = await kv.get<bigint>(["accounts", id]);
      if (typeof value !== "bigint") {
        throw new Error("Invalid value");
      }
      metrics.reads++;
      release();
    }
  })();
}

let lastMetrics = { ...metrics };

for (let i = 0;; i++) {
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

  if (consistencyCheckInterval !== 0 && i % consistencyCheckInterval === 0) {
    const callbacks = await Promise.all([
      [readSemaphore, readConcurrency] as const,
      [writeSemaphore, writeConcurrency] as const,
    ].flatMap(([sem, concurrency]) =>
      Array(concurrency).fill(0).map(() => sem.acquire())
    ));

    const startTime = Date.now();
    const initSemaphore = new Semaphore(initConcurrency);
    let balanceSum = 0n;
    let absSum = 0n;
    for (let i = 0; i < size; i++) {
      const release = await initSemaphore.acquire();
      (async () => {
        const { value } = await kv.get<bigint>(["accounts", i]);
        if (typeof value !== "bigint") {
          throw new Error("Invalid value");
        }
        balanceSum += value;
        absSum += value < 0n ? -value : value;
        release();
      })();
    }
    for (let i = 0; i < initConcurrency; i++) {
      await initSemaphore.acquire();
    }
    if (balanceSum !== 0n) {
      throw new Error(`Consistency check failed: ${balanceSum}`);
    }
    console.log(
      `Consistency check passed in ${
        Date.now() - startTime
      }ms, absSum=${absSum}`,
    );
    callbacks.forEach((x) => x());
  }
}
