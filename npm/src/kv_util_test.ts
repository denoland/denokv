// Copyright 2023 the Deno authors. All rights reserved. MIT license.

import { assertEquals } from "https://deno.land/std@0.208.0/assert/assert_equals.ts";
import { assertThrows } from "https://deno.land/std@0.208.0/assert/assert_throws.ts";
import { checkListSelector } from "./kv_util.ts";

Deno.test({
  name: "checkListSelector ignores selector keys with undefined values",
  fn: () => {
    assertEquals(
      checkListSelector(
        { prefix: ["users"], start: undefined, end: undefined } as never,
      ),
      { prefix: ["users"] },
    );
    assertEquals(
      checkListSelector(
        { start: ["a"], end: ["b"], prefix: undefined } as never,
      ),
      { start: ["a"], end: ["b"] },
    );
  },
});

Deno.test({
  name: "checkListSelector requires range keys to be in the prefix keyspace",
  fn: () => {
    assertThrows(
      () => checkListSelector({ prefix: ["users"], end: ["C"] }),
      TypeError,
      "End key is not in the keyspace defined by prefix",
    );
    assertThrows(
      () => checkListSelector({ prefix: ["users"], start: ["C"] }),
      TypeError,
      "Start key is not in the keyspace defined by prefix",
    );
    // a key equal to the prefix is not in its keyspace
    assertThrows(
      () => checkListSelector({ prefix: ["users"], start: ["users"] }),
      TypeError,
      "Start key is not in the keyspace defined by prefix",
    );
    assertEquals(
      checkListSelector({ prefix: ["users"], end: ["users", "C"] }),
      { prefix: ["users"], end: ["users", "C"] },
    );
  },
});

Deno.test({
  name: "checkListSelector rejects incomplete selectors",
  fn: () => {
    for (const selector of [{ start: [10] }, { end: [10] }, {}]) {
      assertThrows(
        () => checkListSelector(selector as never),
        TypeError,
        "Selector must specify either 'prefix' or both 'start' and 'end' key",
      );
    }
  },
});

Deno.test({
  name: "checkListSelector rejects a start key greater than the end key",
  fn: () => {
    assertThrows(
      () => checkListSelector({ start: ["b"], end: ["a"] }),
      TypeError,
      "Start key is greater than end key",
    );
  },
});

Deno.test({
  name: "list accepts selectors with undefined range keys",
  fn: async () => {
    const { openKv } = await import("./npm.ts");
    const kv = await openKv(undefined, { implementation: "in-memory" });
    await kv.set(["users", "a"], 1);
    const entries = [];
    for await (
      const entry of kv.list({
        prefix: ["users"],
        start: undefined,
        end: undefined,
      } as never)
    ) {
      entries.push(entry.key);
    }
    assertEquals(entries, [["users", "a"]]);
    kv.close();
  },
});
