// Copyright 2023 the Deno authors. All rights reserved. MIT license.

import { decodeV8, encodeV8 } from "./v8.ts";
import { assertEquals } from "https://deno.land/std@0.208.0/assert/assert_equals.ts";

Deno.test({
  name: "encodeV8/decodeV8",
  fn: () => {
    const tests: [unknown, number[]][] = [
      ["bar", [255, 15, 34, 3, 98, 97, 114]],
      [null, [255, 15, 48]],
      [undefined, [255, 15, 95]],
      [true, [255, 15, 84]],
      [false, [255, 15, 70]],
      ["a😮b", [255, 15, 99, 8, 97, 0, 61, 216, 46, 222, 98, 0]],
      [0n, [255, 15, 90, 0]],
      // [ 1n, [ 255, 15, 90, 16, 1,  0,  0,  0,  0, 0, 0,  0 ] ],
    ];
    for (const [value, expected] of tests) {
      const encoded = encodeV8(value);
      assertEquals(encoded, new Uint8Array(expected), `${value}`);
      assertEquals(decodeV8(encoded), value);
    }
  },
});
