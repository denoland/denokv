import { assertEquals } from "https://deno.land/std@0.208.0/assert/assert_equals.ts";
import { assertThrows } from "https://deno.land/std@0.208.0/assert/assert_throws.ts";
import { _KvU64 } from "./kv_u64.ts";

Deno.test({
  name: "_KvU64",
  fn: () => {
    // deno-lint-ignore no-explicit-any
    [[], {}, 1.2, null, undefined, 0, 123, "01", -1n, 1n << 64n].forEach((v) =>
      assertThrows(() => new _KvU64(v as any))
    );
    [0n, 123n, (1n << 64n) - 1n].forEach((v) =>
      assertEquals(new _KvU64(v).value, v)
    );
    [[0n, 0n, 0n], [1n, 2n, 3n], [(1n << 64n) - 1n, 1n, 0n]].forEach((v) =>
      assertEquals(
        new _KvU64(v[0]).sum(new _KvU64(v[1])).value,
        v[2],
        `${v[0]} + ${v[1]} = ${v[2]}`,
      )
    );
    [[0n, 0n, 0n], [1n, 2n, 1n], [(1n << 64n) - 1n, 1n, 1n]].forEach((v) =>
      assertEquals(
        new _KvU64(v[0]).min(new _KvU64(v[1])).value,
        v[2],
        `${v[0]} + ${v[1]} = ${v[2]}`,
      )
    );
    [[0n, 0n, 0n], [1n, 2n, 2n], [(1n << 64n) - 1n, 1n, (1n << 64n) - 1n]]
      .forEach((v) =>
        assertEquals(
          new _KvU64(v[0]).max(new _KvU64(v[1])).value,
          v[2],
          `${v[0]} + ${v[1]} = ${v[2]}`,
        )
      );
  },
});
