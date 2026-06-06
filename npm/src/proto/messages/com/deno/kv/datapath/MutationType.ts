// @ts-nocheck
export declare namespace $.com.deno.kv.datapath {
  export type MutationType =
    | "M_UNSPECIFIED"
    | "M_SET"
    | "M_DELETE"
    | "M_SUM"
    | "M_MAX"
    | "M_MIN"
    | "M_SET_SUFFIX_VERSIONSTAMPED_KEY";
}

export type Type = $.com.deno.kv.datapath.MutationType;

export const num2name = {
  0: "M_UNSPECIFIED",
  1: "M_SET",
  2: "M_DELETE",
  3: "M_SUM",
  4: "M_MAX",
  5: "M_MIN",
  9: "M_SET_SUFFIX_VERSIONSTAMPED_KEY",
} as const;

export const name2num = {
  M_UNSPECIFIED: 0,
  M_SET: 1,
  M_DELETE: 2,
  M_SUM: 3,
  M_MAX: 4,
  M_MIN: 5,
  M_SET_SUFFIX_VERSIONSTAMPED_KEY: 9,
} as const;
