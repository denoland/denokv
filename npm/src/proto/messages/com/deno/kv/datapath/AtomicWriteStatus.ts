// @ts-nocheck
export declare namespace $.com.deno.kv.datapath {
  export type AtomicWriteStatus =
    | "AW_UNSPECIFIED"
    | "AW_SUCCESS"
    | "AW_CHECK_FAILURE"
    | "AW_WRITE_DISABLED";
}

export type Type = $.com.deno.kv.datapath.AtomicWriteStatus;

export const num2name = {
  0: "AW_UNSPECIFIED",
  1: "AW_SUCCESS",
  2: "AW_CHECK_FAILURE",
  5: "AW_WRITE_DISABLED",
} as const;

export const name2num = {
  AW_UNSPECIFIED: 0,
  AW_SUCCESS: 1,
  AW_CHECK_FAILURE: 2,
  AW_WRITE_DISABLED: 5,
} as const;
