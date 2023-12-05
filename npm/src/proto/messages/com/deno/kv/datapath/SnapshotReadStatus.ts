// @ts-nocheck
export declare namespace $.com.deno.kv.datapath {
  export type SnapshotReadStatus =
    | "SR_UNSPECIFIED"
    | "SR_SUCCESS"
    | "SR_READ_DISABLED";
}

export type Type = $.com.deno.kv.datapath.SnapshotReadStatus;

export const num2name = {
  0: "SR_UNSPECIFIED",
  1: "SR_SUCCESS",
  2: "SR_READ_DISABLED",
} as const;

export const name2num = {
  SR_UNSPECIFIED: 0,
  SR_SUCCESS: 1,
  SR_READ_DISABLED: 2,
} as const;
