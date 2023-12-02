// @ts-nocheck
export declare namespace $.com.deno.kv.backup {
  export type BackupKvMutationKind =
    | "MK_UNSPECIFIED"
    | "MK_SET"
    | "MK_CLEAR"
    | "MK_SUM"
    | "MK_MAX"
    | "MK_MIN";
}

export type Type = $.com.deno.kv.backup.BackupKvMutationKind;

export const num2name = {
  0: "MK_UNSPECIFIED",
  1: "MK_SET",
  2: "MK_CLEAR",
  3: "MK_SUM",
  4: "MK_MAX",
  5: "MK_MIN",
} as const;

export const name2num = {
  MK_UNSPECIFIED: 0,
  MK_SET: 1,
  MK_CLEAR: 2,
  MK_SUM: 3,
  MK_MAX: 4,
  MK_MIN: 5,
} as const;
