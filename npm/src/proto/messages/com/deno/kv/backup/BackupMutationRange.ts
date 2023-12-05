// @ts-nocheck
import {
  Type as BackupReplicationLogEntry,
  encodeJson as encodeJson_1,
  decodeJson as decodeJson_1,
  encodeBinary as encodeBinary_1,
  decodeBinary as decodeBinary_1,
} from "./BackupReplicationLogEntry.ts";
import {
  tsValueToJsonValueFns,
  jsonValueToTsValueFns,
} from "../../../../../runtime/json/scalar.ts";
import {
  WireMessage,
  WireType,
} from "../../../../../runtime/wire/index.ts";
import {
  default as serialize,
} from "../../../../../runtime/wire/serialize.ts";
import {
  tsValueToWireValueFns,
  wireValueToTsValueFns,
} from "../../../../../runtime/wire/scalar.ts";
import {
  default as deserialize,
} from "../../../../../runtime/wire/deserialize.ts";

export declare namespace $.com.deno.kv.backup {
  export type BackupMutationRange = {
    entries: BackupReplicationLogEntry[];
    timestampMs: string;
  }
}

export type Type = $.com.deno.kv.backup.BackupMutationRange;

export function getDefaultValue(): $.com.deno.kv.backup.BackupMutationRange {
  return {
    entries: [],
    timestampMs: "0",
  };
}

export function createValue(partialValue: Partial<$.com.deno.kv.backup.BackupMutationRange>): $.com.deno.kv.backup.BackupMutationRange {
  return {
    ...getDefaultValue(),
    ...partialValue,
  };
}

export function encodeJson(value: $.com.deno.kv.backup.BackupMutationRange): unknown {
  const result: any = {};
  result.entries = value.entries.map(value => encodeJson_1(value));
  if (value.timestampMs !== undefined) result.timestampMs = tsValueToJsonValueFns.uint64(value.timestampMs);
  return result;
}

export function decodeJson(value: any): $.com.deno.kv.backup.BackupMutationRange {
  const result = getDefaultValue();
  result.entries = value.entries?.map((value: any) => decodeJson_1(value)) ?? [];
  if (value.timestampMs !== undefined) result.timestampMs = jsonValueToTsValueFns.uint64(value.timestampMs);
  return result;
}

export function encodeBinary(value: $.com.deno.kv.backup.BackupMutationRange): Uint8Array {
  const result: WireMessage = [];
  for (const tsValue of value.entries) {
    result.push(
      [1, { type: WireType.LengthDelimited as const, value: encodeBinary_1(tsValue) }],
    );
  }
  if (value.timestampMs !== undefined) {
    const tsValue = value.timestampMs;
    result.push(
      [2, tsValueToWireValueFns.uint64(tsValue)],
    );
  }
  return serialize(result);
}

export function decodeBinary(binary: Uint8Array): $.com.deno.kv.backup.BackupMutationRange {
  const result = getDefaultValue();
  const wireMessage = deserialize(binary);
  const wireFields = new Map(wireMessage);
  collection: {
    const wireValues = wireMessage.filter(([fieldNumber]) => fieldNumber === 1).map(([, wireValue]) => wireValue);
    const value = wireValues.map((wireValue) => wireValue.type === WireType.LengthDelimited ? decodeBinary_1(wireValue.value) : undefined).filter(x => x !== undefined);
    if (!value.length) break collection;
    result.entries = value as any;
  }
  field: {
    const wireValue = wireFields.get(2);
    if (wireValue === undefined) break field;
    const value = wireValueToTsValueFns.uint64(wireValue);
    if (value === undefined) break field;
    result.timestampMs = value;
  }
  return result;
}
