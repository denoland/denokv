// @ts-nocheck
import {
  Type as BackupKvMutationKind,
  name2num,
  num2name,
} from "./BackupKvMutationKind.ts";
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
  default as Long,
} from "../../../../../runtime/Long.ts";
import {
  default as deserialize,
} from "../../../../../runtime/wire/deserialize.ts";

export declare namespace $.com.deno.kv.backup {
  export type BackupReplicationLogEntry = {
    versionstamp: Uint8Array;
    kind: BackupKvMutationKind;
    key: Uint8Array;
    value: Uint8Array;
    valueEncoding: number;
    expireAtMs: string;
  }
}

export type Type = $.com.deno.kv.backup.BackupReplicationLogEntry;

export function getDefaultValue(): $.com.deno.kv.backup.BackupReplicationLogEntry {
  return {
    versionstamp: new Uint8Array(),
    kind: "MK_UNSPECIFIED",
    key: new Uint8Array(),
    value: new Uint8Array(),
    valueEncoding: 0,
    expireAtMs: "0",
  };
}

export function createValue(partialValue: Partial<$.com.deno.kv.backup.BackupReplicationLogEntry>): $.com.deno.kv.backup.BackupReplicationLogEntry {
  return {
    ...getDefaultValue(),
    ...partialValue,
  };
}

export function encodeJson(value: $.com.deno.kv.backup.BackupReplicationLogEntry): unknown {
  const result: any = {};
  if (value.versionstamp !== undefined) result.versionstamp = tsValueToJsonValueFns.bytes(value.versionstamp);
  if (value.kind !== undefined) result.kind = tsValueToJsonValueFns.enum(value.kind);
  if (value.key !== undefined) result.key = tsValueToJsonValueFns.bytes(value.key);
  if (value.value !== undefined) result.value = tsValueToJsonValueFns.bytes(value.value);
  if (value.valueEncoding !== undefined) result.valueEncoding = tsValueToJsonValueFns.int32(value.valueEncoding);
  if (value.expireAtMs !== undefined) result.expireAtMs = tsValueToJsonValueFns.uint64(value.expireAtMs);
  return result;
}

export function decodeJson(value: any): $.com.deno.kv.backup.BackupReplicationLogEntry {
  const result = getDefaultValue();
  if (value.versionstamp !== undefined) result.versionstamp = jsonValueToTsValueFns.bytes(value.versionstamp);
  if (value.kind !== undefined) result.kind = jsonValueToTsValueFns.enum(value.kind) as BackupKvMutationKind;
  if (value.key !== undefined) result.key = jsonValueToTsValueFns.bytes(value.key);
  if (value.value !== undefined) result.value = jsonValueToTsValueFns.bytes(value.value);
  if (value.valueEncoding !== undefined) result.valueEncoding = jsonValueToTsValueFns.int32(value.valueEncoding);
  if (value.expireAtMs !== undefined) result.expireAtMs = jsonValueToTsValueFns.uint64(value.expireAtMs);
  return result;
}

export function encodeBinary(value: $.com.deno.kv.backup.BackupReplicationLogEntry): Uint8Array {
  const result: WireMessage = [];
  if (value.versionstamp !== undefined) {
    const tsValue = value.versionstamp;
    result.push(
      [1, tsValueToWireValueFns.bytes(tsValue)],
    );
  }
  if (value.kind !== undefined) {
    const tsValue = value.kind;
    result.push(
      [2, { type: WireType.Varint as const, value: new Long(name2num[tsValue as keyof typeof name2num]) }],
    );
  }
  if (value.key !== undefined) {
    const tsValue = value.key;
    result.push(
      [3, tsValueToWireValueFns.bytes(tsValue)],
    );
  }
  if (value.value !== undefined) {
    const tsValue = value.value;
    result.push(
      [4, tsValueToWireValueFns.bytes(tsValue)],
    );
  }
  if (value.valueEncoding !== undefined) {
    const tsValue = value.valueEncoding;
    result.push(
      [5, tsValueToWireValueFns.int32(tsValue)],
    );
  }
  if (value.expireAtMs !== undefined) {
    const tsValue = value.expireAtMs;
    result.push(
      [6, tsValueToWireValueFns.uint64(tsValue)],
    );
  }
  return serialize(result);
}

export function decodeBinary(binary: Uint8Array): $.com.deno.kv.backup.BackupReplicationLogEntry {
  const result = getDefaultValue();
  const wireMessage = deserialize(binary);
  const wireFields = new Map(wireMessage);
  field: {
    const wireValue = wireFields.get(1);
    if (wireValue === undefined) break field;
    const value = wireValueToTsValueFns.bytes(wireValue);
    if (value === undefined) break field;
    result.versionstamp = value;
  }
  field: {
    const wireValue = wireFields.get(2);
    if (wireValue === undefined) break field;
    const value = wireValue.type === WireType.Varint ? num2name[wireValue.value[0] as keyof typeof num2name] : undefined;
    if (value === undefined) break field;
    result.kind = value;
  }
  field: {
    const wireValue = wireFields.get(3);
    if (wireValue === undefined) break field;
    const value = wireValueToTsValueFns.bytes(wireValue);
    if (value === undefined) break field;
    result.key = value;
  }
  field: {
    const wireValue = wireFields.get(4);
    if (wireValue === undefined) break field;
    const value = wireValueToTsValueFns.bytes(wireValue);
    if (value === undefined) break field;
    result.value = value;
  }
  field: {
    const wireValue = wireFields.get(5);
    if (wireValue === undefined) break field;
    const value = wireValueToTsValueFns.int32(wireValue);
    if (value === undefined) break field;
    result.valueEncoding = value;
  }
  field: {
    const wireValue = wireFields.get(6);
    if (wireValue === undefined) break field;
    const value = wireValueToTsValueFns.uint64(wireValue);
    if (value === undefined) break field;
    result.expireAtMs = value;
  }
  return result;
}
