// @ts-nocheck
import {
  Type as BackupKvPair,
  encodeJson as encodeJson_1,
  decodeJson as decodeJson_1,
  encodeBinary as encodeBinary_1,
  decodeBinary as decodeBinary_1,
} from "./BackupKvPair.ts";
import {
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
  default as deserialize,
} from "../../../../../runtime/wire/deserialize.ts";

export declare namespace $.com.deno.kv.backup {
  export type BackupSnapshotRange = {
    dataList: BackupKvPair[];
    metadataList: BackupKvPair[];
  }
}

export type Type = $.com.deno.kv.backup.BackupSnapshotRange;

export function getDefaultValue(): $.com.deno.kv.backup.BackupSnapshotRange {
  return {
    dataList: [],
    metadataList: [],
  };
}

export function createValue(partialValue: Partial<$.com.deno.kv.backup.BackupSnapshotRange>): $.com.deno.kv.backup.BackupSnapshotRange {
  return {
    ...getDefaultValue(),
    ...partialValue,
  };
}

export function encodeJson(value: $.com.deno.kv.backup.BackupSnapshotRange): unknown {
  const result: any = {};
  result.dataList = value.dataList.map(value => encodeJson_1(value));
  result.metadataList = value.metadataList.map(value => encodeJson_1(value));
  return result;
}

export function decodeJson(value: any): $.com.deno.kv.backup.BackupSnapshotRange {
  const result = getDefaultValue();
  result.dataList = value.dataList?.map((value: any) => decodeJson_1(value)) ?? [];
  result.metadataList = value.metadataList?.map((value: any) => decodeJson_1(value)) ?? [];
  return result;
}

export function encodeBinary(value: $.com.deno.kv.backup.BackupSnapshotRange): Uint8Array {
  const result: WireMessage = [];
  for (const tsValue of value.dataList) {
    result.push(
      [1, { type: WireType.LengthDelimited as const, value: encodeBinary_1(tsValue) }],
    );
  }
  for (const tsValue of value.metadataList) {
    result.push(
      [2, { type: WireType.LengthDelimited as const, value: encodeBinary_1(tsValue) }],
    );
  }
  return serialize(result);
}

export function decodeBinary(binary: Uint8Array): $.com.deno.kv.backup.BackupSnapshotRange {
  const result = getDefaultValue();
  const wireMessage = deserialize(binary);
  const wireFields = new Map(wireMessage);
  collection: {
    const wireValues = wireMessage.filter(([fieldNumber]) => fieldNumber === 1).map(([, wireValue]) => wireValue);
    const value = wireValues.map((wireValue) => wireValue.type === WireType.LengthDelimited ? decodeBinary_1(wireValue.value) : undefined).filter(x => x !== undefined);
    if (!value.length) break collection;
    result.dataList = value as any;
  }
  collection: {
    const wireValues = wireMessage.filter(([fieldNumber]) => fieldNumber === 2).map(([, wireValue]) => wireValue);
    const value = wireValues.map((wireValue) => wireValue.type === WireType.LengthDelimited ? decodeBinary_1(wireValue.value) : undefined).filter(x => x !== undefined);
    if (!value.length) break collection;
    result.metadataList = value as any;
  }
  return result;
}
