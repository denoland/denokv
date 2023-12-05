// @ts-nocheck
import {
  Type as ValueEncoding,
  name2num,
  num2name,
} from "./ValueEncoding.ts";
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

export declare namespace $.com.deno.kv.datapath {
  export type KvEntry = {
    key: Uint8Array;
    value: Uint8Array;
    encoding: ValueEncoding;
    versionstamp: Uint8Array;
  }
}

export type Type = $.com.deno.kv.datapath.KvEntry;

export function getDefaultValue(): $.com.deno.kv.datapath.KvEntry {
  return {
    key: new Uint8Array(),
    value: new Uint8Array(),
    encoding: "VE_UNSPECIFIED",
    versionstamp: new Uint8Array(),
  };
}

export function createValue(partialValue: Partial<$.com.deno.kv.datapath.KvEntry>): $.com.deno.kv.datapath.KvEntry {
  return {
    ...getDefaultValue(),
    ...partialValue,
  };
}

export function encodeJson(value: $.com.deno.kv.datapath.KvEntry): unknown {
  const result: any = {};
  if (value.key !== undefined) result.key = tsValueToJsonValueFns.bytes(value.key);
  if (value.value !== undefined) result.value = tsValueToJsonValueFns.bytes(value.value);
  if (value.encoding !== undefined) result.encoding = tsValueToJsonValueFns.enum(value.encoding);
  if (value.versionstamp !== undefined) result.versionstamp = tsValueToJsonValueFns.bytes(value.versionstamp);
  return result;
}

export function decodeJson(value: any): $.com.deno.kv.datapath.KvEntry {
  const result = getDefaultValue();
  if (value.key !== undefined) result.key = jsonValueToTsValueFns.bytes(value.key);
  if (value.value !== undefined) result.value = jsonValueToTsValueFns.bytes(value.value);
  if (value.encoding !== undefined) result.encoding = jsonValueToTsValueFns.enum(value.encoding) as ValueEncoding;
  if (value.versionstamp !== undefined) result.versionstamp = jsonValueToTsValueFns.bytes(value.versionstamp);
  return result;
}

export function encodeBinary(value: $.com.deno.kv.datapath.KvEntry): Uint8Array {
  const result: WireMessage = [];
  if (value.key !== undefined) {
    const tsValue = value.key;
    result.push(
      [1, tsValueToWireValueFns.bytes(tsValue)],
    );
  }
  if (value.value !== undefined) {
    const tsValue = value.value;
    result.push(
      [2, tsValueToWireValueFns.bytes(tsValue)],
    );
  }
  if (value.encoding !== undefined) {
    const tsValue = value.encoding;
    result.push(
      [3, { type: WireType.Varint as const, value: new Long(name2num[tsValue as keyof typeof name2num]) }],
    );
  }
  if (value.versionstamp !== undefined) {
    const tsValue = value.versionstamp;
    result.push(
      [4, tsValueToWireValueFns.bytes(tsValue)],
    );
  }
  return serialize(result);
}

export function decodeBinary(binary: Uint8Array): $.com.deno.kv.datapath.KvEntry {
  const result = getDefaultValue();
  const wireMessage = deserialize(binary);
  const wireFields = new Map(wireMessage);
  field: {
    const wireValue = wireFields.get(1);
    if (wireValue === undefined) break field;
    const value = wireValueToTsValueFns.bytes(wireValue);
    if (value === undefined) break field;
    result.key = value;
  }
  field: {
    const wireValue = wireFields.get(2);
    if (wireValue === undefined) break field;
    const value = wireValueToTsValueFns.bytes(wireValue);
    if (value === undefined) break field;
    result.value = value;
  }
  field: {
    const wireValue = wireFields.get(3);
    if (wireValue === undefined) break field;
    const value = wireValue.type === WireType.Varint ? num2name[wireValue.value[0] as keyof typeof num2name] : undefined;
    if (value === undefined) break field;
    result.encoding = value;
  }
  field: {
    const wireValue = wireFields.get(4);
    if (wireValue === undefined) break field;
    const value = wireValueToTsValueFns.bytes(wireValue);
    if (value === undefined) break field;
    result.versionstamp = value;
  }
  return result;
}
