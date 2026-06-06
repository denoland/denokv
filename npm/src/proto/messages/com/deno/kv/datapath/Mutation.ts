// @ts-nocheck
import {
  Type as KvValue,
  encodeJson as encodeJson_1,
  decodeJson as decodeJson_1,
  encodeBinary as encodeBinary_1,
  decodeBinary as decodeBinary_1,
} from "./KvValue.ts";
import {
  Type as MutationType,
  name2num,
  num2name,
} from "./MutationType.ts";
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
  export type Mutation = {
    key: Uint8Array;
    value?: KvValue;
    mutationType: MutationType;
    expireAtMs: string;
    sumMin: Uint8Array;
    sumMax: Uint8Array;
    sumClamp: boolean;
  }
}

export type Type = $.com.deno.kv.datapath.Mutation;

export function getDefaultValue(): $.com.deno.kv.datapath.Mutation {
  return {
    key: new Uint8Array(),
    value: undefined,
    mutationType: "M_UNSPECIFIED",
    expireAtMs: "0",
    sumMin: new Uint8Array(),
    sumMax: new Uint8Array(),
    sumClamp: false,
  };
}

export function createValue(partialValue: Partial<$.com.deno.kv.datapath.Mutation>): $.com.deno.kv.datapath.Mutation {
  return {
    ...getDefaultValue(),
    ...partialValue,
  };
}

export function encodeJson(value: $.com.deno.kv.datapath.Mutation): unknown {
  const result: any = {};
  if (value.key !== undefined) result.key = tsValueToJsonValueFns.bytes(value.key);
  if (value.value !== undefined) result.value = encodeJson_1(value.value);
  if (value.mutationType !== undefined) result.mutationType = tsValueToJsonValueFns.enum(value.mutationType);
  if (value.expireAtMs !== undefined) result.expireAtMs = tsValueToJsonValueFns.int64(value.expireAtMs);
  if (value.sumMin !== undefined) result.sumMin = tsValueToJsonValueFns.bytes(value.sumMin);
  if (value.sumMax !== undefined) result.sumMax = tsValueToJsonValueFns.bytes(value.sumMax);
  if (value.sumClamp !== undefined) result.sumClamp = tsValueToJsonValueFns.bool(value.sumClamp);
  return result;
}

export function decodeJson(value: any): $.com.deno.kv.datapath.Mutation {
  const result = getDefaultValue();
  if (value.key !== undefined) result.key = jsonValueToTsValueFns.bytes(value.key);
  if (value.value !== undefined) result.value = decodeJson_1(value.value);
  if (value.mutationType !== undefined) result.mutationType = jsonValueToTsValueFns.enum(value.mutationType) as MutationType;
  if (value.expireAtMs !== undefined) result.expireAtMs = jsonValueToTsValueFns.int64(value.expireAtMs);
  if (value.sumMin !== undefined) result.sumMin = jsonValueToTsValueFns.bytes(value.sumMin);
  if (value.sumMax !== undefined) result.sumMax = jsonValueToTsValueFns.bytes(value.sumMax);
  if (value.sumClamp !== undefined) result.sumClamp = jsonValueToTsValueFns.bool(value.sumClamp);
  return result;
}

export function encodeBinary(value: $.com.deno.kv.datapath.Mutation): Uint8Array {
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
      [2, { type: WireType.LengthDelimited as const, value: encodeBinary_1(tsValue) }],
    );
  }
  if (value.mutationType !== undefined) {
    const tsValue = value.mutationType;
    result.push(
      [3, { type: WireType.Varint as const, value: new Long(name2num[tsValue as keyof typeof name2num]) }],
    );
  }
  if (value.expireAtMs !== undefined) {
    const tsValue = value.expireAtMs;
    result.push(
      [4, tsValueToWireValueFns.int64(tsValue)],
    );
  }
  if (value.sumMin !== undefined) {
    const tsValue = value.sumMin;
    result.push(
      [5, tsValueToWireValueFns.bytes(tsValue)],
    );
  }
  if (value.sumMax !== undefined) {
    const tsValue = value.sumMax;
    result.push(
      [6, tsValueToWireValueFns.bytes(tsValue)],
    );
  }
  if (value.sumClamp !== undefined) {
    const tsValue = value.sumClamp;
    result.push(
      [7, tsValueToWireValueFns.bool(tsValue)],
    );
  }
  return serialize(result);
}

export function decodeBinary(binary: Uint8Array): $.com.deno.kv.datapath.Mutation {
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
    const value = wireValue.type === WireType.LengthDelimited ? decodeBinary_1(wireValue.value) : undefined;
    if (value === undefined) break field;
    result.value = value;
  }
  field: {
    const wireValue = wireFields.get(3);
    if (wireValue === undefined) break field;
    const value = wireValue.type === WireType.Varint ? num2name[wireValue.value[0] as keyof typeof num2name] : undefined;
    if (value === undefined) break field;
    result.mutationType = value;
  }
  field: {
    const wireValue = wireFields.get(4);
    if (wireValue === undefined) break field;
    const value = wireValueToTsValueFns.int64(wireValue);
    if (value === undefined) break field;
    result.expireAtMs = value;
  }
  field: {
    const wireValue = wireFields.get(5);
    if (wireValue === undefined) break field;
    const value = wireValueToTsValueFns.bytes(wireValue);
    if (value === undefined) break field;
    result.sumMin = value;
  }
  field: {
    const wireValue = wireFields.get(6);
    if (wireValue === undefined) break field;
    const value = wireValueToTsValueFns.bytes(wireValue);
    if (value === undefined) break field;
    result.sumMax = value;
  }
  field: {
    const wireValue = wireFields.get(7);
    if (wireValue === undefined) break field;
    const value = wireValueToTsValueFns.bool(wireValue);
    if (value === undefined) break field;
    result.sumClamp = value;
  }
  return result;
}
