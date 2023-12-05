// @ts-nocheck
import {
  tsValueToJsonValueFns,
  jsonValueToTsValueFns,
} from "../../../../../runtime/json/scalar.ts";
import {
  WireMessage,
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

export declare namespace $.com.deno.kv.datapath {
  export type ReadRange = {
    start: Uint8Array;
    end: Uint8Array;
    limit: number;
    reverse: boolean;
  }
}

export type Type = $.com.deno.kv.datapath.ReadRange;

export function getDefaultValue(): $.com.deno.kv.datapath.ReadRange {
  return {
    start: new Uint8Array(),
    end: new Uint8Array(),
    limit: 0,
    reverse: false,
  };
}

export function createValue(partialValue: Partial<$.com.deno.kv.datapath.ReadRange>): $.com.deno.kv.datapath.ReadRange {
  return {
    ...getDefaultValue(),
    ...partialValue,
  };
}

export function encodeJson(value: $.com.deno.kv.datapath.ReadRange): unknown {
  const result: any = {};
  if (value.start !== undefined) result.start = tsValueToJsonValueFns.bytes(value.start);
  if (value.end !== undefined) result.end = tsValueToJsonValueFns.bytes(value.end);
  if (value.limit !== undefined) result.limit = tsValueToJsonValueFns.int32(value.limit);
  if (value.reverse !== undefined) result.reverse = tsValueToJsonValueFns.bool(value.reverse);
  return result;
}

export function decodeJson(value: any): $.com.deno.kv.datapath.ReadRange {
  const result = getDefaultValue();
  if (value.start !== undefined) result.start = jsonValueToTsValueFns.bytes(value.start);
  if (value.end !== undefined) result.end = jsonValueToTsValueFns.bytes(value.end);
  if (value.limit !== undefined) result.limit = jsonValueToTsValueFns.int32(value.limit);
  if (value.reverse !== undefined) result.reverse = jsonValueToTsValueFns.bool(value.reverse);
  return result;
}

export function encodeBinary(value: $.com.deno.kv.datapath.ReadRange): Uint8Array {
  const result: WireMessage = [];
  if (value.start !== undefined) {
    const tsValue = value.start;
    result.push(
      [1, tsValueToWireValueFns.bytes(tsValue)],
    );
  }
  if (value.end !== undefined) {
    const tsValue = value.end;
    result.push(
      [2, tsValueToWireValueFns.bytes(tsValue)],
    );
  }
  if (value.limit !== undefined) {
    const tsValue = value.limit;
    result.push(
      [3, tsValueToWireValueFns.int32(tsValue)],
    );
  }
  if (value.reverse !== undefined) {
    const tsValue = value.reverse;
    result.push(
      [4, tsValueToWireValueFns.bool(tsValue)],
    );
  }
  return serialize(result);
}

export function decodeBinary(binary: Uint8Array): $.com.deno.kv.datapath.ReadRange {
  const result = getDefaultValue();
  const wireMessage = deserialize(binary);
  const wireFields = new Map(wireMessage);
  field: {
    const wireValue = wireFields.get(1);
    if (wireValue === undefined) break field;
    const value = wireValueToTsValueFns.bytes(wireValue);
    if (value === undefined) break field;
    result.start = value;
  }
  field: {
    const wireValue = wireFields.get(2);
    if (wireValue === undefined) break field;
    const value = wireValueToTsValueFns.bytes(wireValue);
    if (value === undefined) break field;
    result.end = value;
  }
  field: {
    const wireValue = wireFields.get(3);
    if (wireValue === undefined) break field;
    const value = wireValueToTsValueFns.int32(wireValue);
    if (value === undefined) break field;
    result.limit = value;
  }
  field: {
    const wireValue = wireFields.get(4);
    if (wireValue === undefined) break field;
    const value = wireValueToTsValueFns.bool(wireValue);
    if (value === undefined) break field;
    result.reverse = value;
  }
  return result;
}
