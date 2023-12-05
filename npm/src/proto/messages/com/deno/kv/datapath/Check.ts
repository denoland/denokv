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
  export type Check = {
    key: Uint8Array;
    versionstamp: Uint8Array;
  }
}

export type Type = $.com.deno.kv.datapath.Check;

export function getDefaultValue(): $.com.deno.kv.datapath.Check {
  return {
    key: new Uint8Array(),
    versionstamp: new Uint8Array(),
  };
}

export function createValue(partialValue: Partial<$.com.deno.kv.datapath.Check>): $.com.deno.kv.datapath.Check {
  return {
    ...getDefaultValue(),
    ...partialValue,
  };
}

export function encodeJson(value: $.com.deno.kv.datapath.Check): unknown {
  const result: any = {};
  if (value.key !== undefined) result.key = tsValueToJsonValueFns.bytes(value.key);
  if (value.versionstamp !== undefined) result.versionstamp = tsValueToJsonValueFns.bytes(value.versionstamp);
  return result;
}

export function decodeJson(value: any): $.com.deno.kv.datapath.Check {
  const result = getDefaultValue();
  if (value.key !== undefined) result.key = jsonValueToTsValueFns.bytes(value.key);
  if (value.versionstamp !== undefined) result.versionstamp = jsonValueToTsValueFns.bytes(value.versionstamp);
  return result;
}

export function encodeBinary(value: $.com.deno.kv.datapath.Check): Uint8Array {
  const result: WireMessage = [];
  if (value.key !== undefined) {
    const tsValue = value.key;
    result.push(
      [1, tsValueToWireValueFns.bytes(tsValue)],
    );
  }
  if (value.versionstamp !== undefined) {
    const tsValue = value.versionstamp;
    result.push(
      [2, tsValueToWireValueFns.bytes(tsValue)],
    );
  }
  return serialize(result);
}

export function decodeBinary(binary: Uint8Array): $.com.deno.kv.datapath.Check {
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
    result.versionstamp = value;
  }
  return result;
}
