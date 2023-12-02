// @ts-nocheck
import {
  Type as KvEntry,
  encodeJson as encodeJson_1,
  decodeJson as decodeJson_1,
  encodeBinary as encodeBinary_1,
  decodeBinary as decodeBinary_1,
} from "./KvEntry.ts";
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

export declare namespace $.com.deno.kv.datapath {
  export type WatchKeyOutput = {
    changed: boolean;
    entryIfChanged?: KvEntry;
  }
}

export type Type = $.com.deno.kv.datapath.WatchKeyOutput;

export function getDefaultValue(): $.com.deno.kv.datapath.WatchKeyOutput {
  return {
    changed: false,
    entryIfChanged: undefined,
  };
}

export function createValue(partialValue: Partial<$.com.deno.kv.datapath.WatchKeyOutput>): $.com.deno.kv.datapath.WatchKeyOutput {
  return {
    ...getDefaultValue(),
    ...partialValue,
  };
}

export function encodeJson(value: $.com.deno.kv.datapath.WatchKeyOutput): unknown {
  const result: any = {};
  if (value.changed !== undefined) result.changed = tsValueToJsonValueFns.bool(value.changed);
  if (value.entryIfChanged !== undefined) result.entryIfChanged = encodeJson_1(value.entryIfChanged);
  return result;
}

export function decodeJson(value: any): $.com.deno.kv.datapath.WatchKeyOutput {
  const result = getDefaultValue();
  if (value.changed !== undefined) result.changed = jsonValueToTsValueFns.bool(value.changed);
  if (value.entryIfChanged !== undefined) result.entryIfChanged = decodeJson_1(value.entryIfChanged);
  return result;
}

export function encodeBinary(value: $.com.deno.kv.datapath.WatchKeyOutput): Uint8Array {
  const result: WireMessage = [];
  if (value.changed !== undefined) {
    const tsValue = value.changed;
    result.push(
      [1, tsValueToWireValueFns.bool(tsValue)],
    );
  }
  if (value.entryIfChanged !== undefined) {
    const tsValue = value.entryIfChanged;
    result.push(
      [2, { type: WireType.LengthDelimited as const, value: encodeBinary_1(tsValue) }],
    );
  }
  return serialize(result);
}

export function decodeBinary(binary: Uint8Array): $.com.deno.kv.datapath.WatchKeyOutput {
  const result = getDefaultValue();
  const wireMessage = deserialize(binary);
  const wireFields = new Map(wireMessage);
  field: {
    const wireValue = wireFields.get(1);
    if (wireValue === undefined) break field;
    const value = wireValueToTsValueFns.bool(wireValue);
    if (value === undefined) break field;
    result.changed = value;
  }
  field: {
    const wireValue = wireFields.get(2);
    if (wireValue === undefined) break field;
    const value = wireValue.type === WireType.LengthDelimited ? decodeBinary_1(wireValue.value) : undefined;
    if (value === undefined) break field;
    result.entryIfChanged = value;
  }
  return result;
}
