// @ts-nocheck
import {
  Type as AtomicWriteStatus,
  name2num,
  num2name,
} from "./AtomicWriteStatus.ts";
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
  default as Long,
} from "../../../../../runtime/Long.ts";
import {
  tsValueToWireValueFns,
  wireValueToTsValueFns,
  unpackFns,
} from "../../../../../runtime/wire/scalar.ts";
import {
  default as deserialize,
} from "../../../../../runtime/wire/deserialize.ts";

export declare namespace $.com.deno.kv.datapath {
  export type AtomicWriteOutput = {
    status: AtomicWriteStatus;
    versionstamp: Uint8Array;
    failedChecks: number[];
  }
}

export type Type = $.com.deno.kv.datapath.AtomicWriteOutput;

export function getDefaultValue(): $.com.deno.kv.datapath.AtomicWriteOutput {
  return {
    status: "AW_UNSPECIFIED",
    versionstamp: new Uint8Array(),
    failedChecks: [],
  };
}

export function createValue(partialValue: Partial<$.com.deno.kv.datapath.AtomicWriteOutput>): $.com.deno.kv.datapath.AtomicWriteOutput {
  return {
    ...getDefaultValue(),
    ...partialValue,
  };
}

export function encodeJson(value: $.com.deno.kv.datapath.AtomicWriteOutput): unknown {
  const result: any = {};
  if (value.status !== undefined) result.status = tsValueToJsonValueFns.enum(value.status);
  if (value.versionstamp !== undefined) result.versionstamp = tsValueToJsonValueFns.bytes(value.versionstamp);
  result.failedChecks = value.failedChecks.map(value => tsValueToJsonValueFns.uint32(value));
  return result;
}

export function decodeJson(value: any): $.com.deno.kv.datapath.AtomicWriteOutput {
  const result = getDefaultValue();
  if (value.status !== undefined) result.status = jsonValueToTsValueFns.enum(value.status) as AtomicWriteStatus;
  if (value.versionstamp !== undefined) result.versionstamp = jsonValueToTsValueFns.bytes(value.versionstamp);
  result.failedChecks = value.failedChecks?.map((value: any) => jsonValueToTsValueFns.uint32(value)) ?? [];
  return result;
}

export function encodeBinary(value: $.com.deno.kv.datapath.AtomicWriteOutput): Uint8Array {
  const result: WireMessage = [];
  if (value.status !== undefined) {
    const tsValue = value.status;
    result.push(
      [1, { type: WireType.Varint as const, value: new Long(name2num[tsValue as keyof typeof name2num]) }],
    );
  }
  if (value.versionstamp !== undefined) {
    const tsValue = value.versionstamp;
    result.push(
      [2, tsValueToWireValueFns.bytes(tsValue)],
    );
  }
  for (const tsValue of value.failedChecks) {
    result.push(
      [4, tsValueToWireValueFns.uint32(tsValue)],
    );
  }
  return serialize(result);
}

export function decodeBinary(binary: Uint8Array): $.com.deno.kv.datapath.AtomicWriteOutput {
  const result = getDefaultValue();
  const wireMessage = deserialize(binary);
  const wireFields = new Map(wireMessage);
  field: {
    const wireValue = wireFields.get(1);
    if (wireValue === undefined) break field;
    const value = wireValue.type === WireType.Varint ? num2name[wireValue.value[0] as keyof typeof num2name] : undefined;
    if (value === undefined) break field;
    result.status = value;
  }
  field: {
    const wireValue = wireFields.get(2);
    if (wireValue === undefined) break field;
    const value = wireValueToTsValueFns.bytes(wireValue);
    if (value === undefined) break field;
    result.versionstamp = value;
  }
  collection: {
    const wireValues = wireMessage.filter(([fieldNumber]) => fieldNumber === 4).map(([, wireValue]) => wireValue);
    const value = Array.from(unpackFns.uint32(wireValues));
    if (!value.length) break collection;
    result.failedChecks = value as any;
  }
  return result;
}
