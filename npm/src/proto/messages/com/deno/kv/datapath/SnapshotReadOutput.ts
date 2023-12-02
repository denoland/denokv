// @ts-nocheck
import {
  Type as ReadRangeOutput,
  encodeJson as encodeJson_1,
  decodeJson as decodeJson_1,
  encodeBinary as encodeBinary_1,
  decodeBinary as decodeBinary_1,
} from "./ReadRangeOutput.ts";
import {
  Type as SnapshotReadStatus,
  name2num,
  num2name,
} from "./SnapshotReadStatus.ts";
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
  export type SnapshotReadOutput = {
    ranges: ReadRangeOutput[];
    readDisabled: boolean;
    readIsStronglyConsistent: boolean;
    status: SnapshotReadStatus;
  }
}

export type Type = $.com.deno.kv.datapath.SnapshotReadOutput;

export function getDefaultValue(): $.com.deno.kv.datapath.SnapshotReadOutput {
  return {
    ranges: [],
    readDisabled: false,
    readIsStronglyConsistent: false,
    status: "SR_UNSPECIFIED",
  };
}

export function createValue(partialValue: Partial<$.com.deno.kv.datapath.SnapshotReadOutput>): $.com.deno.kv.datapath.SnapshotReadOutput {
  return {
    ...getDefaultValue(),
    ...partialValue,
  };
}

export function encodeJson(value: $.com.deno.kv.datapath.SnapshotReadOutput): unknown {
  const result: any = {};
  result.ranges = value.ranges.map(value => encodeJson_1(value));
  if (value.readDisabled !== undefined) result.readDisabled = tsValueToJsonValueFns.bool(value.readDisabled);
  if (value.readIsStronglyConsistent !== undefined) result.readIsStronglyConsistent = tsValueToJsonValueFns.bool(value.readIsStronglyConsistent);
  if (value.status !== undefined) result.status = tsValueToJsonValueFns.enum(value.status);
  return result;
}

export function decodeJson(value: any): $.com.deno.kv.datapath.SnapshotReadOutput {
  const result = getDefaultValue();
  result.ranges = value.ranges?.map((value: any) => decodeJson_1(value)) ?? [];
  if (value.readDisabled !== undefined) result.readDisabled = jsonValueToTsValueFns.bool(value.readDisabled);
  if (value.readIsStronglyConsistent !== undefined) result.readIsStronglyConsistent = jsonValueToTsValueFns.bool(value.readIsStronglyConsistent);
  if (value.status !== undefined) result.status = jsonValueToTsValueFns.enum(value.status) as SnapshotReadStatus;
  return result;
}

export function encodeBinary(value: $.com.deno.kv.datapath.SnapshotReadOutput): Uint8Array {
  const result: WireMessage = [];
  for (const tsValue of value.ranges) {
    result.push(
      [1, { type: WireType.LengthDelimited as const, value: encodeBinary_1(tsValue) }],
    );
  }
  if (value.readDisabled !== undefined) {
    const tsValue = value.readDisabled;
    result.push(
      [2, tsValueToWireValueFns.bool(tsValue)],
    );
  }
  if (value.readIsStronglyConsistent !== undefined) {
    const tsValue = value.readIsStronglyConsistent;
    result.push(
      [4, tsValueToWireValueFns.bool(tsValue)],
    );
  }
  if (value.status !== undefined) {
    const tsValue = value.status;
    result.push(
      [8, { type: WireType.Varint as const, value: new Long(name2num[tsValue as keyof typeof name2num]) }],
    );
  }
  return serialize(result);
}

export function decodeBinary(binary: Uint8Array): $.com.deno.kv.datapath.SnapshotReadOutput {
  const result = getDefaultValue();
  const wireMessage = deserialize(binary);
  const wireFields = new Map(wireMessage);
  collection: {
    const wireValues = wireMessage.filter(([fieldNumber]) => fieldNumber === 1).map(([, wireValue]) => wireValue);
    const value = wireValues.map((wireValue) => wireValue.type === WireType.LengthDelimited ? decodeBinary_1(wireValue.value) : undefined).filter(x => x !== undefined);
    if (!value.length) break collection;
    result.ranges = value as any;
  }
  field: {
    const wireValue = wireFields.get(2);
    if (wireValue === undefined) break field;
    const value = wireValueToTsValueFns.bool(wireValue);
    if (value === undefined) break field;
    result.readDisabled = value;
  }
  field: {
    const wireValue = wireFields.get(4);
    if (wireValue === undefined) break field;
    const value = wireValueToTsValueFns.bool(wireValue);
    if (value === undefined) break field;
    result.readIsStronglyConsistent = value;
  }
  field: {
    const wireValue = wireFields.get(8);
    if (wireValue === undefined) break field;
    const value = wireValue.type === WireType.Varint ? num2name[wireValue.value[0] as keyof typeof num2name] : undefined;
    if (value === undefined) break field;
    result.status = value;
  }
  return result;
}
