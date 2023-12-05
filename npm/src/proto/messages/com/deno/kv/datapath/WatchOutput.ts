// @ts-nocheck
import {
  Type as SnapshotReadStatus,
  name2num,
  num2name,
} from "./SnapshotReadStatus.ts";
import {
  Type as WatchKeyOutput,
  encodeJson as encodeJson_1,
  decodeJson as decodeJson_1,
  encodeBinary as encodeBinary_1,
  decodeBinary as decodeBinary_1,
} from "./WatchKeyOutput.ts";
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
  default as deserialize,
} from "../../../../../runtime/wire/deserialize.ts";

export declare namespace $.com.deno.kv.datapath {
  export type WatchOutput = {
    status: SnapshotReadStatus;
    keys: WatchKeyOutput[];
  }
}

export type Type = $.com.deno.kv.datapath.WatchOutput;

export function getDefaultValue(): $.com.deno.kv.datapath.WatchOutput {
  return {
    status: "SR_UNSPECIFIED",
    keys: [],
  };
}

export function createValue(partialValue: Partial<$.com.deno.kv.datapath.WatchOutput>): $.com.deno.kv.datapath.WatchOutput {
  return {
    ...getDefaultValue(),
    ...partialValue,
  };
}

export function encodeJson(value: $.com.deno.kv.datapath.WatchOutput): unknown {
  const result: any = {};
  if (value.status !== undefined) result.status = tsValueToJsonValueFns.enum(value.status);
  result.keys = value.keys.map(value => encodeJson_1(value));
  return result;
}

export function decodeJson(value: any): $.com.deno.kv.datapath.WatchOutput {
  const result = getDefaultValue();
  if (value.status !== undefined) result.status = jsonValueToTsValueFns.enum(value.status) as SnapshotReadStatus;
  result.keys = value.keys?.map((value: any) => decodeJson_1(value)) ?? [];
  return result;
}

export function encodeBinary(value: $.com.deno.kv.datapath.WatchOutput): Uint8Array {
  const result: WireMessage = [];
  if (value.status !== undefined) {
    const tsValue = value.status;
    result.push(
      [1, { type: WireType.Varint as const, value: new Long(name2num[tsValue as keyof typeof name2num]) }],
    );
  }
  for (const tsValue of value.keys) {
    result.push(
      [2, { type: WireType.LengthDelimited as const, value: encodeBinary_1(tsValue) }],
    );
  }
  return serialize(result);
}

export function decodeBinary(binary: Uint8Array): $.com.deno.kv.datapath.WatchOutput {
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
  collection: {
    const wireValues = wireMessage.filter(([fieldNumber]) => fieldNumber === 2).map(([, wireValue]) => wireValue);
    const value = wireValues.map((wireValue) => wireValue.type === WireType.LengthDelimited ? decodeBinary_1(wireValue.value) : undefined).filter(x => x !== undefined);
    if (!value.length) break collection;
    result.keys = value as any;
  }
  return result;
}
