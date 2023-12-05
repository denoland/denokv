// @ts-nocheck
import {
  Type as WatchKey,
  encodeJson as encodeJson_1,
  decodeJson as decodeJson_1,
  encodeBinary as encodeBinary_1,
  decodeBinary as decodeBinary_1,
} from "./WatchKey.ts";
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

export declare namespace $.com.deno.kv.datapath {
  export type Watch = {
    keys: WatchKey[];
  }
}

export type Type = $.com.deno.kv.datapath.Watch;

export function getDefaultValue(): $.com.deno.kv.datapath.Watch {
  return {
    keys: [],
  };
}

export function createValue(partialValue: Partial<$.com.deno.kv.datapath.Watch>): $.com.deno.kv.datapath.Watch {
  return {
    ...getDefaultValue(),
    ...partialValue,
  };
}

export function encodeJson(value: $.com.deno.kv.datapath.Watch): unknown {
  const result: any = {};
  result.keys = value.keys.map(value => encodeJson_1(value));
  return result;
}

export function decodeJson(value: any): $.com.deno.kv.datapath.Watch {
  const result = getDefaultValue();
  result.keys = value.keys?.map((value: any) => decodeJson_1(value)) ?? [];
  return result;
}

export function encodeBinary(value: $.com.deno.kv.datapath.Watch): Uint8Array {
  const result: WireMessage = [];
  for (const tsValue of value.keys) {
    result.push(
      [1, { type: WireType.LengthDelimited as const, value: encodeBinary_1(tsValue) }],
    );
  }
  return serialize(result);
}

export function decodeBinary(binary: Uint8Array): $.com.deno.kv.datapath.Watch {
  const result = getDefaultValue();
  const wireMessage = deserialize(binary);
  const wireFields = new Map(wireMessage);
  collection: {
    const wireValues = wireMessage.filter(([fieldNumber]) => fieldNumber === 1).map(([, wireValue]) => wireValue);
    const value = wireValues.map((wireValue) => wireValue.type === WireType.LengthDelimited ? decodeBinary_1(wireValue.value) : undefined).filter(x => x !== undefined);
    if (!value.length) break collection;
    result.keys = value as any;
  }
  return result;
}
