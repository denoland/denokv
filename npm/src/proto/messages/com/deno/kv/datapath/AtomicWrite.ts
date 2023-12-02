// @ts-nocheck
import {
  Type as Check,
  encodeJson as encodeJson_1,
  decodeJson as decodeJson_1,
  encodeBinary as encodeBinary_1,
  decodeBinary as decodeBinary_1,
} from "./Check.ts";
import {
  Type as Mutation,
  encodeJson as encodeJson_2,
  decodeJson as decodeJson_2,
  encodeBinary as encodeBinary_2,
  decodeBinary as decodeBinary_2,
} from "./Mutation.ts";
import {
  Type as Enqueue,
  encodeJson as encodeJson_3,
  decodeJson as decodeJson_3,
  encodeBinary as encodeBinary_3,
  decodeBinary as decodeBinary_3,
} from "./Enqueue.ts";
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
  export type AtomicWrite = {
    checks: Check[];
    mutations: Mutation[];
    enqueues: Enqueue[];
  }
}

export type Type = $.com.deno.kv.datapath.AtomicWrite;

export function getDefaultValue(): $.com.deno.kv.datapath.AtomicWrite {
  return {
    checks: [],
    mutations: [],
    enqueues: [],
  };
}

export function createValue(partialValue: Partial<$.com.deno.kv.datapath.AtomicWrite>): $.com.deno.kv.datapath.AtomicWrite {
  return {
    ...getDefaultValue(),
    ...partialValue,
  };
}

export function encodeJson(value: $.com.deno.kv.datapath.AtomicWrite): unknown {
  const result: any = {};
  result.checks = value.checks.map(value => encodeJson_1(value));
  result.mutations = value.mutations.map(value => encodeJson_2(value));
  result.enqueues = value.enqueues.map(value => encodeJson_3(value));
  return result;
}

export function decodeJson(value: any): $.com.deno.kv.datapath.AtomicWrite {
  const result = getDefaultValue();
  result.checks = value.checks?.map((value: any) => decodeJson_1(value)) ?? [];
  result.mutations = value.mutations?.map((value: any) => decodeJson_2(value)) ?? [];
  result.enqueues = value.enqueues?.map((value: any) => decodeJson_3(value)) ?? [];
  return result;
}

export function encodeBinary(value: $.com.deno.kv.datapath.AtomicWrite): Uint8Array {
  const result: WireMessage = [];
  for (const tsValue of value.checks) {
    result.push(
      [1, { type: WireType.LengthDelimited as const, value: encodeBinary_1(tsValue) }],
    );
  }
  for (const tsValue of value.mutations) {
    result.push(
      [2, { type: WireType.LengthDelimited as const, value: encodeBinary_2(tsValue) }],
    );
  }
  for (const tsValue of value.enqueues) {
    result.push(
      [3, { type: WireType.LengthDelimited as const, value: encodeBinary_3(tsValue) }],
    );
  }
  return serialize(result);
}

export function decodeBinary(binary: Uint8Array): $.com.deno.kv.datapath.AtomicWrite {
  const result = getDefaultValue();
  const wireMessage = deserialize(binary);
  const wireFields = new Map(wireMessage);
  collection: {
    const wireValues = wireMessage.filter(([fieldNumber]) => fieldNumber === 1).map(([, wireValue]) => wireValue);
    const value = wireValues.map((wireValue) => wireValue.type === WireType.LengthDelimited ? decodeBinary_1(wireValue.value) : undefined).filter(x => x !== undefined);
    if (!value.length) break collection;
    result.checks = value as any;
  }
  collection: {
    const wireValues = wireMessage.filter(([fieldNumber]) => fieldNumber === 2).map(([, wireValue]) => wireValue);
    const value = wireValues.map((wireValue) => wireValue.type === WireType.LengthDelimited ? decodeBinary_2(wireValue.value) : undefined).filter(x => x !== undefined);
    if (!value.length) break collection;
    result.mutations = value as any;
  }
  collection: {
    const wireValues = wireMessage.filter(([fieldNumber]) => fieldNumber === 3).map(([, wireValue]) => wireValue);
    const value = wireValues.map((wireValue) => wireValue.type === WireType.LengthDelimited ? decodeBinary_3(wireValue.value) : undefined).filter(x => x !== undefined);
    if (!value.length) break collection;
    result.enqueues = value as any;
  }
  return result;
}
