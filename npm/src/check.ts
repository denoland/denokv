// Copyright 2023 the Deno authors. All rights reserved. MIT license.

import { KvKey } from "./kv_types.ts";

export function isRecord(obj: unknown): obj is Record<string, unknown> {
  return typeof obj === "object" && obj !== null && !Array.isArray(obj) &&
    obj.constructor === Object;
}

export function isDateTime(value: string): boolean {
  return typeof value === "string" &&
    /^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(\.\d+)?Z$/.test(value);
}

export function checkKeyNotEmpty(key: KvKey): void {
  if (key.length === 0) throw new TypeError(`Key cannot be empty`);
}

export function checkExpireIn(expireIn: number | undefined): void {
  const valid = expireIn === undefined ||
    typeof expireIn === "number" && expireIn > 0 &&
      Number.isSafeInteger(expireIn);
  if (!valid) {
    throw new TypeError(
      `Bad 'expireIn', expected optional positive integer, found ${expireIn}`,
    );
  }
}

export function checkMatches(
  name: string,
  value: string,
  pattern: RegExp,
): RegExpMatchArray {
  const m = pattern.exec(value);
  if (!m) throw new TypeError(`Bad '${name}': ${value}`);
  return m;
}

export function checkString(
  name: string,
  value: unknown,
): asserts value is string {
  if (typeof value !== "string") {
    throw new TypeError(`Bad '${name}': expected string, found ${value}`);
  }
}

export function checkOptionalString(
  name: string,
  value: unknown,
): asserts value is string | undefined {
  if (!(value === undefined || typeof value === "string")) {
    throw new TypeError(
      `Bad '${name}': expected optional string, found ${value}`,
    );
  }
}

export function checkRecord(
  name: string,
  value: unknown,
): asserts value is Record<string, unknown> {
  if (!isRecord(value)) {
    throw new TypeError(
      `Bad '${name}': expected simple object, found ${value}`,
    );
  }
}

export function checkOptionalBoolean(
  name: string,
  value: unknown,
): asserts value is boolean | undefined {
  if (!(value === undefined || typeof value === "boolean")) {
    throw new TypeError(
      `Bad '${name}': expected optional boolean, found ${value}`,
    );
  }
}

export function checkOptionalNumber(
  name: string,
  value: unknown,
): asserts value is number | undefined {
  if (!(value === undefined || typeof value === "number")) {
    throw new TypeError(
      `Bad '${name}': expected optional number, found ${value}`,
    );
  }
}

// deno-lint-ignore ban-types
export function checkOptionalFunction(
  name: string,
  value: unknown,
): asserts value is Function | undefined {
  if (!(value === undefined || typeof value === "function")) {
    throw new TypeError(
      `Bad '${name}': expected optional function, found ${value}`,
    );
  }
}

export function checkOptionalObject(
  name: string,
  value: unknown,
): asserts value is object {
  if (!(value === undefined || typeof value === "object")) {
    throw new TypeError(
      `Bad '${name}': expected optional object, found ${value}`,
    );
  }
}

export function check(name: string, value: unknown, valid: boolean) {
  if (!valid) throw new TypeError(`Bad '${name}': ${value}`);
}
