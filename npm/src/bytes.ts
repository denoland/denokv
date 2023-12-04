// Copyright 2023 the Deno authors. All rights reserved. MIT license.

export {
  decodeHex,
  encodeHex,
} from "https://deno.land/std@0.208.0/encoding/hex.ts";
import { concat } from "https://deno.land/std@0.208.0/bytes/concat.ts";

export function checkEnd(bytes: Uint8Array, pos: number) {
  const extra = bytes.length - pos;
  if (extra > 0) throw new Error(`Unexpected trailing bytes: ${extra}`);
}

export function equalBytes(lhs: Uint8Array, rhs: Uint8Array): boolean {
  if (lhs.length !== rhs.length) return false;
  for (let i = 0; i < lhs.length; i++) {
    if (lhs[i] !== rhs[i]) return false;
  }
  return true;
}

export function flipBytes(arr: Uint8Array | number[], start = 0): void {
  for (let i = start; i < arr.length; i++) {
    arr[i] = 0xff - arr[i];
  }
}

export function computeBigintMinimumNumberOfBytes(val: bigint): number {
  let n = 0;
  while (val !== 0n) {
    val >>= 8n;
    n++;
  }
  return n;
}

export function compareBytes(lhs: Uint8Array, rhs: Uint8Array): number {
  if (lhs === rhs) return 0;

  let x = lhs.length, y = rhs.length;
  const len = Math.min(x, y);

  for (let i = 0; i < len; i++) {
    if (lhs[i] !== rhs[i]) {
      x = lhs[i];
      y = rhs[i];
      break;
    }
  }

  return x < y ? -1 : y < x ? 1 : 0;
}

//

const ZERO_BYTES = new Uint8Array(0);

export class ByteReader {
  private readonly reader: ReadableStreamDefaultReader<Uint8Array>;

  private remaining = ZERO_BYTES;
  private done = false;

  constructor(reader: ReadableStreamDefaultReader<Uint8Array>) {
    this.reader = reader;
  }

  async read(
    bytes: number,
  ): Promise<
    { done: true; value: undefined } | { done: false; value: Uint8Array }
  > {
    if (bytes < 0) throw new Error(`Invalid bytes: ${bytes}`);
    if (bytes === 0) return { done: false, value: ZERO_BYTES };
    while (this.remaining.length < bytes && !this.done) {
      const { done, value } = await this.reader.read();
      if (done) {
        this.done = true;
      } else {
        this.remaining = concat([this.remaining, value]);
      }
    }
    const { remaining } = this;
    if (remaining.length < bytes && this.done) {
      return { done: true, value: undefined };
    }
    const value = remaining.slice(0, bytes);
    this.remaining = remaining.slice(bytes);
    return { done: false, value };
  }
}
