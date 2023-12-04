// Copyright 2023 the Deno authors. All rights reserved. MIT license.

import test from 'ava'

import { open, close, atomicWrite, dequeueNextMessage, finishMessage, snapshotRead } from '../index'

test('native function exports', (t) => {
  t.is(typeof open, 'function');
  t.is(typeof close, 'function');
  t.is(typeof atomicWrite, 'function');
  t.is(typeof dequeueNextMessage, 'function');
  t.is(typeof finishMessage, 'function');
  t.is(typeof snapshotRead, 'function');
})

test('memory db open/close', (t) => {
  t.notThrows(() => {
    const debug = false;
    const inMemory = undefined;
    const dbId = open(':memory:', inMemory, debug);
    close(dbId, debug);
  })
})
