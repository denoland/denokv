// Copyright 2023 the Deno authors. All rights reserved. MIT license.

pub const MAX_WRITE_KEY_SIZE_BYTES: usize = 2048;
pub const MAX_READ_KEY_SIZE_BYTES: usize = MAX_WRITE_KEY_SIZE_BYTES + 1;
pub const MAX_VALUE_SIZE_BYTES: usize = 65536;
pub const MAX_READ_RANGES: usize = 10;
pub const MAX_READ_ENTRIES: usize = 1000;
pub const MAX_CHECKS: usize = 10;
pub const MAX_MUTATIONS: usize = 1000;
pub const MAX_TOTAL_MUTATION_SIZE_BYTES: usize = 819200;
pub const MAX_QUEUE_DELAY_MS: u64 = 30 * 24 * 60 * 60 * 1000; // 30 days
pub const MAX_QUEUE_UNDELIVERED_KEYS: usize = 10;
pub const MAX_QUEUE_BACKOFF_INTERVALS: usize = 10;
pub const MAX_QUEUE_BACKOFF_MS: u32 = 3600000; // 1 hour
