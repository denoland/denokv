# denokv

This repository contains two crates:

- `denokv_proto` (`/proto`): Shared interfaces backing KV, like definitions of
  `Key`, `Database`, and `Value`.
- `denokv_sqlite` (`/sqlite`): An implementation of `Database` backed by SQLite.

These crates are used by the `deno_kv` crate in the Deno repository to provide a
JavaScript API for interacting with Deno KV.
