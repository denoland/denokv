# denokv

This repository contains two crates:

- `denokv_proto` (`/proto`): Shared interfaces backing KV, like definitions of
  `Key`, `Database`, and `Value`.
- `denokv_sqlite` (`/sqlite`): An implementation of `Database` backed by SQLite.
- `denokv_remote` (`/remote`): An implementation of `Database` backed by a
  remote KV database, acessible via the KV Connect protocol.

These crates are used by the `deno_kv` crate in the Deno repository to provide a
JavaScript API for interacting with Deno KV.

The Deno KV Connect protocol used for communication between Deno and a remote KV
database is defined in [`/proto/kv-connect.md`](./proto/kv-connect.md).
