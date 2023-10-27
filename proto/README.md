# denokv_proto

The `denokv_proto` crate provides foundational types and structures related to
Deno KV protocol.

It contains the protobuf definitions for the KV Connect protocol, as well as the
[KV Connect specification](./kv-connect.md). It also contains a `Database` trait
that can be implemented to provide a Deno KV compatible database.
