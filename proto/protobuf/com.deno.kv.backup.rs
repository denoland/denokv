// Copyright 2018-2024 the Deno authors. All rights reserved. MIT license.

// Generated by prost-build, enable the `build_protos` feature to regenerate.

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BackupSnapshotRange {
  #[prost(message, repeated, tag = "1")]
  pub data_list: ::prost::alloc::vec::Vec<BackupKvPair>,
  #[prost(message, repeated, tag = "2")]
  pub metadata_list: ::prost::alloc::vec::Vec<BackupKvPair>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BackupKvPair {
  #[prost(bytes = "vec", tag = "1")]
  pub key: ::prost::alloc::vec::Vec<u8>,
  #[prost(bytes = "vec", tag = "2")]
  pub value: ::prost::alloc::vec::Vec<u8>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BackupMutationRange {
  #[prost(message, repeated, tag = "1")]
  pub entries: ::prost::alloc::vec::Vec<BackupReplicationLogEntry>,
  #[prost(uint64, tag = "2")]
  pub timestamp_ms: u64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BackupReplicationLogEntry {
  #[prost(bytes = "vec", tag = "1")]
  pub versionstamp: ::prost::alloc::vec::Vec<u8>,
  #[prost(enumeration = "BackupKvMutationKind", tag = "2")]
  pub kind: i32,
  #[prost(bytes = "vec", tag = "3")]
  pub key: ::prost::alloc::vec::Vec<u8>,
  #[prost(bytes = "vec", tag = "4")]
  pub value: ::prost::alloc::vec::Vec<u8>,
  #[prost(int32, tag = "5")]
  pub value_encoding: i32,
  #[prost(uint64, tag = "6")]
  pub expire_at_ms: u64,
}
#[derive(
  Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration,
)]
#[repr(i32)]
pub enum BackupKvMutationKind {
  MkUnspecified = 0,
  MkSet = 1,
  MkClear = 2,
  MkSum = 3,
  MkMax = 4,
  MkMin = 5,
}
impl BackupKvMutationKind {
  /// String value of the enum field names used in the ProtoBuf definition.
  ///
  /// The values are not transformed in any way and thus are considered stable
  /// (if the ProtoBuf definition does not change) and safe for programmatic use.
  pub fn as_str_name(&self) -> &'static str {
    match self {
      BackupKvMutationKind::MkUnspecified => "MK_UNSPECIFIED",
      BackupKvMutationKind::MkSet => "MK_SET",
      BackupKvMutationKind::MkClear => "MK_CLEAR",
      BackupKvMutationKind::MkSum => "MK_SUM",
      BackupKvMutationKind::MkMax => "MK_MAX",
      BackupKvMutationKind::MkMin => "MK_MIN",
    }
  }
  /// Creates an enum from field names used in the ProtoBuf definition.
  pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
    match value {
      "MK_UNSPECIFIED" => Some(Self::MkUnspecified),
      "MK_SET" => Some(Self::MkSet),
      "MK_CLEAR" => Some(Self::MkClear),
      "MK_SUM" => Some(Self::MkSum),
      "MK_MAX" => Some(Self::MkMax),
      "MK_MIN" => Some(Self::MkMin),
      _ => None,
    }
  }
}