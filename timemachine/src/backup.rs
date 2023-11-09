use std::fmt::Display;

use async_trait::async_trait;

use denokv_proto::backup::BackupMutationRange;
use denokv_proto::backup::BackupSnapshotRange;

#[async_trait]
pub trait DatabaseBackupSource {
  async fn get_differential_versionstamp(
    &self,
  ) -> anyhow::Result<Option<[u8; 10]>>;
  async fn list_snapshot_ranges(&self)
    -> anyhow::Result<Vec<SnapshotRangeKey>>;
  async fn list_logs(
    &self,
    start_after: Option<&MutationRangeKey>,
    limit: u64,
  ) -> anyhow::Result<Vec<MutationRangeEntry>>;
  async fn fetch_snapshot(
    &self,
    key: &SnapshotRangeKey,
  ) -> anyhow::Result<BackupSnapshotRange>;
  async fn fetch_log(
    &self,
    key: &MutationRangeKey,
  ) -> anyhow::Result<BackupMutationRange>;
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct SnapshotRangeKey {
  pub format_version: u16,
  pub monoseq: u64,
  pub seq: u64,
}

impl Display for SnapshotRangeKey {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    if self.format_version == 0 {
      let epoch = self.monoseq >> 32;
      let tsn = self.monoseq & 0xffff_ffff;
      write!(f, "{:016x}_{:016x}_{:016x}", epoch, tsn, self.seq)
    } else {
      write!(
        f,
        "{:04x}_{:016x}_{:016x}",
        self.format_version, self.monoseq, self.seq
      )
    }
  }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct MutationRangeKey {
  pub format_version: u16,
  pub monoseq: u64,
  pub first_versionstamp12: [u8; 12],
  pub last_versionstamp12: [u8; 12],
}

impl Display for MutationRangeKey {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    if self.format_version == 0 {
      let epoch = self.monoseq >> 32;
      let tsn = self.monoseq & 0xffff_ffff;
      write!(
        f,
        "{:016x}_{:016x}_{}_{}",
        epoch,
        tsn,
        hex::encode(self.first_versionstamp12),
        hex::encode(self.last_versionstamp12)
      )
    } else {
      write!(
        f,
        "{:04x}_{:016x}_{}_{}",
        self.format_version,
        self.monoseq,
        hex::encode(self.first_versionstamp12),
        hex::encode(self.last_versionstamp12)
      )
    }
  }
}

#[derive(Clone, Debug)]
pub struct MutationRangeEntry {
  pub key: MutationRangeKey,
  pub last_modified_ms: u64,
}
