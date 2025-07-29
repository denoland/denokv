use std::str::Split;

use anyhow::Context;
use async_trait::async_trait;
use aws_sdk_s3::operation::get_object::GetObjectError;
use prost::Message;

use crate::backup::DatabaseBackupSource;
use crate::backup::MutationRangeEntry;
use crate::backup::MutationRangeKey;
use crate::backup::SnapshotRangeKey;
use denokv_proto::backup::BackupMutationRange;
use denokv_proto::backup::BackupSnapshotRange;

pub struct DatabaseBackupSourceS3 {
  s3_client: aws_sdk_s3::Client,
  config: DatabaseBackupSourceS3Config,
}

#[derive(Clone, Debug)]
pub struct DatabaseBackupSourceS3Config {
  pub bucket: String,
  pub prefix: String,
}

impl DatabaseBackupSourceS3 {
  pub fn new(
    s3_client: aws_sdk_s3::Client,
    config: DatabaseBackupSourceS3Config,
  ) -> Self {
    Self { s3_client, config }
  }
}

#[async_trait]
impl DatabaseBackupSource for DatabaseBackupSourceS3 {
  async fn get_differential_versionstamp(
    &self,
  ) -> anyhow::Result<Option<[u8; 10]>> {
    let builder = self
      .s3_client
      .get_object()
      .bucket(&self.config.bucket)
      .key(format!("{}differential", self.config.prefix));

    let get_object_output = builder.send().await;

    match get_object_output {
      Ok(x) => {
        let body = x.body.collect().await?.into_bytes();
        let mut versionstamp = [0u8; 10];
        hex::decode_to_slice(&body[..], &mut versionstamp)?;
        Ok(Some(versionstamp))
      }
      Err(e) => match e.into_service_error() {
        GetObjectError::NoSuchKey(_) => Ok(None),
        e => return Err(e.into()),
      },
    }
  }

  async fn list_snapshot_ranges(
    &self,
  ) -> anyhow::Result<Vec<SnapshotRangeKey>> {
    // snapshot range key is in the format `<prefix>snapshots/<hex-encoded-format-version-u16>_<hex-encoded-monoseq-u64>_<hex-encoded-seq-u64>.bin
    let mut snapshot_keys = Vec::new();
    let mut continuation_token = None;
    let list_prefix = format!("{}snapshots/", self.config.prefix);
    loop {
      let mut builder = self
        .s3_client
        .list_objects_v2()
        .bucket(&self.config.bucket)
        .prefix(&list_prefix);

      if let Some(token) = &continuation_token {
        builder = builder.continuation_token(token);
      }

      let list_objects_output = builder.send().await?;

      for object in list_objects_output.contents.unwrap_or_default() {
        let key = object
          .key
          .as_ref()
          .and_then(|x| x.strip_prefix(&list_prefix))
          .and_then(|x| x.strip_suffix(".bin"))
          .ok_or_else(|| anyhow::anyhow!("invalid key: {:?}", object.key))?;

        let mut parts = key.split('_');
        let (format_version, monoseq) =
          decode_format_version_and_monoseq(&mut parts)
            .with_context(|| format!("key decode failed: {:?}", object.key))?;

        let seq = u64::from_str_radix(
          parts.next().ok_or_else(|| {
            anyhow::anyhow!("invalid seq in key: {:?}", object.key)
          })?,
          16,
        )?;
        if parts.next().is_some() {
          anyhow::bail!("key contains trailing data: {:?}", object.key);
        }
        snapshot_keys.push(SnapshotRangeKey {
          format_version,
          monoseq,
          seq,
        });
      }

      if list_objects_output.is_truncated {
        continuation_token = list_objects_output.next_continuation_token;
      } else {
        break;
      }
    }

    Ok(snapshot_keys)
  }
  async fn list_logs(
    &self,
    start_after: Option<&MutationRangeKey>,
    limit: u64,
  ) -> anyhow::Result<Vec<MutationRangeEntry>> {
    // mutation range key is in the format `<prefix>logs/<hex-encoded-epoch-u64>_<hex-encoded-tsn-u64>_<hex-encoded-first-versionstamp>_<hex-encoded-last-versionstamp>.bin
    let mut log_entries = Vec::new();

    let list_prefix = format!("{}logs/", self.config.prefix);
    let mut builder = self
      .s3_client
      .list_objects_v2()
      .bucket(&self.config.bucket)
      .prefix(&list_prefix)
      .max_keys(
        i32::try_from(limit)
          .map_err(anyhow::Error::from)
          .with_context(|| "invalid limit")?,
      );

    if let Some(start_after) = &start_after {
      builder = builder.start_after(format!("{list_prefix}{start_after}.bin"));
    }

    let list_objects_output = builder.send().await?;

    for object in list_objects_output.contents.unwrap_or_default() {
      let key = object
        .key
        .as_ref()
        .and_then(|x| x.strip_prefix(&list_prefix))
        .and_then(|x| x.strip_suffix(".bin"))
        .ok_or_else(|| anyhow::anyhow!("invalid key: {:?}", object.key))?;

      let mut parts = key.split('_');
      let (format_version, monoseq) =
        decode_format_version_and_monoseq(&mut parts)
          .with_context(|| format!("key decode failed: {:?}", object.key))?;
      let mut first_versionstamp12 = [0u8; 12];
      let mut last_versionstamp12 = [0u8; 12];

      hex::decode_to_slice(
        parts.next().ok_or_else(|| {
          anyhow::anyhow!(
            "invalid first_versionstamp12 in key: {:?}",
            object.key
          )
        })?,
        &mut first_versionstamp12,
      )?;

      hex::decode_to_slice(
        parts.next().ok_or_else(|| {
          anyhow::anyhow!(
            "invalid last_versionstamp12 in key: {:?}",
            object.key
          )
        })?,
        &mut last_versionstamp12,
      )?;

      if parts.next().is_some() {
        anyhow::bail!("key contains trailing data: {:?}", object.key);
      }

      let last_modified_ms = object
        .last_modified()
        .as_ref()
        .and_then(|x| x.to_millis().ok())
        .and_then(|x| u64::try_from(x).ok())
        .unwrap_or_default();
      log_entries.push(MutationRangeEntry {
        key: MutationRangeKey {
          format_version,
          monoseq,
          first_versionstamp12,
          last_versionstamp12,
        },
        last_modified_ms,
      });
    }

    Ok(log_entries)
  }

  async fn fetch_snapshot(
    &self,
    key: &SnapshotRangeKey,
  ) -> anyhow::Result<BackupSnapshotRange> {
    let builder = self
      .s3_client
      .get_object()
      .bucket(&self.config.bucket)
      .key(format!("{}snapshots/{}.bin", self.config.prefix, key));

    let get_object_output = builder.send().await?;
    let snapshot_range =
      BackupSnapshotRange::decode(get_object_output.body.collect().await?)?;
    Ok(snapshot_range)
  }

  async fn fetch_log(
    &self,
    key: &MutationRangeKey,
  ) -> anyhow::Result<BackupMutationRange> {
    let builder = self
      .s3_client
      .get_object()
      .bucket(&self.config.bucket)
      .key(format!("{}logs/{}.bin", self.config.prefix, key));

    let get_object_output = builder.send().await?;
    let mutation_range =
      BackupMutationRange::decode(get_object_output.body.collect().await?)?;
    Ok(mutation_range)
  }
}

fn decode_format_version_and_monoseq(
  split: &mut Split<'_, char>,
) -> anyhow::Result<(u16, u64)> {
  let first_part = split
    .next()
    .ok_or_else(|| anyhow::anyhow!("invalid first part"))?;
  let format_version = if first_part.len() == 4 {
    u16::from_str_radix(first_part, 16)?
  } else {
    0
  };
  let monoseq = match format_version {
    0 => {
      let epoch = u64::from_str_radix(first_part, 16)?;
      let tsn = u64::from_str_radix(
        split.next().ok_or_else(|| anyhow::anyhow!("invalid tsn"))?,
        16,
      )?;
      if epoch >= u32::MAX as u64 || tsn >= u32::MAX as u64 {
        anyhow::bail!("invalid epoch or tsn");
      }
      (epoch << 32) | tsn
    }
    1 => u64::from_str_radix(
      split
        .next()
        .ok_or_else(|| anyhow::anyhow!("invalid monoseq"))?,
      16,
    )?,
    _ => anyhow::bail!("invalid format version"),
  };
  Ok((format_version, monoseq))
}
