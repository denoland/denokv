// Copyright 2023 the Deno authors. All rights reserved. MIT license.

use std::num::NonZeroU32;

use crate::datapath as pb;
use crate::AtomicWrite;
use crate::Check;
use crate::CommitResult;
use crate::Enqueue;
use crate::KvEntry;
use crate::Mutation;
use crate::MutationKind;
use crate::ReadRangeOutput;
use crate::WatchKeyOutput;

use crate::decode_value;
use crate::encode_value;
use crate::limits;
use crate::time::utc_now;
use crate::ReadRange;

pub enum ConvertError {
  KeyTooLong,
  ValueTooLong,
  ReadRangeTooLarge,
  AtomicWriteTooLarge,
  TooManyReadRanges,
  TooManyWatchedKeys,
  TooManyChecks,
  TooManyMutations,
  TooManyQueueUndeliveredKeys,
  TooManyQueueBackoffIntervals,
  QueueBackoffIntervalTooLarge,
  InvalidReadRangeLimit,
  DecodeError,
  InvalidVersionstamp,
  InvalidMutationKind,
  InvalidMutationExpireAt,
  InvalidMutationEnqueueDeadline,
}

impl TryFrom<pb::SnapshotRead> for Vec<ReadRange> {
  type Error = ConvertError;

  fn try_from(
    snapshot_read: pb::SnapshotRead,
  ) -> Result<Vec<ReadRange>, ConvertError> {
    if snapshot_read.ranges.len() > limits::MAX_READ_RANGES {
      return Err(ConvertError::TooManyReadRanges);
    }
    let mut requests = Vec::with_capacity(snapshot_read.ranges.len());
    let mut total_limit: usize = 0;
    for range in snapshot_read.ranges {
      let limit: NonZeroU32 = u32::try_from(range.limit)
        .map_err(|_| ConvertError::InvalidReadRangeLimit)?
        .try_into()
        .map_err(|_| ConvertError::InvalidReadRangeLimit)?;
      if range.start.len() > limits::MAX_READ_KEY_SIZE_BYTES {
        return Err(ConvertError::KeyTooLong);
      }
      if range.end.len() > limits::MAX_READ_KEY_SIZE_BYTES {
        return Err(ConvertError::KeyTooLong);
      }
      total_limit += limit.get() as usize;
      requests.push(ReadRange {
        start: range.start,
        end: range.end,
        reverse: range.reverse,
        limit,
      });
    }
    if total_limit > limits::MAX_READ_ENTRIES {
      return Err(ConvertError::ReadRangeTooLarge);
    }
    Ok(requests)
  }
}

impl TryFrom<pb::AtomicWrite> for AtomicWrite {
  type Error = ConvertError;

  fn try_from(
    atomic_write: pb::AtomicWrite,
  ) -> Result<AtomicWrite, ConvertError> {
    if atomic_write.checks.len() > limits::MAX_CHECKS {
      return Err(ConvertError::TooManyChecks);
    }
    if atomic_write.mutations.len() + atomic_write.enqueues.len()
      > limits::MAX_MUTATIONS
    {
      return Err(ConvertError::TooManyMutations);
    }

    let mut total_payload_size = 0;

    let mut checks = Vec::with_capacity(atomic_write.checks.len());
    for check in atomic_write.checks {
      if check.key.len() > limits::MAX_READ_KEY_SIZE_BYTES {
        return Err(ConvertError::KeyTooLong);
      }
      total_payload_size += check.key.len();
      checks.push(Check {
        key: check.key,
        versionstamp: match check.versionstamp.len() {
          0 => None,
          10 => {
            let mut versionstamp = [0; 10];
            versionstamp.copy_from_slice(&check.versionstamp);
            Some(versionstamp)
          }
          _ => return Err(ConvertError::InvalidVersionstamp),
        },
      });
    }

    let mut mutations = Vec::with_capacity(atomic_write.mutations.len());
    for mutation in atomic_write.mutations {
      if mutation.key.len() > limits::MAX_WRITE_KEY_SIZE_BYTES {
        return Err(ConvertError::KeyTooLong);
      }
      total_payload_size += mutation.key.len();
      let value_size =
        mutation.value.as_ref().map(|v| v.data.len()).unwrap_or(0);
      if value_size > limits::MAX_VALUE_SIZE_BYTES {
        return Err(ConvertError::ValueTooLong);
      }
      total_payload_size += value_size;

      let kind = match (mutation.mutation_type(), mutation.value) {
        (pb::MutationType::MSet, Some(value)) => {
          let value = decode_value(value.data, value.encoding as i64)
            .ok_or(ConvertError::DecodeError)?;
          MutationKind::Set(value)
        }
        (pb::MutationType::MDelete, _) => MutationKind::Delete,
        (pb::MutationType::MSum, Some(value)) => {
          let value = decode_value(value.data, value.encoding as i64)
            .ok_or(ConvertError::DecodeError)?;
          MutationKind::Sum {
            value,
            min_v8: mutation.sum_min,
            max_v8: mutation.sum_max,
            clamp: mutation.sum_clamp,
          }
        }
        (pb::MutationType::MMin, Some(value)) => {
          let value = decode_value(value.data, value.encoding as i64)
            .ok_or(ConvertError::DecodeError)?;
          MutationKind::Min(value)
        }
        (pb::MutationType::MMax, Some(value)) => {
          let value = decode_value(value.data, value.encoding as i64)
            .ok_or(ConvertError::DecodeError)?;
          MutationKind::Max(value)
        }
        (pb::MutationType::MSetSuffixVersionstampedKey, Some(value)) => {
          let value = decode_value(value.data, value.encoding as i64)
            .ok_or(ConvertError::DecodeError)?;
          MutationKind::SetSuffixVersionstampedKey(value)
        }
        _ => return Err(ConvertError::InvalidMutationKind),
      };
      let expire_at = match mutation.expire_at_ms {
        -1 | 0 => None,
        millis @ 1.. => Some(
          chrono::DateTime::UNIX_EPOCH
            + std::time::Duration::from_millis(
              millis
                .try_into()
                .map_err(|_| ConvertError::InvalidMutationExpireAt)?,
            ),
        ),
        _ => return Err(ConvertError::InvalidMutationExpireAt),
      };
      mutations.push(Mutation {
        key: mutation.key,
        expire_at,
        kind,
      })
    }

    let mut enqueues = Vec::with_capacity(atomic_write.enqueues.len());
    for enqueue in atomic_write.enqueues {
      if enqueue.payload.len() > limits::MAX_VALUE_SIZE_BYTES {
        return Err(ConvertError::ValueTooLong);
      }
      total_payload_size += enqueue.payload.len();
      if enqueue.keys_if_undelivered.len() > limits::MAX_QUEUE_UNDELIVERED_KEYS
      {
        return Err(ConvertError::TooManyQueueUndeliveredKeys);
      }
      for key in &enqueue.keys_if_undelivered {
        if key.len() > limits::MAX_WRITE_KEY_SIZE_BYTES {
          return Err(ConvertError::KeyTooLong);
        }
        total_payload_size += key.len();
      }
      if enqueue.backoff_schedule.len() > limits::MAX_QUEUE_BACKOFF_INTERVALS {
        return Err(ConvertError::TooManyQueueBackoffIntervals);
      }
      for interval in &enqueue.backoff_schedule {
        if *interval > limits::MAX_QUEUE_BACKOFF_MS {
          return Err(ConvertError::QueueBackoffIntervalTooLarge);
        }
        total_payload_size += 4;
      }
      let deadline = chrono::DateTime::UNIX_EPOCH
        + std::time::Duration::from_millis(
          enqueue
            .deadline_ms
            .try_into()
            .map_err(|_| ConvertError::InvalidMutationEnqueueDeadline)?,
        );
      if utc_now().signed_duration_since(deadline).num_milliseconds()
        > limits::MAX_QUEUE_DELAY_MS as i64
      {
        return Err(ConvertError::InvalidMutationEnqueueDeadline);
      }
      enqueues.push(Enqueue {
        payload: enqueue.payload,
        backoff_schedule: if enqueue.backoff_schedule.is_empty() {
          None
        } else {
          Some(enqueue.backoff_schedule)
        },
        deadline,
        keys_if_undelivered: enqueue.keys_if_undelivered,
      });
    }

    if total_payload_size > limits::MAX_TOTAL_MUTATION_SIZE_BYTES {
      return Err(ConvertError::AtomicWriteTooLarge);
    }

    Ok(AtomicWrite {
      checks,
      mutations,
      enqueues,
    })
  }
}

impl From<Vec<ReadRangeOutput>> for pb::SnapshotReadOutput {
  fn from(result_ranges: Vec<ReadRangeOutput>) -> pb::SnapshotReadOutput {
    let mut ranges = Vec::with_capacity(result_ranges.len());
    for range in result_ranges {
      let values = range.entries.into_iter().map(Into::into).collect();
      ranges.push(pb::ReadRangeOutput { values });
    }

    pb::SnapshotReadOutput {
      ranges,
      read_disabled: false,
      read_is_strongly_consistent: true,
      status: pb::SnapshotReadStatus::SrSuccess as i32,
    }
  }
}

impl From<Option<CommitResult>> for pb::AtomicWriteOutput {
  fn from(commit_result: Option<CommitResult>) -> pb::AtomicWriteOutput {
    match commit_result {
      None => pb::AtomicWriteOutput {
        status: pb::AtomicWriteStatus::AwCheckFailure as i32,
        failed_checks: vec![], // todo!
        ..Default::default()
      },
      Some(commit_result) => pb::AtomicWriteOutput {
        status: pb::AtomicWriteStatus::AwSuccess as i32,
        versionstamp: commit_result.versionstamp.to_vec(),
        ..Default::default()
      },
    }
  }
}

impl From<KvEntry> for pb::KvEntry {
  fn from(entry: KvEntry) -> Self {
    let (value, encoding) = encode_value(&entry.value);
    pb::KvEntry {
      key: entry.key,
      value: value.into_owned(),
      encoding: encoding as i32,
      versionstamp: entry.versionstamp.to_vec(),
    }
  }
}

impl TryFrom<pb::Watch> for Vec<Vec<u8>> {
  type Error = ConvertError;

  fn try_from(value: pb::Watch) -> Result<Self, Self::Error> {
    if value.keys.len() > limits::MAX_WATCHED_KEYS {
      return Err(ConvertError::TooManyWatchedKeys);
    }

    value
      .keys
      .into_iter()
      .map(|key| {
        if key.key.len() > limits::MAX_WRITE_KEY_SIZE_BYTES {
          return Err(ConvertError::KeyTooLong);
        }
        Ok(key.key)
      })
      .collect::<Result<Vec<_>, _>>()
  }
}

impl From<Vec<WatchKeyOutput>> for pb::WatchOutput {
  fn from(watch_outputs: Vec<WatchKeyOutput>) -> Self {
    let mut keys = Vec::with_capacity(watch_outputs.len());
    for key in watch_outputs {
      match key {
        WatchKeyOutput::Unchanged => {
          keys.push(pb::WatchKeyOutput {
            changed: false,
            ..Default::default()
          });
        }
        WatchKeyOutput::Changed { entry } => {
          keys.push(pb::WatchKeyOutput {
            changed: true,
            entry_if_changed: entry.map(Into::into),
          });
        }
      }
    }
    pb::WatchOutput {
      status: pb::SnapshotReadStatus::SrSuccess as i32,
      keys,
    }
  }
}
