use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

use anyhow::Context;
use anyhow::Result;
use chrono::DateTime;
use chrono::TimeZone;
use chrono::Utc;
use denokv_proto::backup::BackupKvMutationKind;
use futures::FutureExt;
use futures::StreamExt;
use futures::TryStreamExt;
use rusqlite::OptionalExtension;

use crate::backup::DatabaseBackupSource;
use crate::backup::MutationRangeEntry;
use crate::backup::MutationRangeKey;
use crate::backup::SnapshotRangeKey;
use crate::backup_source_s3::DatabaseBackupSourceS3Config;
use crate::key_metadata::KeyMetadata;

const MIGRATIONS: [&str; 1] = ["
create view migration_state as select 0 as k, 3 as version;
create table data_version (
  k integer primary key,
  version integer not null,
  seq integer not null default 0
);
insert into data_version (k, version) values (0, 0);
create table kv_snapshot (
  k blob primary key,
  v blob not null,
  v_encoding integer not null,
  version integer not null,
  seq integer not null default 0,
  expiration_ms integer not null default -1
) without rowid;
create view kv as
  select k, v, v_encoding, version, seq, expiration_ms from kv_snapshot;

-- REDO/UNDO
create table tt_redo_log (
  versionstamp12 blob not null primary key,
  timestamp_ms integer not null,

  k blob not null,
  v blob,
  v_encoding integer not null,
  real_versionstamp blob not null
) without rowid;

create table tt_undo_log (
  versionstamp12 blob not null primary key,
  timestamp_ms integer not null,

  k blob not null,
  v blob,
  v_encoding integer not null,
  real_versionstamp blob not null
) without rowid;

create table tt_redo_cursor (
  zero integer not null primary key,

  format_version integer not null,
  monoseq integer not null,
  first_versionstamp12 blob not null,
  last_versionstamp12 blob not null
);

create table tt_initial_snapshot_ranges (
  format_version integer not null,
  monoseq integer not null,
  seq integer not null,

  pulled integer not null,

  primary key (format_version, monoseq, seq)
) without rowid;

create table tt_config (
  k text not null primary key,
  v text not null
) without rowid;
"];

const TT_CONFIG_KEY_S3_INIT_COMPLETED: &str = "s3_init_completed";
const TT_CONFIG_KEY_INITIAL_SNAPSHOT_COMPLETED: &str =
  "initial_snapshot_completed";
const TT_CONFIG_KEY_CURRENT_SNAPSHOT_VERSIONSTAMP12: &str =
  "current_snapshot_versionstamp12";
const TT_CONFIG_KEY_S3_BUCKET: &str = "s3_bucket";
const TT_CONFIG_KEY_S3_PREFIX: &str = "s3_prefix";

const SNAPSHOT_FETCH_CONCURRENCY: usize = 16;
const LOG_FETCH_CONCURRENCY: usize = 32;

pub struct TimeTravelControl {
  db: rusqlite::Connection,
}

impl TimeTravelControl {
  pub fn open(mut db: rusqlite::Connection) -> Result<Self> {
    db.pragma_update(None, "journal_mode", "wal")?;

    let tx = db.transaction()?;
    tx.execute(
      "create table if not exists tt_migration_state(
  k integer not null primary key,
  version integer not null
);",
      [],
    )?;

    let current_version: usize = tx
      .query_row(
        "select version from tt_migration_state where k = 0",
        [],
        |row| row.get(0),
      )
      .optional()?
      .unwrap_or(0);

    for (i, migration) in MIGRATIONS.iter().enumerate() {
      let version = i + 1;
      if version > current_version {
        tx.execute_batch(migration)?;
        tx.execute(
          "replace into tt_migration_state (k, version) values(?, ?)",
          [&0, &version],
        )?;
      }
    }

    tx.commit()?;

    Ok(Self { db })
  }

  pub fn initial_snapshot_is_complete(&mut self) -> Result<bool> {
    Ok(
      get_config(&self.db, TT_CONFIG_KEY_INITIAL_SNAPSHOT_COMPLETED)?
        .as_deref()
        == Some("1"),
    )
  }

  pub async fn ensure_initial_snapshot_completed(
    &mut self,
    source: &impl DatabaseBackupSource,
  ) -> Result<()> {
    let dv: [u8; 12];
    let ranges: Vec<InitialSnapshotRange>;

    // Step 1: Read some initial information from the backup source:
    // - Snapshot completion status
    // - DV (differential versionstamp)
    // - Snapshot range list
    {
      let tx = self.db.transaction()?;

      if get_config(&tx, TT_CONFIG_KEY_INITIAL_SNAPSHOT_COMPLETED)?.as_deref()
        == Some("1")
      {
        return Ok(());
      }

      // Wait until the source is differential
      dv = loop {
        let Some(dv) = source.get_differential_versionstamp().await? else {
          tokio::time::sleep(Duration::from_secs(1)).await;
          continue;
        };
        let mut out = [0u8; 12];
        out[0..10].copy_from_slice(&dv[..]);
        out[10..12].copy_from_slice(&[0xff, 0xff]);
        break out;
      };

      // If we haven't yet fetched the snapshot range list, fetch it now.
      // False-positive is possible if the remote database is empty, but it's fine.
      let maybe_ranges = list_initial_snapshot_ranges(&tx)?;
      ranges = if maybe_ranges.is_empty() {
        let ranges = source.list_snapshot_ranges().await?;
        insert_initial_snapshot_ranges(&tx, &ranges)?;
        list_initial_snapshot_ranges(&tx)?
      } else {
        maybe_ranges
      };

      // Commit now to save progress
      tx.commit()?;
    }

    // Step 2: Pull snapshot ranges that are not yet pulled
    {
      let mut incoming =
        futures::stream::iter(ranges.iter().filter(|x| !x.pulled).map(|x| {
          source
            .fetch_snapshot(&x.key)
            .map(|y| y.map(|y| (&x.key, y)))
        }))
        .buffered(SNAPSHOT_FETCH_CONCURRENCY);
      while let Some(incoming) = incoming.next().await {
        let (key, range) = incoming?;
        let tx = self.db.transaction()?;

        // Check again in the transaction to make sure we don't overwrite new data
        let pulled_now = tx.query_row(
          "select pulled from tt_initial_snapshot_ranges where format_version = ? and monoseq = ? and seq = ?",
          rusqlite::params![&key.format_version, &key.monoseq, &key.seq],
          |row| Ok(row.get::<_, u64>(0)? != 0),
        )?;
        if pulled_now {
          continue;
        }

        for (data_entry, metadata_entry) in
          range.data_list.iter().zip(range.metadata_list.iter())
        {
          assert_eq!(data_entry.key[0], b'd');
          assert_eq!(metadata_entry.key[0], b'm');
          assert_eq!(data_entry.key[1..], metadata_entry.key[1..]);

          let key = &data_entry.key[1..];
          let value = &data_entry.value[..];
          let metadata = KeyMetadata::decode(&metadata_entry.value[..])
            .ok_or_else(|| {
              anyhow::anyhow!("invalid metadata for key {}", hex::encode(key))
            })?;
          tx.execute(
          "insert into kv_snapshot (k, v, v_encoding, version, seq) values (?, ?, ?, ?, ?)
          on conflict(k) do update set v = excluded.v, v_encoding = excluded.v_encoding, version = excluded.version, seq = excluded.seq",
          rusqlite::params![
            key,
            value,
            metadata.value_encoding,
            u64::from_be_bytes(<[u8; 8]>::try_from(&metadata.versionstamp[0..8]).unwrap()),
            u16::from_be_bytes(<[u8; 2]>::try_from(&metadata.versionstamp[8..10]).unwrap()),
          ],
        )?;
        }
        tx.execute(
          "update tt_initial_snapshot_ranges set pulled = 1 where format_version = ? and monoseq = ? and seq = ?",
          rusqlite::params![&key.format_version, &key.monoseq, &key.seq],
        )?;
        tx.commit()?;
      }

      let ranges = list_initial_snapshot_ranges(&self.db)?;
      assert!(ranges.iter().all(|x| x.pulled));
    }

    // Step 3: Fetch REDO logs and store them in SQLite until DV is reached and one of
    // the following conditions hold:
    // - the (real) timestamp of the current log entry is greater than the time at which log
    //   fetching starts
    // - the end of logs has been reached
    let start_ts = SystemTime::now()
      .duration_since(SystemTime::UNIX_EPOCH)
      .unwrap()
      .as_millis() as u64;
    let last_key = self
      .fetch_redo_outside_tx(source, |x| {
        x.key.first_versionstamp12 > dv && x.last_modified_ms > start_ts
      })
      .await?;

    // Step 4: Apply REDO to bring kv_snapshot to DV
    {
      let tx = self.db.transaction()?;
      if get_config(&tx, TT_CONFIG_KEY_INITIAL_SNAPSHOT_COMPLETED)?.as_deref()
        == Some("1")
      {
        return Ok(());
      }

      if last_key.is_some() {
        // If any redo logs were fetched, apply them until `dv` to bring the database to
        // a consistent state.
        //
        // Inverted logs are not written, because states before `dv` are not consistent.
        set_config(
          &tx,
          TT_CONFIG_KEY_CURRENT_SNAPSHOT_VERSIONSTAMP12,
          &hex::encode([0u8; 12]),
        )?;
        replay_redo_or_undo(&tx, dv, false)?;
      } else {
        // Otherwise, no mutations happened during the snapshot period and we are at `dv` now.
        set_config(
          &tx,
          TT_CONFIG_KEY_CURRENT_SNAPSHOT_VERSIONSTAMP12,
          &hex::encode(dv),
        )?;
      }

      set_config(&tx, TT_CONFIG_KEY_INITIAL_SNAPSHOT_COMPLETED, "1")?;
      tx.commit()?;
    }

    Ok(())
  }

  pub async fn sync(
    &mut self,
    source: &impl DatabaseBackupSource,
  ) -> Result<()> {
    if !self.initial_snapshot_is_complete()? {
      anyhow::bail!("initial snapshot not completed");
    }
    let start_ts = SystemTime::now()
      .duration_since(SystemTime::UNIX_EPOCH)
      .unwrap()
      .as_millis() as u64;
    self
      .fetch_redo_outside_tx(source, |x| x.last_modified_ms > start_ts)
      .await?;
    Ok(())
  }

  pub fn get_redo_cursor(&self) -> Result<Option<MutationRangeKey>> {
    query_redo_cursor(&self.db)
  }

  pub fn get_s3_config(
    &mut self,
  ) -> Result<Option<DatabaseBackupSourceS3Config>> {
    let tx = self.db.transaction()?;
    let s3_bucket = get_config(&tx, TT_CONFIG_KEY_S3_BUCKET)?;
    let s3_prefix = get_config(&tx, TT_CONFIG_KEY_S3_PREFIX)?;

    let (Some(s3_bucket), Some(s3_prefix)) = (s3_bucket, s3_prefix) else {
      return Ok(None);
    };

    Ok(Some(DatabaseBackupSourceS3Config {
      bucket: s3_bucket,
      prefix: s3_prefix,
    }))
  }

  pub fn init_s3(
    &mut self,
    s3_config: &DatabaseBackupSourceS3Config,
  ) -> Result<()> {
    let tx = self.db.transaction()?;
    if get_config(&tx, TT_CONFIG_KEY_S3_INIT_COMPLETED)?.as_deref() == Some("1")
    {
      // Validate consistency
      let current_bucket =
        get_config(&tx, TT_CONFIG_KEY_S3_BUCKET)?.unwrap_or_default();
      let current_prefix =
        get_config(&tx, TT_CONFIG_KEY_S3_PREFIX)?.unwrap_or_default();

      if current_bucket != s3_config.bucket
        || current_prefix != s3_config.prefix
      {
        anyhow::bail!(
          "Database is previously synced with a different S3 bucket or prefix. If you have moved the backup directory, please use the SQLite CLI to manually delete `s3_init_completed` in table `tt_config`."
        );
      }

      return Ok(());
    }
    set_config(&tx, TT_CONFIG_KEY_S3_BUCKET, &s3_config.bucket)?;
    set_config(&tx, TT_CONFIG_KEY_S3_PREFIX, &s3_config.prefix)?;
    set_config(&tx, TT_CONFIG_KEY_S3_INIT_COMPLETED, "1")?;
    tx.commit()?;
    Ok(())
  }

  pub fn checkout(&mut self, target_versionstamp: [u8; 10]) -> Result<()> {
    let tx = self.db.transaction()?;
    if get_config(&tx, TT_CONFIG_KEY_INITIAL_SNAPSHOT_COMPLETED)?.as_deref()
      != Some("1")
    {
      anyhow::bail!("initial snapshot not completed");
    }
    let mut target_versionstamp12: [u8; 12] = [0u8; 12];
    target_versionstamp12[0..10].copy_from_slice(&target_versionstamp[..]);
    target_versionstamp12[10..12].copy_from_slice(&[0xff, 0xff]);
    replay_redo_or_undo(&tx, target_versionstamp12, true)?;
    tx.commit()?;
    Ok(())
  }

  pub fn get_current_versionstamp(&mut self) -> Result<[u8; 10]> {
    let tx = self.db.transaction()?;
    let current_versionstamp12 = hex::decode(
      get_config(&tx, TT_CONFIG_KEY_CURRENT_SNAPSHOT_VERSIONSTAMP12)?
        .ok_or_else(|| {
          anyhow::anyhow!("current_snapshot_versionstamp12 not found")
        })?,
    )
    .ok()
    .and_then(|x| <[u8; 12]>::try_from(x).ok())
    .with_context(|| "invalid current_snapshot_versionstamp12")?;
    assert_eq!(current_versionstamp12[10..12], [0xff, 0xff]);
    let mut out = [0u8; 10];
    out.copy_from_slice(&current_versionstamp12[0..10]);
    Ok(out)
  }

  pub fn lookup_versionstamps_around_timestamp(
    &mut self,
    start: DateTime<Utc>,
    end: Option<DateTime<Utc>>,
  ) -> Result<Vec<([u8; 10], DateTime<Utc>)>> {
    let tx = self.db.transaction()?;
    let mut stmt = tx.prepare_cached(
      "select versionstamp12, timestamp_ms
      from (select versionstamp12, timestamp_ms from tt_redo_log
            union all select versionstamp12, timestamp_ms from tt_undo_log)
      where timestamp_ms >= ? and timestamp_ms < ? and substr(hex(versionstamp12), 21, 4) = 'FFFF'
      order by versionstamp12 desc",
    )?;
    let mut out = Vec::new();
    for row in stmt.query_map(
      rusqlite::params![
        start.timestamp_millis(),
        end.map(|x| x.timestamp_millis()).unwrap_or(std::i64::MAX)
      ],
      |row| {
        let mut versionstamp = [0u8; 10];
        versionstamp.copy_from_slice(&row.get::<_, Vec<u8>>(0)?[0..10]);

        let timestamp = row.get::<_, i64>(1)?;
        let chrono_timestamp = Utc
          .timestamp_millis_opt(timestamp)
          .single()
          .ok_or_else(|| {
            anyhow::anyhow!(
              "invalid timestamp {} for versionstamp {}",
              timestamp,
              hex::encode(versionstamp)
            )
          })
          .unwrap_or_default();
        Ok((versionstamp, chrono_timestamp))
      },
    )? {
      out.push(row?);
    }

    Ok(out)
  }

  async fn fetch_redo_outside_tx(
    &mut self,
    source: &impl DatabaseBackupSource,
    mut should_stop_at: impl FnMut(&MutationRangeEntry) -> bool + Copy,
  ) -> Result<Option<MutationRangeKey>> {
    let cursor = query_redo_cursor(&self.db)?;

    let stopped = Arc::new(AtomicBool::new(false));

    let log_range_stream = futures::stream::try_unfold(cursor, |start_after| {
      let stopped = stopped.clone();
      async move {
        if stopped.load(Ordering::Relaxed) {
          return Ok::<_, anyhow::Error>(None);
        }

        let mut range = source.list_logs(start_after.as_ref(), 1000).await?;

        if let Some((i, _)) =
          range.iter().enumerate().find(|(_, x)| should_stop_at(x))
        {
          range.truncate(i);
          stopped.store(true, Ordering::Relaxed);
        }

        if range.is_empty() {
          return Ok::<_, anyhow::Error>(None);
        }

        let next_start_after = range.last().unwrap().clone();

        Ok(Some((
          futures::stream::iter(range).map(Ok::<_, anyhow::Error>),
          Some(next_start_after.key),
        )))
      }
    })
    .try_flatten()
    .map_ok(|entry| async move {
      source.fetch_log(&entry.key).await.map(|x| (entry, x))
    })
    .try_buffered(LOG_FETCH_CONCURRENCY);

    let mut log_range_stream = std::pin::pin!(log_range_stream.chunks(64));

    // Write REDO
    let mut last_key: Option<MutationRangeKey> = None;
    while let Some(x) = log_range_stream.next().await {
      let tx = self.db.transaction()?;
      last_key = None;
      {
        let mut write_redo_stmt = tx.prepare_cached("insert or ignore into tt_redo_log (versionstamp12, timestamp_ms, k, v, v_encoding, real_versionstamp) values(?, ?, ?, ?, ?, ?)")?;
        for entry in x {
          let (range_entry, range) = entry?;
          for entry in range.entries {
            let value = if entry.kind() == BackupKvMutationKind::MkClear {
              None
            } else {
              Some(entry.value)
            };
            write_redo_stmt.execute(rusqlite::params![
              &entry.versionstamp,
              range.timestamp_ms,
              &entry.key,
              &value,
              entry.value_encoding,
              &entry.versionstamp[0..10],
            ])?;
          }
          last_key = Some(range_entry.key);
        }
      }
      let last_key = last_key.as_ref().unwrap();
      update_redo_cursor(&tx, last_key)?;
      tx.commit()?;
    }

    Ok(last_key)
  }
}

fn query_redo_cursor(
  conn: &rusqlite::Connection,
) -> Result<Option<MutationRangeKey>> {
  conn.query_row(
      "select format_version, monoseq, first_versionstamp12, last_versionstamp12 from tt_redo_cursor where zero = 0",
      [],
      |row| Ok(MutationRangeKey{
        format_version: row.get(0)?,
        monoseq: row.get(1)?,
        first_versionstamp12: row.get(2)?,
        last_versionstamp12: row.get(3)?,
      })).optional().map_err(anyhow::Error::from)
}

fn update_redo_cursor(
  conn: &rusqlite::Connection,
  key: &MutationRangeKey,
) -> Result<()> {
  conn.execute(
    "replace into tt_redo_cursor (zero, format_version, monoseq, first_versionstamp12, last_versionstamp12) values (0, ?, ?, ?, ?)",
    rusqlite::params![&key.format_version, &key.monoseq, &key.first_versionstamp12, &key.last_versionstamp12],
  )?;
  Ok(())
}

fn get_config(
  conn: &rusqlite::Connection,
  key: &str,
) -> Result<Option<String>> {
  conn
    .query_row("select v from tt_config where k = ?", [key], |row| {
      row.get(0)
    })
    .optional()
    .map_err(anyhow::Error::from)
}

fn set_config(
  conn: &rusqlite::Connection,
  key: &str,
  value: &str,
) -> Result<()> {
  conn.execute("replace into tt_config (k, v) values (?, ?)", [key, value])?;
  Ok(())
}

struct InitialSnapshotRange {
  key: SnapshotRangeKey,
  pulled: bool,
}

fn list_initial_snapshot_ranges(
  conn: &rusqlite::Connection,
) -> Result<Vec<InitialSnapshotRange>> {
  conn
    .prepare_cached(
      "select format_version, monoseq, seq, pulled from tt_initial_snapshot_ranges order by format_version, monoseq, seq",
    )?
    .query_map([], |row| {
      Ok(InitialSnapshotRange {
        key: SnapshotRangeKey {
          format_version: row.get(0)?,
          monoseq: row.get(1)?,
          seq: row.get(2)?,
        },
        pulled: row.get::<_, u64>(3)? != 0,
      })
    })?
    .collect::<Result<Vec<_>, rusqlite::Error>>()
    .map_err(anyhow::Error::from)
}

fn insert_initial_snapshot_ranges(
  conn: &rusqlite::Connection,
  ranges: &[SnapshotRangeKey],
) -> Result<()> {
  for range in ranges {
    conn.execute(
      "insert into tt_initial_snapshot_ranges (format_version, monoseq, seq, pulled) values (?, ?, ?, ?)",
      rusqlite::params![&range.format_version, &range.monoseq, &range.seq, &0u64],
    )?;
  }
  Ok(())
}

fn replay_redo_or_undo(
  tx: &rusqlite::Transaction,
  target_versionstamp12: [u8; 12],
  write_inverted: bool,
) -> Result<()> {
  let current_versionstamp12 = hex::decode(
    get_config(tx, TT_CONFIG_KEY_CURRENT_SNAPSHOT_VERSIONSTAMP12)?.ok_or_else(
      || anyhow::anyhow!("current_snapshot_versionstamp12 not found"),
    )?,
  )
  .ok()
  .and_then(|x| <[u8; 12]>::try_from(x).ok())
  .with_context(|| "invalid current_snapshot_versionstamp12")?;
  if target_versionstamp12 == current_versionstamp12 {
    return Ok(());
  }
  if target_versionstamp12[10..12] != [0xff, 0xff] {
    anyhow::bail!("target_versionstamp12 is not at a snapshot boundary");
  }

  let is_redo = target_versionstamp12 > current_versionstamp12;
  let source_table = if is_redo {
    "tt_redo_log"
  } else {
    "tt_undo_log"
  };
  let inverted_table = if is_redo {
    "tt_undo_log"
  } else {
    "tt_redo_log"
  };
  let source_table_select_suffix = if is_redo {
    "versionstamp12 > ? order by versionstamp12 asc"
  } else {
    "versionstamp12 <= ? order by versionstamp12 desc"
  };
  let mut stmt_list = tx.prepare_cached(&format!(
    "select versionstamp12, timestamp_ms, k, v, v_encoding, real_versionstamp from {} where {}",
    source_table, source_table_select_suffix
  ))?;
  let mut stmt_delete = tx.prepare_cached(&format!(
    "delete from {} where versionstamp12 = ?",
    source_table
  ))?;
  let mut it = stmt_list.query(rusqlite::params![current_versionstamp12])?;
  let mut last_versionstamp12: Option<[u8; 12]> = None;
  while let Some(row) = it.next()? {
    let versionstamp12: [u8; 12] = row.get(0)?;
    let timestamp_ms: u64 = row.get(1)?;
    let k: Vec<u8> = row.get(2)?;
    let v: Option<Vec<u8>> = row.get(3)?;
    let v_encoding: i32 = row.get(4)?;
    let real_versionstamp: [u8; 10] = row.get(5)?;

    if !is_redo {
      last_versionstamp12 = Some(versionstamp12);
      if versionstamp12[10..12] == [0xff, 0xff]
        && versionstamp12 <= target_versionstamp12
      {
        break;
      }
    }

    if write_inverted {
      invert_op(tx, inverted_table, versionstamp12, timestamp_ms, &k)?;
    }

    apply_log_entry(tx, real_versionstamp, &k, v.as_deref(), v_encoding)?;
    stmt_delete.execute(rusqlite::params![versionstamp12])?;

    if is_redo {
      last_versionstamp12 = Some(versionstamp12);
      if versionstamp12[10..12] == [0xff, 0xff]
        && versionstamp12 >= target_versionstamp12
      {
        break;
      }
    }
  }
  if let Some(last_versionstamp12) = last_versionstamp12 {
    if last_versionstamp12[10..12] != [0xff, 0xff] {
      anyhow::bail!("redo/undo log did not end at a snapshot boundary");
    }
    set_config(
      tx,
      TT_CONFIG_KEY_CURRENT_SNAPSHOT_VERSIONSTAMP12,
      &hex::encode(last_versionstamp12),
    )?;
  };
  Ok(())
}

fn apply_log_entry(
  tx: &rusqlite::Transaction,
  real_versionstamp: [u8; 10],
  k: &[u8],
  v: Option<&[u8]>,
  v_encoding: i32,
) -> Result<()> {
  if let Some(v) = v {
    tx.prepare_cached("insert into kv_snapshot (k, v, v_encoding, version, seq) values (?, ?, ?, ?, ?) on conflict(k) do update set v = excluded.v, v_encoding = excluded.v_encoding, version = excluded.version, seq = excluded.seq")?.execute(rusqlite::params![
      k,
      v,
      v_encoding,
      u64::from_be_bytes(<[u8; 8]>::try_from(&real_versionstamp[0..8]).unwrap()),
      u16::from_be_bytes(<[u8; 2]>::try_from(&real_versionstamp[8..10]).unwrap()),
    ])?;
  } else {
    tx.execute("delete from kv_snapshot where k = ?", [k])?;
  }
  Ok(())
}

fn invert_op(
  tx: &rusqlite::Transaction,
  target_table: &str,
  versionstamp12: [u8; 12],
  timestamp_ms: u64,
  k: &[u8],
) -> Result<()> {
  let old_value = tx
    .query_row(
      "select v, v_encoding, version, seq from kv_snapshot where k = ?",
      [k],
      |row| {
        Ok((
          row.get::<_, Vec<u8>>(0)?,
          row.get::<_, i32>(1)?,
          row.get::<_, u64>(2)?,
          row.get::<_, u16>(3)?,
        ))
      },
    )
    .optional()?;
  let mut real_versionstamp: [u8; 10] = [0u8; 10];
  if let Some((_, _, version, seq)) = &old_value {
    real_versionstamp[0..8].copy_from_slice(&version.to_be_bytes());
    real_versionstamp[8..10].copy_from_slice(&seq.to_be_bytes());
  }
  tx.execute(
    &format!(
      "insert or ignore into {} (versionstamp12, timestamp_ms, k, v, v_encoding, real_versionstamp) values(?, ?, ?, ?, ?, ?)",
      target_table
    ),
    rusqlite::params![
      versionstamp12,
      timestamp_ms,
      k,
      old_value.as_ref().map(|x| &x.0),
      old_value.as_ref().map(|x| x.1).unwrap_or_default(),
      real_versionstamp,
    ],
  )?;
  Ok(())
}
