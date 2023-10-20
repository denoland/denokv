use std::borrow::Cow;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use deno_kv_proto::AtomicWrite;
use deno_kv_proto::CommitResult;
use deno_kv_proto::KvEntry;
use deno_kv_proto::MutationKind;
use deno_kv_proto::ReadRange;
use deno_kv_proto::ReadRangeOutput;
use deno_kv_proto::SnapshotReadOptions;
use deno_kv_proto::Value;
use rand::Rng;
use rusqlite::params;
use rusqlite::OptionalExtension;
use rusqlite::Transaction;
use uuid::Uuid;

const STATEMENT_INC_AND_GET_DATA_VERSION: &str =
  "update data_version set version = version + 1 where k = 0 returning version";
const STATEMENT_KV_RANGE_SCAN: &str =
  "select k, v, v_encoding, version from kv where k >= ? and k < ? order by k asc limit ?";
const STATEMENT_KV_RANGE_SCAN_REVERSE: &str =
  "select k, v, v_encoding, version from kv where k >= ? and k < ? order by k desc limit ?";
const STATEMENT_KV_POINT_GET_VALUE_ONLY: &str =
  "select v, v_encoding from kv where k = ?";
const STATEMENT_KV_POINT_GET_VERSION_ONLY: &str =
  "select version from kv where k = ?";
const STATEMENT_KV_POINT_SET: &str =
  "insert into kv (k, v, v_encoding, version, expiration_ms) values (:k, :v, :v_encoding, :version, :expiration_ms) on conflict(k) do update set v = :v, v_encoding = :v_encoding, version = :version, expiration_ms = :expiration_ms";
const STATEMENT_KV_POINT_DELETE: &str = "delete from kv where k = ?";

const STATEMENT_DELETE_ALL_EXPIRED: &str =
  "delete from kv where expiration_ms >= 0 and expiration_ms <= ?";
const STATEMENT_EARLIEST_EXPIRATION: &str =
  "select expiration_ms from kv where expiration_ms >= 0 order by expiration_ms limit 1";

const STATEMENT_QUEUE_ADD_READY: &str = "insert into queue (ts, id, data, backoff_schedule, keys_if_undelivered) values(?, ?, ?, ?, ?)";
const STATEMENT_QUEUE_GET_NEXT_READY: &str = "select ts, id, data, backoff_schedule, keys_if_undelivered from queue where ts <= ? order by ts limit 1";
const STATEMENT_QUEUE_GET_EARLIEST_READY: &str =
  "select ts from queue order by ts limit 1";
const STATEMENT_QUEUE_REMOVE_READY: &str = "delete from queue where id = ?";
const STATEMENT_QUEUE_ADD_RUNNING: &str = "insert into queue_running (deadline, id, data, backoff_schedule, keys_if_undelivered) values(?, ?, ?, ?, ?)";
const STATEMENT_QUEUE_UPDATE_RUNNING_DEADLINE: &str =
  "update queue_running set deadline = ? where id = ?";
const STATEMENT_QUEUE_REMOVE_RUNNING: &str =
  "delete from queue_running where id = ?";
const STATEMENT_QUEUE_GET_RUNNING_BY_ID: &str = "select deadline, id, data, backoff_schedule, keys_if_undelivered from queue_running where id = ?";
const STATEMENT_QUEUE_GET_RUNNING_PAST_DEADLINE: &str =
  "select id from queue_running where deadline <= ? order by deadline limit 100";

const STATEMENT_CREATE_MIGRATION_TABLE: &str = "
create table if not exists migration_state(
  k integer not null primary key,
  version integer not null
)
";

const MIGRATIONS: [&str; 3] = [
  "
create table data_version (
  k integer primary key,
  version integer not null
);
insert into data_version (k, version) values (0, 0);
create table kv (
  k blob primary key,
  v blob not null,
  v_encoding integer not null,
  version integer not null
) without rowid;
",
  "
create table queue (
  ts integer not null,
  id text not null,
  data blob not null,
  backoff_schedule text not null,
  keys_if_undelivered blob not null,

  primary key (ts, id)
);
create table queue_running(
  deadline integer not null,
  id text not null,
  data blob not null,
  backoff_schedule text not null,
  keys_if_undelivered blob not null,

  primary key (deadline, id)
);
",
  "
alter table kv add column seq integer not null default 0;
alter table data_version add column seq integer not null default 0;
alter table kv add column expiration_ms integer not null default -1;
create index kv_expiration_ms_idx on kv (expiration_ms);
",
];

const DISPATCH_CONCURRENCY_LIMIT: usize = 100;
const DEFAULT_BACKOFF_SCHEDULE: [u32; 5] = [100, 1000, 5000, 30000, 60000];

// The time after which a message in the queue_running table is considered dead
// and should be requeued. This is a safety net in case the process dies while
// processing a message.
const MESSAGE_DEADLINE_TIMEOUT: Duration = Duration::from_secs(5);

pub struct TypeError(String);

impl std::fmt::Display for TypeError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}", self.0)
  }
}

impl std::fmt::Debug for TypeError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}", self.0)
  }
}

impl std::error::Error for TypeError {}

pub struct SqliteBackend {
  conn: rusqlite::Connection,
  pub dequeue_notify: Arc<tokio::sync::Notify>,
  pub messages_running: HashSet<QueueMessageId>,
}

impl SqliteBackend {
  fn run_tx<F, R>(&mut self, mut f: F) -> Result<R, anyhow::Error>
  where
    F: FnMut(&mut rusqlite::Transaction) -> Result<R, anyhow::Error>,
  {
    sqlite_retry_loop(move || {
      let mut tx = self.conn.transaction()?;
      let result = f(&mut tx);
      if result.is_ok() {
        tx.commit()?;
      }
      result
    })
  }

  pub fn new(
    conn: rusqlite::Connection,
    dequeue_notify: Arc<tokio::sync::Notify>,
  ) -> Result<Self, anyhow::Error> {
    conn.pragma_update(None, "journal_mode", "wal")?;

    let mut this = Self {
      conn,
      dequeue_notify,
      messages_running: HashSet::new(),
    };

    this.run_tx(|tx| {
      tx.execute(STATEMENT_CREATE_MIGRATION_TABLE, [])?;

      let current_version: usize = tx
        .query_row(
          "select version from migration_state where k = 0",
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
            "replace into migration_state (k, version) values(?, ?)",
            [&0, &version],
          )?;
        }
      }

      Ok(())
    })?;

    Ok(this)
  }

  pub fn snapshot_read(
    &mut self,
    requests: Vec<ReadRange>,
    _options: SnapshotReadOptions,
  ) -> Result<Vec<ReadRangeOutput>, anyhow::Error> {
    self.run_tx(|tx| {
      let mut responses = Vec::with_capacity(requests.len());
      for request in &*requests {
        let mut stmt = tx.prepare_cached(if request.reverse {
          STATEMENT_KV_RANGE_SCAN_REVERSE
        } else {
          STATEMENT_KV_RANGE_SCAN
        })?;
        let entries = stmt
          .query_map(
            (
              request.start.as_slice(),
              request.end.as_slice(),
              request.limit.get(),
            ),
            |row| {
              let key: Vec<u8> = row.get(0)?;
              let value: Vec<u8> = row.get(1)?;
              let encoding: i64 = row.get(2)?;

              let value = decode_value(value, encoding);

              let version: i64 = row.get(3)?;
              Ok(KvEntry {
                key,
                value,
                versionstamp: version_to_versionstamp(version),
              })
            },
          )?
          .collect::<Result<Vec<_>, rusqlite::Error>>()?;
        responses.push(ReadRangeOutput { entries });
      }

      Ok(responses)
    })
  }

  pub fn atomic_write(
    &mut self,
    write: AtomicWrite,
  ) -> Result<Option<CommitResult>, anyhow::Error> {
    let (has_enqueues, commit_result) = self.run_tx(|tx| {
      for check in &write.checks {
        let real_versionstamp = tx
          .prepare_cached(STATEMENT_KV_POINT_GET_VERSION_ONLY)?
          .query_row([check.key.as_slice()], |row| row.get(0))
          .optional()?
          .map(version_to_versionstamp);
        if real_versionstamp != check.versionstamp {
          return Ok((false, None));
        }
      }

      let version: i64 = tx
        .prepare_cached(STATEMENT_INC_AND_GET_DATA_VERSION)?
        .query_row([], |row| row.get(0))?;

      for mutation in &write.mutations {
        match &mutation.kind {
          MutationKind::Set(value) => {
            let (value, encoding) = encode_value(value);
            let changed =
              tx.prepare_cached(STATEMENT_KV_POINT_SET)?.execute(params![
                mutation.key,
                value,
                &encoding,
                &version,
                mutation
                  .expire_at
                  .and_then(|x| i64::try_from(x).ok())
                  .unwrap_or(-1i64)
              ])?;
            assert_eq!(changed, 1)
          }
          MutationKind::Delete => {
            let changed = tx
              .prepare_cached(STATEMENT_KV_POINT_DELETE)?
              .execute(params![mutation.key])?;
            assert!(changed == 0 || changed == 1)
          }
          MutationKind::Sum(operand) => {
            mutate_le64(
              &tx,
              &mutation.key,
              "sum",
              operand,
              version,
              |a, b| a.wrapping_add(b),
            )?;
          }
          MutationKind::Min(operand) => {
            mutate_le64(
              &tx,
              &mutation.key,
              "min",
              operand,
              version,
              |a, b| a.min(b),
            )?;
          }
          MutationKind::Max(operand) => {
            mutate_le64(
              &tx,
              &mutation.key,
              "max",
              operand,
              version,
              |a, b| a.max(b),
            )?;
          }
        }
      }

      let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

      let has_enqueues = !write.enqueues.is_empty();
      for enqueue in &write.enqueues {
        let id = Uuid::new_v4().to_string();
        let backoff_schedule = serde_json::to_string(
          &enqueue
            .backoff_schedule
            .as_deref()
            .or_else(|| Some(&DEFAULT_BACKOFF_SCHEDULE[..])),
        )?;
        let keys_if_undelivered =
          serde_json::to_string(&enqueue.keys_if_undelivered)?;

        let changed =
          tx.prepare_cached(STATEMENT_QUEUE_ADD_READY)?
            .execute(params![
              now + enqueue.delay_ms,
              id,
              &enqueue.payload,
              &backoff_schedule,
              &keys_if_undelivered
            ])?;
        assert_eq!(changed, 1)
      }

      let new_versionstamp = version_to_versionstamp(version);

      Ok((
        has_enqueues,
        Some(CommitResult {
          versionstamp: new_versionstamp,
        }),
      ))
    })?;

    if has_enqueues {
      self.dequeue_notify.notify_one();
    }

    Ok(commit_result)
  }

  pub fn queue_running_keepalive(&mut self) -> Result<(), anyhow::Error> {
    let running_messages = self.messages_running.clone();
    let now = SystemTime::now()
      .duration_since(SystemTime::UNIX_EPOCH)
      .unwrap()
      .as_millis() as u64;
    self.run_tx(|tx| {
      for id in &running_messages {
        let changed = tx
          .prepare_cached(STATEMENT_QUEUE_UPDATE_RUNNING_DEADLINE)?
          .execute(params![
            now + MESSAGE_DEADLINE_TIMEOUT.as_millis() as u64,
            &id.0
          ])?;
        assert!(changed <= 1);
      }
      Ok(())
    })?;
    Ok(())
  }

  pub fn queue_cleanup(&mut self) -> Result<(), anyhow::Error> {
    let now = UNIX_EPOCH.elapsed().unwrap().as_millis() as u64;
    let queue_dequeue_waker = self.dequeue_notify.clone();
    loop {
      let done = self.run_tx(|tx| {
        let entries = tx
          .prepare_cached(STATEMENT_QUEUE_GET_RUNNING_PAST_DEADLINE)?
          .query_map([now], |row| {
            let id: String = row.get(0)?;
            Ok(id)
          })?
          .collect::<Result<Vec<_>, rusqlite::Error>>()?;
        for id in &entries {
          if requeue_message(tx, id)? {
            queue_dequeue_waker.notify_one();
          }
        }
        Ok(entries.is_empty())
      })?;
      if done {
        break;
      }
    }
    Ok(())
  }

  pub fn queue_dequeue_message(
    &mut self,
  ) -> Result<(Option<DequeuedMessage>, Option<SystemTime>), anyhow::Error> {
    let now = UNIX_EPOCH.elapsed().unwrap().as_millis() as u64;

    let can_dispatch = self.messages_running.len() < DISPATCH_CONCURRENCY_LIMIT;

    self.run_tx(|tx| {
      let message = can_dispatch
        .then(|| {
          let message = tx
            .prepare_cached(STATEMENT_QUEUE_GET_NEXT_READY)?
            .query_row([now], |row| {
              let ts: u64 = row.get(0)?;
              let id: String = row.get(1)?;
              let data: Vec<u8> = row.get(2)?;
              let backoff_schedule: String = row.get(3)?;
              let keys_if_undelivered: String = row.get(4)?;
              Ok((ts, id, data, backoff_schedule, keys_if_undelivered))
            })
            .optional()?;

          if let Some((ts, id, data, backoff_schedule, keys_if_undelivered)) =
            message
          {
            let changed = tx
              .prepare_cached(STATEMENT_QUEUE_REMOVE_READY)?
              .execute(params![id])?;
            assert_eq!(changed, 1);

            let deadline = ts + MESSAGE_DEADLINE_TIMEOUT.as_millis() as u64;
            let changed = tx
              .prepare_cached(STATEMENT_QUEUE_ADD_RUNNING)?
              .execute(params![
                deadline,
                id,
                &data,
                &backoff_schedule,
                &keys_if_undelivered
              ])?;
            assert_eq!(changed, 1);

            Ok::<_, anyhow::Error>(Some(DequeuedMessage {
              id: QueueMessageId(id),
              payload: data,
            }))
          } else {
            Ok(None)
          }
        })
        .transpose()?
        .flatten();

      let next_ready_ts = tx
        .prepare_cached(STATEMENT_QUEUE_GET_EARLIEST_READY)?
        .query_row([], |row| {
          let ts: u64 = row.get(0)?;
          Ok(ts)
        })
        .optional()?;
      let next_ready =
        next_ready_ts.map(|x| UNIX_EPOCH + Duration::from_millis(x));

      Ok((message, next_ready))
    })
  }

  pub fn queue_next_ready(
    &mut self,
  ) -> Result<Option<SystemTime>, anyhow::Error> {
    self.run_tx(|tx| {
      let next_ready_ts = tx
        .prepare_cached(STATEMENT_QUEUE_GET_EARLIEST_READY)?
        .query_row([], |row| {
          let ts: u64 = row.get(0)?;
          Ok(ts)
        })
        .optional()?;
      let next_ready =
        next_ready_ts.map(|x| UNIX_EPOCH + Duration::from_millis(x));

      Ok(next_ready)
    })
  }

  pub fn queue_finish_message(
    &mut self,
    id: &QueueMessageId,
    success: bool,
  ) -> Result<(), anyhow::Error> {
    let requeued = self.run_tx(|tx| {
      let requeued = if success {
        let changed = tx
          .prepare_cached(STATEMENT_QUEUE_REMOVE_RUNNING)?
          .execute([&id.0])?;
        assert!(changed <= 1);
        false
      } else {
        requeue_message(tx, &id.0)?
      };
      Ok(requeued)
    })?;
    if requeued {
      self.dequeue_notify.notify_one();
    }
    Ok(())
  }

  pub fn collect_expired(
    &mut self,
  ) -> Result<Option<SystemTime>, anyhow::Error> {
    let now = SystemTime::now()
      .duration_since(SystemTime::UNIX_EPOCH)
      .unwrap()
      .as_millis() as u64;
    self.run_tx(|tx| {
      tx.prepare_cached(STATEMENT_DELETE_ALL_EXPIRED)?
        .execute(params![now])?;
      let earliest_expiration_ms: Option<u64> = tx
        .prepare_cached(STATEMENT_EARLIEST_EXPIRATION)?
        .query_row(params![], |row| {
          let expiration_ms: u64 = row.get(0)?;
          Ok(expiration_ms)
        })
        .optional()?;
      Ok(earliest_expiration_ms.map(|x| UNIX_EPOCH + Duration::from_millis(x)))
    })
  }
}

fn requeue_message(
  tx: &mut Transaction,
  id: &str,
) -> Result<bool, anyhow::Error> {
  let maybe_message = tx
    .prepare_cached(STATEMENT_QUEUE_GET_RUNNING_BY_ID)?
    .query_row([id], |row| {
      let deadline: u64 = row.get(0)?;
      let id: String = row.get(1)?;
      let data: Vec<u8> = row.get(2)?;
      let backoff_schedule: String = row.get(3)?;
      let keys_if_undelivered: String = row.get(4)?;
      Ok((deadline, id, data, backoff_schedule, keys_if_undelivered))
    })
    .optional()?;
  let Some((_, id, data, backoff_schedule, keys_if_undelivered)) = maybe_message else {
    return Ok(false);
  };

  let backoff_schedule = {
    let backoff_schedule =
      serde_json::from_str::<Option<Vec<u64>>>(&backoff_schedule)?;
    backoff_schedule.unwrap_or_default()
  };

  let mut requeued = false;
  if !backoff_schedule.is_empty() {
    // Requeue based on backoff schedule
    let now = SystemTime::now()
      .duration_since(SystemTime::UNIX_EPOCH)
      .unwrap()
      .as_millis() as u64;
    let new_ts = now + backoff_schedule[0];
    let new_backoff_schedule = serde_json::to_string(&backoff_schedule[1..])?;
    let changed = tx
      .prepare_cached(STATEMENT_QUEUE_ADD_READY)?
      .execute(params![
        new_ts,
        id,
        &data,
        &new_backoff_schedule,
        &keys_if_undelivered
      ])
      .unwrap();
    assert_eq!(changed, 1);
    requeued = true;
  } else if !keys_if_undelivered.is_empty() {
    // No more requeues. Insert the message into the undelivered queue.
    let keys_if_undelivered =
      serde_json::from_str::<Vec<Vec<u8>>>(&keys_if_undelivered)?;

    let version: i64 = tx
      .prepare_cached(STATEMENT_INC_AND_GET_DATA_VERSION)?
      .query_row([], |row| row.get(0))?;

    for key in keys_if_undelivered {
      let changed = tx
        .prepare_cached(STATEMENT_KV_POINT_SET)?
        .execute(params![key, &data, &VALUE_ENCODING_V8, &version, -1i64])?;
      assert_eq!(changed, 1);
    }
  }

  // Remove from running
  let changed = tx
    .prepare_cached(STATEMENT_QUEUE_REMOVE_RUNNING)?
    .execute(params![id])?;
  assert_eq!(changed, 1);

  Ok(requeued)
}

#[derive(Clone)]
pub struct QueueMessageId(String);

pub struct DequeuedMessage {
  pub id: QueueMessageId,
  pub payload: Vec<u8>,
}

pub fn sqlite_retry_loop<R>(
  mut f: impl FnMut() -> Result<R, anyhow::Error>,
) -> Result<R, anyhow::Error> {
  loop {
    match f() {
      Ok(x) => return Ok(x),
      Err(e) => {
        if let Some(x) = e.downcast_ref::<rusqlite::Error>() {
          if x.sqlite_error_code() == Some(rusqlite::ErrorCode::DatabaseBusy) {
            log::debug!("kv: Database is busy, retrying");
            std::thread::sleep(Duration::from_millis(
              rand::thread_rng().gen_range(5..20),
            ));
            continue;
          }
        }
        return Err(e);
      }
    }
  }
}

/// Mutates a LE64 value in the database, defaulting to setting it to the
/// operand if it doesn't exist.
fn mutate_le64(
  tx: &Transaction,
  key: &[u8],
  op_name: &str,
  operand: &Value,
  new_version: i64,
  mutate: impl FnOnce(u64, u64) -> u64,
) -> Result<(), anyhow::Error> {
  let Value::U64(operand) = *operand else {
    return Err(TypeError(format!(
      "Failed to perform '{op_name}' mutation on a non-U64 operand"
    )).into());
  };

  let old_value = tx
    .prepare_cached(STATEMENT_KV_POINT_GET_VALUE_ONLY)?
    .query_row([key], |row| {
      let value: Vec<u8> = row.get(0)?;
      let encoding: i64 = row.get(1)?;

      let value = decode_value(value, encoding);
      Ok(value)
    })
    .optional()?;

  let new_value = match old_value {
    Some(Value::U64(old_value) ) => mutate(old_value, operand),
    Some(_) => return Err(TypeError(format!("Failed to perform '{op_name}' mutation on a non-U64 value in the database")).into()),
    None => operand,
  };

  let new_value = Value::U64(new_value);
  let (new_value, encoding) = encode_value(&new_value);

  let changed = tx.prepare_cached(STATEMENT_KV_POINT_SET)?.execute(params![
    key,
    &new_value[..],
    encoding,
    new_version,
    -1i64,
  ])?;
  assert_eq!(changed, 1);

  Ok(())
}

fn version_to_versionstamp(version: i64) -> [u8; 10] {
  let mut versionstamp = [0; 10];
  versionstamp[..8].copy_from_slice(&version.to_be_bytes());
  versionstamp
}

const VALUE_ENCODING_V8: i64 = 1;
const VALUE_ENCODING_LE64: i64 = 2;
const VALUE_ENCODING_BYTES: i64 = 3;

fn decode_value(value: Vec<u8>, encoding: i64) -> Value {
  match encoding {
    VALUE_ENCODING_V8 => Value::V8(value),
    VALUE_ENCODING_BYTES => Value::Bytes(value),
    VALUE_ENCODING_LE64 => {
      let mut buf = [0; 8];
      buf.copy_from_slice(&value);
      Value::U64(u64::from_le_bytes(buf))
    }
    _ => todo!(),
  }
}

fn encode_value(value: &Value) -> (Cow<'_, [u8]>, i64) {
  match value {
    Value::V8(value) => (Cow::Borrowed(value), VALUE_ENCODING_V8),
    Value::Bytes(value) => (Cow::Borrowed(value), VALUE_ENCODING_BYTES),
    Value::U64(value) => {
      let mut buf = [0; 8];
      buf.copy_from_slice(&value.to_le_bytes());
      (Cow::Owned(buf.to_vec()), VALUE_ENCODING_LE64)
    }
  }
}
