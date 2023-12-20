// Copyright 2023 the Deno authors. All rights reserved. MIT license.

#![deny(clippy::all)]
use denokv_proto::datapath as pb;
use denokv_proto::Consistency;
use denokv_proto::ConvertError;
use denokv_proto::ReadRange;
use denokv_proto::SnapshotReadOptions;
use denokv_proto::WatchKeyOutput;
use denokv_sqlite::Connection;
use denokv_sqlite::Sqlite;
use denokv_sqlite::SqliteBackendError;
use denokv_sqlite::SqliteConfig;
use denokv_sqlite::SqliteMessageHandle;
use denokv_sqlite::SqliteNotifier;
use futures::stream::BoxStream;
use futures::StreamExt;
use futures::TryStreamExt;
use napi::bindgen_prelude::{Buffer, Either, Result, Undefined};
use napi_derive::napi;
use once_cell::sync::Lazy;
use prost::Message;
use rand::SeedableRng;
use rusqlite::OpenFlags;
use std::collections::HashMap;
use std::io::Cursor;
use std::path::Path;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::sync::Mutex;

#[napi]
pub fn open(path: String, in_memory: Option<bool>, debug: bool) -> Result<u32> {
  if debug {
    println!("[napi] open: path={:#?} in_memory={:#?}", path, in_memory)
  }
  let flags = if let Some(true) = in_memory {
    OpenFlags::default() | OpenFlags::SQLITE_OPEN_MEMORY
  } else {
    OpenFlags::default()
  };

  let opened_path = Connection::open_with_flags(Path::new(&path), flags)
    .map_err(anyhow::Error::from)?
    .path()
    .map(|p| p.to_string());
  let notifier = match &opened_path {
    Some(opened_path) if !opened_path.is_empty() => NOTIFIERS_MAP
      .lock()
      .unwrap()
      .entry(opened_path.clone())
      .or_default()
      .clone(),
    _ => SqliteNotifier::default(),
  };

  let sqlite = Sqlite::new(
    || {
      Ok((
        Connection::open_with_flags(Path::new(&path), flags)
          .map_err(anyhow::Error::from)?,
        Box::new(rand::rngs::StdRng::from_entropy()),
      ))
    },
    notifier,
    SqliteConfig {
      num_workers: 1,
      batch_timeout: None,
    },
  )?;

  let db_id = DB_ID.fetch_add(1, Ordering::Relaxed);
  DBS.lock().unwrap().insert(db_id, sqlite);

  if debug {
    println!(
      "[napi] open: db_id={:#?} opened_path={:#?}",
      db_id, opened_path
    )
  }
  Ok(db_id)
}

#[napi]
pub fn close(db_id: u32, debug: bool) {
  if debug {
    println!("[napi] close: db_id={:#?}", db_id)
  }
  let db = DBS.lock().unwrap().remove(&db_id);
  if let Some(db) = db {
    db.close()
  };
}

#[napi]
pub async fn snapshot_read(
  db_id: u32,
  snapshot_read_bytes: Buffer,
  debug: bool,
) -> Result<Buffer> {
  let snapshot_read_pb =
    pb::SnapshotRead::decode(&mut Cursor::new(snapshot_read_bytes))
      .map_err(convert_prost_decode_error_to_anyhow)?;
  if debug {
    println!(
      "[napi] snapshot_read: db_id={:#?} snapshot_read_pb={:#?}",
      db_id, snapshot_read_pb
    )
  }

  let options = SnapshotReadOptions {
    consistency: Consistency::Strong,
  };

  let requests: Vec<ReadRange> = snapshot_read_pb
    .try_into()
    .map_err(convert_error_to_anyhow)?;

  let db = DBS
    .lock()
    .unwrap()
    .get(&db_id)
    .cloned()
    .ok_or_else(|| anyhow::anyhow!("db not found"))?;

  let output_pb: pb::SnapshotReadOutput = db
    .snapshot_read(requests, options)
    .await
    .map_err(convert_sqlite_backend_error_to_anyhow)?
    .into();

  if debug {
    println!("[napi] snapshot_read: output_pb={:#?}", output_pb)
  }

  Ok(to_buffer(output_pb))
}

#[napi]
pub async fn atomic_write(
  db_id: u32,
  atomic_write_bytes: Buffer,
  debug: bool,
) -> Result<Buffer> {
  let atomic_write_pb =
    pb::AtomicWrite::decode(&mut Cursor::new(atomic_write_bytes))
      .map_err(convert_prost_decode_error_to_anyhow)?;
  if debug {
    println!(
      "[napi] atomic_write: db_id={:#?} atomic_write_pb={:#?}",
      db_id, atomic_write_pb
    )
  }

  let atomic_write: denokv_proto::AtomicWrite = atomic_write_pb
    .try_into()
    .map_err(convert_error_to_anyhow)?;

  let db = DBS
    .lock()
    .unwrap()
    .get(&db_id)
    .cloned()
    .ok_or_else(|| anyhow::anyhow!("db not found"))?;

  let output_pb: pb::AtomicWriteOutput = db
    .atomic_write(atomic_write)
    .await
    .map_err(convert_sqlite_backend_error_to_anyhow)?
    .into();

  if debug {
    println!(
      "[napi] atomic_write: db_id={:#?} output_pb={:#?}",
      db_id, output_pb
    )
  }

  Ok(to_buffer(output_pb))
}

#[napi(object)]
pub struct QueueMessage {
  pub bytes: Buffer,
  pub message_id: u32,
}

#[napi]
pub async fn dequeue_next_message(
  db_id: u32,
  debug: bool,
) -> Result<Either<QueueMessage, Undefined>> {
  if debug {
    println!("[napi] dequeue_next_message: db_id={:#?}", db_id)
  }

  let db = DBS
    .lock()
    .unwrap()
    .get(&db_id)
    .cloned()
    .ok_or_else(|| anyhow::anyhow!("db not found"))?;

  let opt_handle = db
    .dequeue_next_message()
    .await
    .map_err(convert_sqlite_backend_error_to_anyhow)?;

  let Some(mut handle) = opt_handle else {
    if debug {
      println!(
        "[napi] dequeue_next_message: no messages! db_id={:#?}",
        db_id
      )
    }
    return Ok(Either::B(()));
  };

  let payload = handle
    .take_payload()
    .await
    .map_err(convert_sqlite_backend_error_to_anyhow)?;

  let message_id = MSG_ID.fetch_add(1, Ordering::Relaxed);
  MSGS.lock().unwrap().insert(message_id, handle);

  if debug {
    println!(
      "[napi] dequeue_next_message: received message db_id={:#?} message_id={:#?}",
      db_id, message_id
    )
  }

  let bytes = Buffer::from(payload);
  Ok(Either::A(QueueMessage { bytes, message_id }))
}

#[napi]
pub async fn finish_message(
  db_id: u32,
  message_id: u32,
  success: bool,
  debug: bool,
) -> Result<()> {
  if debug {
    println!(
      "[napi] finish_message db_id={:#?} message_id={:#?} success={:#?}",
      db_id, message_id, success
    )
  }
  let handle = MSGS
    .lock()
    .unwrap()
    .remove(&message_id)
    .ok_or_else(|| anyhow::anyhow!("message not found"))?;

  handle
    .finish(success)
    .await
    .map_err(convert_sqlite_backend_error_to_anyhow)?;

  Ok(())
}

#[napi]
pub async fn start_watch(
  db_id: u32,
  watch_bytes: Buffer,
  debug: bool,
) -> Result<u32> {
  let watch_pb = pb::Watch::decode(&mut Cursor::new(watch_bytes))
    .map_err(convert_prost_decode_error_to_anyhow)?;

  if debug {
    println!(
      "[napi] start_watch: db_id={:#?} watch_pb={:#?}",
      db_id, watch_pb
    )
  }

  let keys = watch_pb.keys.iter().map(|v| v.key.clone()).collect();
  let db = DBS
    .lock()
    .unwrap()
    .get(&db_id)
    .cloned()
    .ok_or_else(|| anyhow::anyhow!("db not found"))?;
  let stream = db.watch(keys).map_err(|e| e.into()).boxed();

  let watch_id = WATCH_ID.fetch_add(1, Ordering::Relaxed);
  WATCHES.lock().unwrap().insert(watch_id, stream);

  Ok(watch_id)
}

#[napi]
pub async fn dequeue_next_watch_message(
  db_id: u32,
  watch_id: u32,
  debug: bool,
) -> Result<Either<Buffer, Undefined>> {
  if debug {
    println!(
      "[napi] dequeue_next_watch_message: db_id={:#?} watch_id={:#?}",
      db_id, watch_id
    )
  }

  let mut stream = WATCHES
    .lock()
    .unwrap()
    .remove(&watch_id)
    .ok_or_else(|| anyhow::anyhow!("watch not found"))?;

  let Some(next) = stream.next().await else {
    return Ok(Either::B(()));
  };
  let next = next?;
  WATCHES.lock().unwrap().insert(watch_id, stream);

  let watch_output_pb = pb::WatchOutput::from(next);
  let bytes = to_buffer(watch_output_pb);
  Ok(Either::A(bytes))
}

#[napi]
pub fn end_watch(db_id: u32, watch_id: u32, debug: bool) {
  if debug {
    println!(
      "[napi] end_watch: db_id={:#?} watch_id={:#?}",
      db_id, watch_id
    )
  }

  WATCHES.lock().unwrap().remove(&watch_id);
}

static NOTIFIERS_MAP: Lazy<Mutex<HashMap<String, SqliteNotifier>>> =
  Lazy::new(|| Mutex::new(HashMap::new()));

static DB_ID: AtomicU32 = AtomicU32::new(0);
static DBS: Lazy<Mutex<HashMap<u32, Sqlite>>> =
  Lazy::new(|| Mutex::new(HashMap::new()));

static MSG_ID: AtomicU32 = AtomicU32::new(0);
static MSGS: Lazy<Mutex<HashMap<u32, SqliteMessageHandle>>> =
  Lazy::new(|| Mutex::new(HashMap::new()));

static WATCH_ID: AtomicU32 = AtomicU32::new(0);
type WatchStream = BoxStream<'static, Result<Vec<WatchKeyOutput>>>;
static WATCHES: Lazy<Mutex<HashMap<u32, WatchStream>>> =
  Lazy::new(|| Mutex::new(HashMap::new()));

fn to_buffer<T: prost::Message>(output: T) -> Buffer {
  let mut buf = Vec::with_capacity(output.encoded_len());
  output.encode(&mut buf).unwrap();
  buf.into()
}

fn convert_error_to_str(err: denokv_proto::ConvertError) -> String {
  match err {
    ConvertError::KeyTooLong => String::from("KeyTooLong"),
    ConvertError::ValueTooLong => String::from("ValueTooLong"),
    ConvertError::ReadRangeTooLarge => String::from("ReadRangeTooLarge"),
    ConvertError::AtomicWriteTooLarge => String::from("AtomicWriteTooLarge"),
    ConvertError::TooManyReadRanges => String::from("TooManyReadRanges"),
    ConvertError::TooManyChecks => String::from("TooManyChecks"),
    ConvertError::TooManyMutations => String::from("TooManyMutations"),
    ConvertError::TooManyQueueUndeliveredKeys => {
      String::from("TooManyQueueUndeliveredKeys")
    }
    ConvertError::TooManyQueueBackoffIntervals => {
      String::from("TooManyQueueBackoffIntervals")
    }
    ConvertError::QueueBackoffIntervalTooLarge => {
      String::from("QueueBackoffIntervalTooLarge")
    }
    ConvertError::InvalidReadRangeLimit => {
      String::from("InvalidReadRangeLimit")
    }
    ConvertError::DecodeError => String::from("DecodeError"),
    ConvertError::InvalidVersionstamp => String::from("InvalidVersionstamp"),
    ConvertError::InvalidMutationKind => String::from("InvalidMutationKind"),
    ConvertError::InvalidMutationExpireAt => {
      String::from("InvalidMutationExpireAt")
    }
    ConvertError::InvalidMutationEnqueueDeadline => {
      String::from("InvalidMutationEnqueueDeadline")
    }
    ConvertError::TooManyWatchedKeys => String::from("TooManyWatchedKeys"),
  }
}

fn convert_error_to_anyhow(err: denokv_proto::ConvertError) -> anyhow::Error {
  anyhow::anyhow!(convert_error_to_str(err))
}

fn convert_sqlite_backend_error_to_anyhow(
  err: SqliteBackendError,
) -> anyhow::Error {
  err.into()
}

fn convert_prost_decode_error_to_anyhow(
  err: prost::DecodeError,
) -> anyhow::Error {
  err.into()
}
