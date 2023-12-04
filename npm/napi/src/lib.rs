// Copyright 2023 the Deno authors. All rights reserved. MIT license.

#![deny(clippy::all)]
use denokv_proto::ConvertError;
use denokv_sqlite::SqliteBackendError;
// use denokv_proto::datapath::SnapshotReadStatus; // TODO waiting on watch pr
// use denokv_proto::datapath::WatchOutput;
// use denokv_sqlite::SqliteNotifier;
use denokv_proto::datapath as pb;
use denokv_proto::Consistency;
use denokv_proto::ReadRange;
use denokv_proto::SnapshotReadOptions;
use denokv_sqlite::Connection;
use denokv_sqlite::Sqlite;
use denokv_sqlite::SqliteMessageHandle;
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
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::Notify;

#[napi]
pub fn open(path: String, in_memory: Option<bool>, debug: bool) -> u32 {
  if debug {
    println!("[napi] open: path={:#?} in_memory={:#?}", path, in_memory)
  }
  let flags = if let Some(true) = in_memory {
    OpenFlags::default() | OpenFlags::SQLITE_OPEN_MEMORY
  } else {
    OpenFlags::default()
  };
  let conn = Connection::open_with_flags(Path::new(&path), flags).unwrap();
  let rng = Box::new(rand::rngs::StdRng::from_entropy());

  let opened_path = conn.path().unwrap().to_owned();
  // let notifier = SqliteNotifier::default(); // TODO waiting on watch pr
  // let sqlite = Sqlite::new(conn, notifier, rng).unwrap();
  let notify = Arc::new(Notify::new());
  let sqlite = Sqlite::new(conn, notify, rng).unwrap();

  let db_id = DB_ID.fetch_add(1, Ordering::Relaxed);
  DBS.lock().unwrap().insert(db_id, sqlite);

  if debug {
    println!(
      "[napi] open: db_id={:#?} opened_path={:#?}",
      db_id, opened_path
    )
  }
  db_id
}

#[napi]
pub fn close(db_id: u32, debug: bool) {
  if debug {
    println!("[napi] close: db_id={:#?}", db_id)
  }
  let db = DBS.lock().unwrap().remove(&db_id);
  db.map(|db| db.close());
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
    .map(|db| db.clone())
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
    .map(|db| db.clone())
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
    .map(|db| db.clone())
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
  if debug {
    println!(
      "[napi] start_watch: db_id={:#?} watch_bytes={:#?}",
      db_id,
      watch_bytes.to_vec()
    )
  }

  // TODO waiting on watch pr
  // let watch_pb: pb::Watch = pb::Watch::decode(&mut Cursor::new(watch_bytes)).unwrap();
  // if debug { println!("[napi] start_watch: db_id={:#?} watch_pb={:#?}", db_id, watch_pb) }

  // let keys = watch_pb.keys.iter().map(|v| { v.key.clone() }).collect();
  // let db: Sqlite = DBS.lock().unwrap().get(&db_id).unwrap().to_owned();
  // let _stream = db.watch(keys);

  // TODO store stream in WATCHES?

  let watch_id = WATCH_ID.fetch_add(1, Ordering::Relaxed);
  WATCHES.lock().unwrap().insert(watch_id, ());

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

  // TODO waiting on watch pr
  // let watch_output_pb = WatchOutput {
  //   status: SnapshotReadStatus::SrUnspecified.into(),
  //   keys: [].to_vec()
  // };
  // let bytes = to_buffer(watch_output_pb);
  // Ok(Either::A(bytes))
  Ok(Either::B(()))
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

//

static DB_ID: AtomicU32 = AtomicU32::new(0);
static DBS: Lazy<Mutex<HashMap<u32, Sqlite>>> =
  Lazy::new(|| Mutex::new(HashMap::new()));

static MSG_ID: AtomicU32 = AtomicU32::new(0);
static MSGS: Lazy<Mutex<HashMap<u32, SqliteMessageHandle>>> =
  Lazy::new(|| Mutex::new(HashMap::new()));

static WATCH_ID: AtomicU32 = AtomicU32::new(0);
static WATCHES: Lazy<Mutex<HashMap<u32, ()>>> =
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
