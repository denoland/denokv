#![deny(clippy::all)]
use denokv_proto::ConvertError;
use denokv_sqlite::SqliteBackendError;
// use denokv_proto::datapath::SnapshotReadStatus; // TODO waiting on watch pr
// use denokv_proto::datapath::WatchOutput;
// use denokv_sqlite::SqliteNotifier;
use denokv_sqlite::SqliteMessageHandle;
use napi::bindgen_prelude::{Buffer,Result,Either,Undefined};
use napi_derive::napi;
use denokv_sqlite::Sqlite;
use rusqlite::OpenFlags;
use tokio::sync::Notify;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;
use denokv_sqlite::Connection;
use rand::SeedableRng;
use denokv_proto::SnapshotReadOptions;
use denokv_proto::ReadRange;
use denokv_proto::Consistency;
use denokv_proto::datapath as pb;
use prost::Message;
use std::io::Cursor;
use once_cell::sync::Lazy;

#[napi]
pub fn open(path: String, in_memory: Option<bool>, debug: bool) -> u32 {
  if debug { println!("[napi] open: path={:#?} in_memory={:#?}", path, in_memory) }
  let flags = if in_memory.is_some() && in_memory.unwrap() { OpenFlags::default() | OpenFlags::SQLITE_OPEN_MEMORY } else { OpenFlags::default() };
  let conn = Connection::open_with_flags(Path::new(&path), flags).unwrap();
  let rng: Box<_> = Box::new(rand::rngs::StdRng::from_entropy());
  
  let opened_path = conn.path().unwrap().to_owned();
  // let notifier = SqliteNotifier::default(); // TODO waiting on watch pr
  // let sqlite = Sqlite::new(conn, notifier, rng).unwrap();
  let notify = Arc::new(Notify::new());
  let sqlite = Sqlite::new(conn, notify, rng).unwrap();

  let db_id = DBS.lock().unwrap().keys().max().unwrap_or(&0) + 1;
  DBS.lock().unwrap().insert(db_id, sqlite);
  if debug { println!("[napi] open: db_id={:#?} opened_path={:#?}", db_id, opened_path) }
  db_id
}

#[napi]
pub fn close(db_id: u32, debug: bool) {
  if debug { println!("[napi] close: db_id={:#?}", db_id) }
  let mut dbs = DBS.lock().unwrap();
  dbs.get(&db_id).unwrap().close();
  dbs.remove(&db_id);
}

#[napi]
pub async fn snapshot_read(db_id: u32, snapshot_read_bytes: Buffer, debug: bool) -> Result<Buffer> {
  let snapshot_read_pb = pb::SnapshotRead::decode(&mut Cursor::new(snapshot_read_bytes))
    .map_err(convert_prost_decode_error_to_anyhow)?;
  if debug { println!("[napi] snapshot_read: db_id={:#?} snapshot_read_pb={:#?}", db_id, snapshot_read_pb) }

  let options = SnapshotReadOptions {
    consistency: Consistency::Strong,
  };

  let requests: Vec<ReadRange> = snapshot_read_pb.try_into().map_err(convert_error_to_anyhow)?;

  let db = DBS.lock().unwrap().get(&db_id).unwrap().to_owned();

  let output_pb: pb::SnapshotReadOutput = db
    .snapshot_read(requests, options)
    .await
    .map_err(convert_sqlite_backend_error_to_anyhow)?
    .into();

  if debug { println!("[napi] snapshot_read: output_pb={:#?}", output_pb) }

  Ok(to_buffer(output_pb))
}

#[napi]
pub async fn atomic_write(db_id: u32, atomic_write_bytes: Buffer, debug: bool) -> Result<Buffer> {
  let atomic_write_pb = pb::AtomicWrite::decode(&mut Cursor::new(atomic_write_bytes))
    .map_err(convert_prost_decode_error_to_anyhow)?;
  if debug { println!("[napi] atomic_write: db_id={:#?} atomic_write_pb={:#?}", db_id, atomic_write_pb) }

  let atomic_write: denokv_proto::AtomicWrite = atomic_write_pb.try_into().map_err(convert_error_to_anyhow)?;

  let db = DBS.lock().unwrap().get(&db_id).unwrap().to_owned();

  let output_pb: pb::AtomicWriteOutput = db
    .atomic_write(atomic_write)
    .await
    .map_err(convert_sqlite_backend_error_to_anyhow)?
    .into();

  if debug { println!("[napi] atomic_write: db_id={:#?} output_pb={:#?}", db_id, output_pb) }

  Ok(to_buffer(output_pb))
}

#[napi(object)]
pub struct QueueMessage {
  pub bytes: Buffer,
  pub message_id: u32,
}

#[napi]
pub async fn dequeue_next_message(db_id: u32, debug: bool) -> Result<Either<QueueMessage, Undefined>> {
  if debug { println!("[napi] dequeue_next_message: db_id={:#?}", db_id) }

  let db: Sqlite = DBS.lock().unwrap().get(&db_id).unwrap().to_owned();

  let opt_handle = db.dequeue_next_message()
    .await
    .map_err(convert_sqlite_backend_error_to_anyhow)?;

  if opt_handle.is_none() {
    if debug { println!("[napi] dequeue_next_message: no messages! db_id={:#?}", db_id) }
    return Ok(Either::B(()));
  }

  let mut handle = opt_handle.unwrap();
  let payload = handle.take_payload()
    .await
    .map_err(convert_sqlite_backend_error_to_anyhow)?;

  let message_id: u32 = MSGS.lock().unwrap().keys().max().unwrap_or(&0) + 1;
  MSGS.lock().unwrap().insert(message_id, handle);
  if debug { println!("[napi] dequeue_next_message: received message db_id={:#?} message_id={:#?}", db_id, message_id) }
  
  let bytes = Buffer::from(payload);
  Ok(Either::A(QueueMessage { bytes, message_id }))
}

#[napi]
pub async fn finish_message(db_id: u32, message_id: u32, success: bool, debug: bool) -> Result<()> {
  if debug { println!("[napi] finish_message db_id={:#?} message_id={:#?} success={:#?}", db_id, message_id, success) }
  let opt_handle = MSGS.lock().unwrap().remove(&message_id);
  let handle = opt_handle.unwrap();

  handle.finish(success)
    .await
    .map_err(convert_sqlite_backend_error_to_anyhow)?;

  Ok(())
}

#[napi]
pub async fn start_watch(db_id: u32, watch_bytes: Buffer, debug: bool) -> Result<u32> {
  if debug { println!("[napi] start_watch: db_id={:#?} watch_bytes={:#?}", db_id, watch_bytes.to_vec()) }

//   // TODO waiting on watch pr
//   let watch_pb: pb::Watch = pb::Watch::decode(&mut Cursor::new(watch_bytes)).unwrap();
//   if debug { println!("[napi] start_watch: db_id={:#?} watch_pb={:#?}", db_id, watch_pb) }

//   let keys = watch_pb.keys.iter().map(|v| { v.key.clone() }).collect();
//   let db: Sqlite = DBS.lock().unwrap().get(&db_id).unwrap().to_owned();
//   let _stream = db.watch(keys);

//   // TODO store stream in WATCHES?

  let watch_id: u32 = WATCHES.lock().unwrap().keys().max().unwrap_or(&0) + 1;
  WATCHES.lock().unwrap().insert(watch_id, ());

  Ok(watch_id)
}

#[napi]
pub async fn dequeue_next_watch_message(db_id: u32, watch_id: u32, debug: bool) -> Result<Either<Buffer, Undefined>> {
  if debug { println!("[napi] dequeue_next_watch_message: db_id={:#?} watch_id={:#?}", db_id, watch_id) }

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
  if debug { println!("[napi] end_watch: db_id={:#?} watch_id={:#?}", db_id, watch_id) }
  WATCHES.lock().unwrap().remove(&watch_id);
}

//

static DBS: Lazy<Mutex<HashMap<u32, Sqlite>>> = Lazy::new(|| {
  Mutex::new(HashMap::new())
});

static MSGS: Lazy<Mutex<HashMap<u32, SqliteMessageHandle>>> = Lazy::new(|| {
  Mutex::new(HashMap::new())
});

static WATCHES: Lazy<Mutex<HashMap<u32, ()>>> = Lazy::new(|| {
  Mutex::new(HashMap::new())
});

fn to_buffer<T: prost::Message>(output: T) -> Buffer {
  let mut buf = Vec::new();
  buf.reserve(output.encoded_len());
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
    ConvertError::TooManyQueueUndeliveredKeys => String::from("TooManyQueueUndeliveredKeys"),
    ConvertError::TooManyQueueBackoffIntervals => String::from("TooManyQueueBackoffIntervals"),
    ConvertError::QueueBackoffIntervalTooLarge => String::from("QueueBackoffIntervalTooLarge"),
    ConvertError::InvalidReadRangeLimit => String::from("InvalidReadRangeLimit"),
    ConvertError::DecodeError => String::from("DecodeError"),
    ConvertError::InvalidVersionstamp => String::from("InvalidVersionstamp"),
    ConvertError::InvalidMutationKind => String::from("InvalidMutationKind"),
    ConvertError::InvalidMutationExpireAt => String::from("InvalidMutationExpireAt"),
    ConvertError::InvalidMutationEnqueueDeadline => String::from("InvalidMutationEnqueueDeadline"),
    // ConvertError::TooManyWatchedKeys => String::from("TooManyWatchedKeys"),  // TODO waiting on watch pr
  }
}

fn convert_error_to_anyhow(err: denokv_proto::ConvertError) -> anyhow::Error { anyhow::anyhow!(convert_error_to_str(err)) }
fn convert_sqlite_backend_error_to_anyhow(err: SqliteBackendError) -> anyhow::Error { err.into() }
fn convert_prost_decode_error_to_anyhow(err: prost::DecodeError) -> anyhow::Error { err.into() }
