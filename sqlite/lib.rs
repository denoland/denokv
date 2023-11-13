// Copyright 2023 the Deno authors. All rights reserved. MIT license.

mod backend;
mod time;

use std::collections::VecDeque;
use std::ops::Add;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread::JoinHandle;
use std::time::Duration;

pub use crate::backend::sqlite_retry_loop;
use crate::backend::DequeuedMessage;
use crate::backend::QueueMessageId;
use crate::backend::SqliteBackend;
pub use crate::backend::SqliteBackendError;
use chrono::DateTime;
use chrono::Utc;
use denokv_proto::AtomicWrite;
use denokv_proto::CommitResult;
use denokv_proto::Database;
use denokv_proto::QueueMessageHandle;
use denokv_proto::ReadRange;
use denokv_proto::ReadRangeOutput;
use denokv_proto::SnapshotReadOptions;
use futures::future::Either;
use rand::RngCore;
pub use rusqlite::Connection;
use time::utc_now;
use tokio::select;
use tokio::sync::oneshot;
use tokio::sync::Notify;

/// The interval at which the queue_running table is cleaned up of stale
/// entries where the deadline has passed.
const QUEUE_RUNNING_CLEANUP_INTERVAL: Duration = Duration::from_secs(60);
/// The interval at which the deadline of messages being processed in this
/// instance is updated in the queue_running table.
const QUEUE_MESSAGE_DEADLINE_UPDATE_INTERVAL: Duration = Duration::from_secs(2);
/// The minimum interval at which the queue is checked for the next message
/// to process, irrespective of any known deadlines.
const QUEUE_DEQUEUE_FALLBACK_INTERVAL: Duration = Duration::from_secs(60);
/// The minimum interval at which we try to process expired messages,
/// irrespective of any known deadlines.
const EXPIRY_FALLBACK_INTERVAL: Duration = Duration::from_secs(60);
/// The jitter applied to dequeueing of messages.
const QUEUE_DEQUEUE_JITTER: Duration = Duration::from_millis(100);
/// The jitter applied to expiry of messages.
const EXPIRY_JITTER: Duration = Duration::from_secs(1);

enum SqliteRequest {
  SnapshotRead {
    requests: Vec<ReadRange>,
    options: SnapshotReadOptions,
    sender: oneshot::Sender<Result<Vec<ReadRangeOutput>, SqliteBackendError>>,
  },
  AtomicWrite {
    write: AtomicWrite,
    sender: oneshot::Sender<Result<Option<CommitResult>, SqliteBackendError>>,
  },
  QueueDequeueMessage {
    sender: oneshot::Sender<DequeuedMessage>,
  },
  QueueFinishMessage {
    id: QueueMessageId,
    success: bool,
    sender: oneshot::Sender<Result<(), SqliteBackendError>>,
  },
}

#[derive(Clone)]
pub struct Sqlite {
  join_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
  shutdown_notify: Arc<Notify>,
  request_tx: tokio::sync::mpsc::Sender<SqliteRequest>,
}

impl Sqlite {
  pub fn new(
    conn: rusqlite::Connection,
    dequeue_notify: Arc<Notify>,
    versionstamp_rng: Box<dyn RngCore + Send>,
  ) -> Result<Sqlite, anyhow::Error> {
    let backend =
      SqliteBackend::new(conn, dequeue_notify.clone(), versionstamp_rng)?;
    let shutdown_notify = Arc::new(Notify::new());
    let (request_tx, request_rx) = tokio::sync::mpsc::channel(1);
    let shutdown_notify_ = shutdown_notify.clone();
    let join_handle: JoinHandle<()> = std::thread::spawn(move || {
      tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(sqlite_thread(
          backend,
          shutdown_notify_,
          dequeue_notify,
          request_rx,
        ));
    });
    Ok(Self {
      join_handle: Arc::new(Mutex::new(Some(join_handle))),
      shutdown_notify,
      request_tx,
    })
  }
}

async fn sqlite_thread(
  mut backend: SqliteBackend,
  shutdown_notify: Arc<Notify>,
  dequeue_notify: Arc<Notify>,
  mut request_rx: tokio::sync::mpsc::Receiver<SqliteRequest>,
) {
  let start = utc_now();
  if !backend.readonly {
    if let Err(err) = backend.queue_cleanup() {
      panic!("KV queue cleanup failed: {err}");
    }
  }
  let mut dequeue_channels: VecDeque<oneshot::Sender<DequeuedMessage>> =
    VecDeque::with_capacity(4);
  let queue_next_ready = if backend.readonly {
    None
  } else {
    match backend.queue_next_ready() {
      Ok(queue_next_ready) => queue_next_ready,
      Err(err) => panic!("KV queue_next_ready failed: {err}"),
    }
  };
  let mut queue_dequeue_deadline = compute_deadline_with_max_and_jitter(
    queue_next_ready,
    QUEUE_DEQUEUE_FALLBACK_INTERVAL,
    QUEUE_DEQUEUE_JITTER,
  );
  let mut queue_cleanup_deadline =
    start + duration_with_jitter(QUEUE_RUNNING_CLEANUP_INTERVAL);
  let mut queue_keepalive_deadline =
    start + duration_with_jitter(QUEUE_MESSAGE_DEADLINE_UPDATE_INTERVAL);
  let expiry_next_ready = if backend.readonly {
    None
  } else {
    match backend.collect_expired() {
      Ok(expiry_next_ready) => expiry_next_ready,
      Err(err) => panic!("KV collect expired failed: {err}"),
    }
  };
  let mut expiry_deadline = compute_deadline_with_max_and_jitter(
    expiry_next_ready,
    EXPIRY_FALLBACK_INTERVAL,
    EXPIRY_JITTER,
  );
  loop {
    let now = utc_now();
    let mut closest_deadline = queue_cleanup_deadline
      .min(queue_keepalive_deadline)
      .min(expiry_deadline);
    if !dequeue_channels.is_empty() {
      closest_deadline = closest_deadline.min(queue_dequeue_deadline);
    }
    let timer = if backend.readonly {
      Either::Left(futures::future::pending::<()>())
    } else if let Ok(time_to_closest_deadline) =
      closest_deadline.signed_duration_since(now).to_std()
    {
      Either::Right(Either::Left(tokio::time::sleep(time_to_closest_deadline)))
    } else {
      Either::Right(Either::Right(futures::future::ready(())))
    };
    select! {
      biased;
      _ = shutdown_notify.notified() => {
        break;
      },
      _ = dequeue_notify.notified(), if !dequeue_channels.is_empty() => {
        let queue_next_ready = match backend.queue_next_ready() {
          Ok(queue_next_ready) => queue_next_ready,
          Err(err) => panic!("KV queue_next_ready failed: {err}"),
        };
        queue_dequeue_deadline = compute_deadline_with_max_and_jitter(queue_next_ready, QUEUE_DEQUEUE_FALLBACK_INTERVAL, QUEUE_DEQUEUE_JITTER);
      },
      req = request_rx.recv() => {
        match req {
          Some(SqliteRequest::SnapshotRead { requests, options, sender }) => {
            let result = backend.snapshot_read(requests, options);
            sender.send(result).ok(); // ignore error if receiver is gone
          },
          Some(SqliteRequest::AtomicWrite { write, sender }) => {
            let result = backend.atomic_write(write);
            sender.send(result).ok(); // ignore error if receiver is gone
          },
          Some(SqliteRequest::QueueDequeueMessage { sender }) => {
            dequeue_channels.push_back(sender);
          },
          Some(SqliteRequest::QueueFinishMessage { id, success, sender }) => {
            let result = backend.queue_finish_message(&id, success);
            sender.send(result).ok(); // ignore error if receiver is gone
          },
          None => break,
        }
      },
      _ = timer => {
        if now >= queue_cleanup_deadline {
          queue_cleanup_deadline = now.add(duration_with_jitter(QUEUE_RUNNING_CLEANUP_INTERVAL));
          if let Err(err) = backend.queue_cleanup() {
            panic!("KV queue cleanup failed: {err}");
          }
        }
        if now >= queue_keepalive_deadline {
          queue_keepalive_deadline = now.add(duration_with_jitter(QUEUE_MESSAGE_DEADLINE_UPDATE_INTERVAL));
          if let Err(err) = backend.queue_running_keepalive() {
            panic!("KV queue running keepalive failed: {err}");
          }
        }
        if now >= queue_dequeue_deadline && !dequeue_channels.is_empty() {
          match backend.queue_dequeue_message() {
            Ok((Some(dequeued_message), next_ready)) => {
              let sender = dequeue_channels.pop_front().unwrap();
              sender.send(dequeued_message).ok(); // ignore error if receiver is gone
              queue_dequeue_deadline = compute_deadline_with_max_and_jitter(next_ready, QUEUE_DEQUEUE_FALLBACK_INTERVAL, QUEUE_DEQUEUE_JITTER);
            },
            Ok((None, next_ready)) => {
              queue_dequeue_deadline = compute_deadline_with_max_and_jitter(next_ready, QUEUE_DEQUEUE_FALLBACK_INTERVAL, QUEUE_DEQUEUE_JITTER);
            },
            Err(err) => {
              panic!("KV queue dequeue failed: {err}");
            },
          }
        }
        if now >= expiry_deadline {
          match backend.collect_expired() {
            Ok(next_ready) => {
              expiry_deadline = compute_deadline_with_max_and_jitter(next_ready, EXPIRY_FALLBACK_INTERVAL, EXPIRY_JITTER);
            },
            Err(err) => {
              panic!("KV collect expired failed: {err}");
            },
          }
        }
      },
    }
  }
}

//  Jitter to the duration +- ~10%.
fn duration_with_jitter(duration: Duration) -> std::time::Duration {
  let secs = duration.as_secs_f64();
  let secs = secs * (0.9 + rand::random::<f64>() * 0.2);
  std::time::Duration::from_secs_f64(secs)
}

fn duration_with_total_jitter(duration: Duration) -> std::time::Duration {
  let secs = duration.as_secs_f64();
  let secs = secs * rand::random::<f64>();
  std::time::Duration::from_secs_f64(secs)
}

fn compute_deadline_with_max_and_jitter(
  next_ready: Option<DateTime<Utc>>,
  max: Duration,
  jitter_duration: Duration,
) -> DateTime<Utc> {
  let fallback = utc_now() + max;
  next_ready.unwrap_or(fallback).min(fallback)
    + duration_with_total_jitter(jitter_duration)
}

impl Sqlite {
  pub async fn snapshot_read(
    &self,
    requests: Vec<ReadRange>,
    options: SnapshotReadOptions,
  ) -> Result<Vec<ReadRangeOutput>, SqliteBackendError> {
    let (sender, receiver) = oneshot::channel();
    self
      .request_tx
      .send(SqliteRequest::SnapshotRead {
        requests,
        options,
        sender,
      })
      .await
      .map_err(|_| SqliteBackendError::DatabaseClosed)?;
    receiver
      .await
      .map_err(|_| SqliteBackendError::DatabaseClosed)?
  }

  pub async fn atomic_write(
    &self,
    write: AtomicWrite,
  ) -> Result<Option<CommitResult>, SqliteBackendError> {
    let (sender, receiver) = oneshot::channel();
    self
      .request_tx
      .send(SqliteRequest::AtomicWrite { write, sender })
      .await
      .map_err(|_| SqliteBackendError::DatabaseClosed)?;
    receiver
      .await
      .map_err(|_| SqliteBackendError::DatabaseClosed)?
  }

  pub async fn dequeue_next_message(
    &self,
  ) -> Result<Option<SqliteMessageHandle>, SqliteBackendError> {
    let (sender, receiver) = oneshot::channel();
    let req = SqliteRequest::QueueDequeueMessage { sender };
    if self.request_tx.send(req).await.is_err() {
      return Ok(None);
    }
    let Ok(dequeued_message) = receiver.await else {
      return Ok(None);
    };
    Ok(Some(SqliteMessageHandle {
      id: dequeued_message.id,
      payload: Some(dequeued_message.payload),
      request_tx: self.request_tx.clone(),
    }))
  }

  pub fn close(&self) {
    self.shutdown_notify.notify_one();
    self
      .join_handle
      .lock()
      .unwrap()
      .take()
      .expect("can't close database twice")
      .join()
      .unwrap();
  }
}

#[async_trait::async_trait(?Send)]
impl Database for Sqlite {
  type QMH = SqliteMessageHandle;

  async fn snapshot_read(
    &self,
    requests: Vec<ReadRange>,
    options: SnapshotReadOptions,
  ) -> Result<Vec<ReadRangeOutput>, anyhow::Error> {
    let ranges = Sqlite::snapshot_read(self, requests, options).await?;
    Ok(ranges)
  }

  async fn atomic_write(
    &self,
    write: AtomicWrite,
  ) -> Result<Option<CommitResult>, anyhow::Error> {
    let res = Sqlite::atomic_write(self, write).await?;
    Ok(res)
  }

  async fn dequeue_next_message(
    &self,
  ) -> Result<Option<Self::QMH>, anyhow::Error> {
    let message_handle = Sqlite::dequeue_next_message(self).await?;
    Ok(message_handle)
  }

  fn close(&self) {
    Sqlite::close(self);
  }
}

pub struct SqliteMessageHandle {
  id: QueueMessageId,
  payload: Option<Vec<u8>>,
  request_tx: tokio::sync::mpsc::Sender<SqliteRequest>,
}

impl SqliteMessageHandle {
  async fn finish(&self, success: bool) -> Result<(), SqliteBackendError> {
    let (sender, receiver) = oneshot::channel();
    self
      .request_tx
      .send(SqliteRequest::QueueFinishMessage {
        id: self.id.clone(),
        success,
        sender,
      })
      .await
      .map_err(|_| SqliteBackendError::DatabaseClosed)?;
    receiver
      .await
      .map_err(|_| SqliteBackendError::DatabaseClosed)?
  }

  async fn take_payload(&mut self) -> Result<Vec<u8>, SqliteBackendError> {
    Ok(self.payload.take().expect("can't take payload twice"))
  }
}

#[async_trait::async_trait(?Send)]
impl QueueMessageHandle for SqliteMessageHandle {
  async fn finish(&self, success: bool) -> Result<(), anyhow::Error> {
    SqliteMessageHandle::finish(self, success).await?;
    Ok(())
  }

  async fn take_payload(&mut self) -> Result<Vec<u8>, anyhow::Error> {
    let payload = SqliteMessageHandle::take_payload(self).await?;
    Ok(payload)
  }
}
