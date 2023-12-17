// Copyright 2023 the Deno authors. All rights reserved. MIT license.

mod backend;
mod time;

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::num::NonZeroU32;
use std::ops::Add;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Barrier;
use std::sync::Mutex;
use std::sync::RwLock;
use std::sync::Weak;
use std::thread::JoinHandle;
use std::time::Duration;

pub use crate::backend::sqlite_retry_loop;
use crate::backend::DequeuedMessage;
use crate::backend::QueueMessageId;
use crate::backend::SqliteBackend;
pub use crate::backend::SqliteBackendError;
use async_stream::try_stream;
use chrono::DateTime;
use chrono::Utc;
use denokv_proto::AtomicWrite;
use denokv_proto::CommitResult;
use denokv_proto::Database;
use denokv_proto::QueueMessageHandle;
use denokv_proto::ReadRange;
use denokv_proto::ReadRangeOutput;
use denokv_proto::SnapshotReadOptions;
use denokv_proto::Versionstamp;
use denokv_proto::WatchKeyOutput;
use futures::future::Either;
use futures::FutureExt;
use futures::Stream;
use futures::StreamExt;
use rand::RngCore;
pub use rusqlite::Connection;
use time::utc_now;
use tokio::select;
use tokio::sync::futures::Notified;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::sync::Notify;
use tokio_stream::wrappers::ReceiverStream;

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
    sender: oneshot::Sender<
      Result<(Vec<ReadRangeOutput>, Versionstamp), SqliteBackendError>,
    >,
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

// A structure that across threads in the same process, can be used to support
// multiple Sqlite structs that all share the underlying database, but with
// different connections, to notify each other for KV queues and KV watch
// related events.
#[derive(Default, Clone)]
pub struct SqliteNotifier(Arc<SqliteNotifierInner>);

#[derive(Default)]
struct SqliteNotifierInner {
  dequeue_notify: Notify,
  key_watchers: RwLock<HashMap<Vec<u8>, watch::Sender<Versionstamp>>>,
}

struct SqliteKeySubscription {
  notifier: Weak<SqliteNotifierInner>,
  key: Option<Vec<u8>>,
  receiver: watch::Receiver<Versionstamp>,
}

impl SqliteNotifier {
  fn schedule_dequeue(&self) {
    self.0.dequeue_notify.notify_one();
  }

  fn notify_key_update(&self, key: &[u8], versionstamp: Versionstamp) {
    let key_watchers = self.0.key_watchers.read().unwrap();
    if let Some(sender) = key_watchers.get(key) {
      sender.send_if_modified(|in_place_versionstamp| {
        if *in_place_versionstamp < versionstamp {
          *in_place_versionstamp = versionstamp;
          true
        } else {
          false
        }
      });
    }
  }

  /// Subscribe to a given key. The returned subscription can be used to get
  /// updated whenever the key is updated past a given versionstamp.
  fn subscribe(&self, key: Vec<u8>) -> SqliteKeySubscription {
    let mut key_watchers = self.0.key_watchers.write().unwrap();
    let receiver = match key_watchers.entry(key.clone()) {
      Entry::Occupied(entry) => entry.get().subscribe(),
      Entry::Vacant(entry) => {
        let (sender, receiver) = watch::channel([0; 10]);
        entry.insert(sender);
        receiver
      }
    };
    SqliteKeySubscription {
      notifier: Arc::downgrade(&self.0),
      key: Some(key),
      receiver,
    }
  }
}

impl SqliteKeySubscription {
  /// Wait until the key has been updated since the given versionstamp.
  ///
  /// Returns false if the database is closing. Returns true if the key has been
  /// updated.
  async fn wait_until_updated(
    &mut self,
    last_read_versionstamp: Versionstamp,
  ) -> bool {
    let res = self
      .receiver
      .wait_for(|t| *t > last_read_versionstamp)
      .await;
    res.is_ok() // Err(_) means the database is closing.
  }
}

impl Drop for SqliteKeySubscription {
  fn drop(&mut self) {
    if let Some(notifier) = self.notifier.upgrade() {
      let key = self.key.take().unwrap();
      let mut key_watchers = notifier.key_watchers.write().unwrap();
      match key_watchers.entry(key) {
        Entry::Occupied(entry) => {
          // If there is only one subscriber left (this struct), then remove
          // the entry from the map.
          if entry.get().receiver_count() == 1 {
            entry.remove();
          }
        }
        Entry::Vacant(_) => unreachable!("the entry should still exist"),
      }
    }
  }
}

#[derive(Clone)]
pub struct Sqlite {
  shutdown_notify: Arc<Notify>,
  notifier: SqliteNotifier,
  workers: Vec<SqliteWorker>,
}

#[derive(Clone)]
struct SqliteWorker {
  request_tx: tokio::sync::mpsc::Sender<SqliteRequest>,
  join_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
}

#[derive(Clone, Debug)]
pub struct SqliteConfig {
  pub num_workers: usize,
  pub batch_timeout: Option<Duration>,
}

impl Sqlite {
  pub fn new(
    mut conn_gen: impl FnMut() -> anyhow::Result<(
      rusqlite::Connection,
      Box<dyn RngCore + Send>,
    )>,
    notifier: SqliteNotifier,
    config: SqliteConfig,
  ) -> Result<Sqlite, anyhow::Error> {
    let shutdown_notify = Arc::new(Notify::new());
    let batch_timeout = config.batch_timeout;

    let mut workers = Vec::with_capacity(config.num_workers);

    // This fence is used to block the current thread until all workers have got the
    // `Notified` future, to ensure no race is possible during a shutdown `notify_waiters`.
    let init_fence = Arc::new(Barrier::new(config.num_workers + 1));

    for i in 0..config.num_workers {
      let (request_tx, request_rx) = tokio::sync::mpsc::channel(1);
      let (conn, versionstamp_rng) = conn_gen()?;
      let backend =
        SqliteBackend::new(conn, notifier.clone(), versionstamp_rng, i != 0)?;
      let init_fence = init_fence.clone();
      let shutdown_notify = shutdown_notify.clone();
      let join_handle: JoinHandle<()> = std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
          .enable_all()
          .build()
          .unwrap()
          .block_on(async move {
            // We need to fence
            let shutdown_notify = shutdown_notify.notified();
            init_fence.wait();
            sqlite_thread(backend, shutdown_notify, request_rx, batch_timeout)
              .await
          });
      });
      workers.push(SqliteWorker {
        request_tx,
        join_handle: Arc::new(Mutex::new(Some(join_handle))),
      });
    }

    init_fence.wait();

    Ok(Self {
      shutdown_notify,
      notifier,
      workers,
    })
  }
}

async fn sqlite_thread(
  mut backend: SqliteBackend,
  shutdown_notify: Notified<'_>,
  request_rx: tokio::sync::mpsc::Receiver<SqliteRequest>,
  batch_timeout: Option<Duration>,
) {
  let mut shutdown_notify = std::pin::pin!(shutdown_notify);
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
  let mut request_rx =
    std::pin::pin!(ReceiverStream::new(request_rx).peekable());
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
      _ = &mut shutdown_notify => {
        break;
      },
      _ = backend.notifier.0.dequeue_notify.notified(), if !dequeue_channels.is_empty() => {
        let queue_next_ready = match backend.queue_next_ready() {
          Ok(queue_next_ready) => queue_next_ready,
          Err(err) => panic!("KV queue_next_ready failed: {err}"),
        };
        queue_dequeue_deadline = compute_deadline_with_max_and_jitter(queue_next_ready, QUEUE_DEQUEUE_FALLBACK_INTERVAL, QUEUE_DEQUEUE_JITTER);
      },
      req = request_rx.next() => {
        match req {
          Some(SqliteRequest::SnapshotRead { requests, options, sender }) => {
            let result = backend.snapshot_read(requests, options);
            sender.send(result).ok(); // ignore error if receiver is gone
          },
          Some(SqliteRequest::AtomicWrite { write, sender }) => {
            let mut batch_writes = vec![write];
            let mut batch_tx = vec![sender];

            if let Some(batch_timeout) = batch_timeout {
              let mut deadline = std::pin::pin!(tokio::time::sleep(batch_timeout));
              while let Either::Left((Some(x), _)) = futures::future::select(request_rx.as_mut().peek(), &mut deadline).await {
                if !matches!(x, SqliteRequest::AtomicWrite { .. }) {
                  break;
                }
                let x = request_rx.next().await.unwrap();
                match x {
                  SqliteRequest::AtomicWrite { write, sender } => {
                    batch_writes.push(write);
                    batch_tx.push(sender);
                  },
                  _ => unreachable!(),
                }
              }
            }

            let result = backend.atomic_write_batched(batch_writes);
            for (result, tx) in result.into_iter().zip(batch_tx.into_iter()) {
              tx.send(result).ok(); // ignore error if receiver is gone
            }
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

    let slot = if let Some(x) = self
      .workers
      .iter()
      .find_map(|x| x.request_tx.try_reserve().ok())
    {
      x
    } else {
      futures::future::select_all(
        self.workers.iter().map(|x| x.request_tx.reserve().boxed()),
      )
      .await
      .0
      .map_err(|_| SqliteBackendError::DatabaseClosed)?
    };
    slot.send(SqliteRequest::SnapshotRead {
      requests,
      options,
      sender,
    });
    let (ranges, _current_versionstamp) = receiver
      .await
      .map_err(|_| SqliteBackendError::DatabaseClosed)??;
    Ok(ranges)
  }

  pub async fn atomic_write(
    &self,
    write: AtomicWrite,
  ) -> Result<Option<CommitResult>, SqliteBackendError> {
    let (sender, receiver) = oneshot::channel();
    self.workers[0]
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
    if self.workers[0].request_tx.send(req).await.is_err() {
      return Ok(None);
    }
    let Ok(dequeued_message) = receiver.await else {
      return Ok(None);
    };
    Ok(Some(SqliteMessageHandle {
      id: dequeued_message.id,
      payload: Some(dequeued_message.payload),
      request_tx: self.workers[0].request_tx.clone(),
    }))
  }

  pub fn watch(
    &self,
    keys: Vec<Vec<u8>>,
  ) -> Pin<
    Box<dyn Stream<Item = Result<Vec<WatchKeyOutput>, anyhow::Error>> + Send>,
  > {
    let requests = keys
      .iter()
      .map(|key| ReadRange {
        end: key.iter().copied().chain(Some(0)).collect(),
        start: key.clone(),
        limit: NonZeroU32::new(1).unwrap(),
        reverse: false,
      })
      .collect::<Vec<_>>();
    let options = SnapshotReadOptions {
      consistency: denokv_proto::Consistency::Eventual,
    };
    let this = self.clone();
    let stream = try_stream! {
      let mut subscriptions: Vec<SqliteKeySubscription> = Vec::new();
      for key in keys {
        subscriptions.push(this.notifier.subscribe(key));
      }

      loop {
        let (sender, receiver) = oneshot::channel();
        let res = this
          .workers[0]
          .request_tx
          .send(SqliteRequest::SnapshotRead {
            requests: requests.clone(),
            options: options.clone(),
            sender,
          })
          .await;
        if res.is_err() {
          return; // database is closing
        }
        let Some(res) = receiver.await.ok() else {
          return; // database is closing
        };
        let (ranges, current_versionstamp) = res?;
        let mut outputs = Vec::new();
        for range in ranges {
          let entry = range.entries.into_iter().next();
          outputs.push(WatchKeyOutput::Changed { entry });
        }
        yield outputs;

        let futures = subscriptions.iter_mut().map(|subscription| {
          Box::pin(subscription.wait_until_updated(current_versionstamp))
        });
        if !futures::future::select_all(futures).await.0 {
          return; // database is closing
        }
      }
    };
    Box::pin(stream)
  }

  pub fn close(&self) {
    self.shutdown_notify.notify_waiters();
    for w in &self.workers {
      w.join_handle
        .lock()
        .unwrap()
        .take()
        .expect("can't close database twice")
        .join()
        .unwrap();
    }
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

  fn watch(
    &self,
    keys: Vec<Vec<u8>>,
  ) -> Pin<Box<dyn Stream<Item = Result<Vec<WatchKeyOutput>, anyhow::Error>>>>
  {
    Sqlite::watch(self, keys)
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
  pub async fn finish(&self, success: bool) -> Result<(), SqliteBackendError> {
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

  pub async fn take_payload(&mut self) -> Result<Vec<u8>, SqliteBackendError> {
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
