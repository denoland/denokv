// Copyright 2023 the Deno authors. All rights reserved. MIT license.

//! Multiplexed watches over a single WebSocket connection (KV Connect
//! protocol version 4).
//!
//! One [`WatchChannelManager`] exists per database. All `watch()` calls
//! subscribe through it; the manager maintains a single WebSocket channel to
//! the server, registers each watched key once (refcounted across
//! subscriptions), and demultiplexes key-tagged updates back to the
//! subscriptions. When the channel is lost it reconnects with backoff and
//! re-adds every key with the last seen versionstamp as the baseline, so
//! reconnects deliver neither duplicates nor lose updates.
//!
//! When the channel cannot be made to work — the WebSocket handshake fails
//! repeatedly (e.g. an intermediary strips the `Upgrade` header), the
//! server keeps closing the connection, or metadata rotates to an endpoint
//! the caller has not approved — subscriptions are failed with an error
//! starting with [`CHANNEL_UNAVAILABLE_PREFIX`]. `Remote::watch` reacts to
//! that marker by falling back to the protocol version 3 per-watch
//! streaming path, which works over plain HTTP and re-checks permissions
//! per request.

use std::collections::HashMap;
use std::collections::HashSet;
use std::pin::pin;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use async_stream::try_stream;
use deno_error::JsErrorBox;
use denokv_proto::datapath as pb;
use denokv_proto::decode_value;
use denokv_proto::KvEntry;
use denokv_proto::WatchKeyOutput;
use futures::SinkExt;
use futures::Stream;
use futures::StreamExt;
use log::debug;
use prost::Message;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::sync::Notify;
use tokio_util::task::AbortOnDropHandle;
use url::Url;

use crate::randomized_exponential_backoff;
use crate::DataPathConsistency;
use crate::MetadataState;
use crate::RemoteTransport;
use crate::WatchError;
use crate::DATAPATH_BACKOFF_BASE;

/// Errors with this prefix tell `Remote::watch` that the watch channel is
/// not usable and it should fall back to per-watch streaming.
pub(crate) const CHANNEL_UNAVAILABLE_PREFIX: &str =
  "watch channel unavailable: ";

/// A connection that stayed up at least this long is considered healthy:
/// when it dies, the failure counter is reset so routine disconnects (idle
/// timeouts of intermediaries, server restarts) do not accumulate, while
/// connections that fail to establish or die quickly do.
const HEALTHY_CONNECTION_DURATION: Duration = Duration::from_secs(30);

/// After this many consecutive failed or short-lived connections the
/// channel is declared unavailable and all subscriptions are failed (which
/// makes their watch() calls fall back to per-watch streaming).
const MAX_CONSECUTIVE_FAILURES: u64 = 5;

type SubscriptionId = u64;

/// What the client knows the server has last told it about a key.
#[derive(Clone, PartialEq)]
enum Baseline {
  /// No state received yet.
  Unknown,
  /// The key was last seen as not present.
  Absent,
  /// The key was last seen with this versionstamp.
  Versionstamp(Vec<u8>),
}

impl Baseline {
  fn to_add_message(&self, key: &[u8]) -> pb::WatchChannelClientMessage {
    pb::WatchChannelClientMessage {
      message: Some(pb::watch_channel_client_message::Message::Add(
        pb::WatchChannelAdd {
          key: key.to_vec(),
          baseline: match self {
            Baseline::Unknown => None,
            Baseline::Absent => {
              Some(pb::watch_channel_add::Baseline::Absent(true))
            }
            Baseline::Versionstamp(versionstamp) => {
              Some(pb::watch_channel_add::Baseline::Versionstamp(
                versionstamp.clone(),
              ))
            }
          },
        },
      )),
    }
  }
}

fn remove_message(key: &[u8]) -> pb::WatchChannelClientMessage {
  pb::WatchChannelClientMessage {
    message: Some(pb::watch_channel_client_message::Message::Remove(
      pb::WatchChannelRemove { key: key.to_vec() },
    )),
  }
}

enum Command {
  Subscribe {
    id: SubscriptionId,
    keys: Vec<Vec<u8>>,
    tx: mpsc::UnboundedSender<SubscriptionEvent>,
  },
  Unsubscribe {
    id: SubscriptionId,
  },
}

enum SubscriptionEvent {
  /// New state for some of the subscription's keys. Entries are shared
  /// between subscriptions; they are only copied out when converted to the
  /// caller-facing [`WatchKeyOutput`].
  Update(Vec<(Vec<u8>, Option<Arc<pb::KvEntry>>)>),
  /// The channel failed; the subscription stream must error out.
  Error(String),
}

struct KeyState {
  baseline: Baseline,
  subscribers: HashSet<SubscriptionId>,
}

struct SubscriptionState {
  keys: Vec<Vec<u8>>,
  tx: mpsc::UnboundedSender<SubscriptionEvent>,
}

pub(crate) struct WatchChannelManager {
  commands: mpsc::UnboundedSender<Command>,
  next_subscription_id: AtomicU64,
  /// Endpoint URLs (in their original `http`/`https` form) that have
  /// passed the caller's permission check. The driver refuses to connect
  /// anywhere else; see [`Driver::run_connection`].
  approved_endpoints: Arc<Mutex<HashSet<String>>>,
  _driver: AbortOnDropHandle<()>,
}

impl WatchChannelManager {
  pub fn new<T: RemoteTransport>(
    client: T,
    metadata: watch::Receiver<MetadataState>,
    refresh_metadata: Arc<Notify>,
  ) -> Self {
    let (commands_tx, commands_rx) = mpsc::unbounded_channel();
    let approved_endpoints = Arc::new(Mutex::new(HashSet::new()));
    let driver = Driver {
      client,
      metadata,
      refresh_metadata,
      approved_endpoints: approved_endpoints.clone(),
      keys: HashMap::new(),
      subscriptions: HashMap::new(),
    };
    let handle = tokio::spawn(driver.run(commands_rx));
    Self {
      commands: commands_tx,
      next_subscription_id: AtomicU64::new(1),
      approved_endpoints,
      _driver: AbortOnDropHandle::new(handle),
    }
  }

  /// Record endpoint URLs that have passed the caller's permission check.
  pub fn approve_endpoints(&self, urls: impl IntoIterator<Item = String>) {
    self.approved_endpoints.lock().unwrap().extend(urls);
  }

  /// Watch the given keys through the shared channel. The returned stream
  /// has the same contract as the v3 watch stream: the first item carries
  /// the state of every key (all `Changed`), subsequent items mark updated
  /// keys `Changed` and all others `Unchanged`, in the order of `keys`.
  pub fn subscribe(
    &self,
    keys: Vec<Vec<u8>>,
  ) -> impl Stream<Item = Result<Vec<WatchKeyOutput>, JsErrorBox>> + Send {
    let id = self.next_subscription_id.fetch_add(1, Ordering::Relaxed);
    let (tx, mut rx) = mpsc::unbounded_channel();
    let _ = self.commands.send(Command::Subscribe {
      id,
      keys: keys.clone(),
      tx,
    });
    let unsubscribe = UnsubscribeOnDrop {
      id,
      commands: self.commands.clone(),
    };

    // Indices per key, so duplicate keys in one watch call all get updated.
    let mut key_indices: HashMap<Vec<u8>, Vec<usize>> = HashMap::new();
    for (index, key) in keys.iter().enumerate() {
      key_indices.entry(key.clone()).or_default().push(index);
    }

    try_stream! {
      let _unsubscribe = unsubscribe;
      // Latest known state per key position; outer None until first seen.
      let mut latest: Vec<Option<Option<Arc<pb::KvEntry>>>> =
        vec![None; keys.len()];
      let mut emitted_initial = false;
      while let Some(event) = rx.recv().await {
        let items = match event {
          SubscriptionEvent::Update(items) => items,
          SubscriptionEvent::Error(error) => {
            Err(JsErrorBox::generic(error))?;
            unreachable!();
          }
        };
        let mut touched = vec![false; keys.len()];
        for (key, entry) in items {
          if let Some(indices) = key_indices.get(&key) {
            for &index in indices {
              latest[index] = Some(entry.clone());
              touched[index] = true;
            }
          }
        }
        if !emitted_initial {
          // Hold the first item back until every key has reported once, so
          // the first item is a full snapshot like in protocol version 3.
          if latest.iter().any(|state| state.is_none()) {
            continue;
          }
          emitted_initial = true;
          let outputs = latest
            .iter()
            .map(|state| convert_entry(state.as_ref().unwrap().as_deref()))
            .collect::<Result<Vec<_>, JsErrorBox>>()?;
          yield outputs;
        } else {
          let outputs = (0..keys.len())
            .map(|index| {
              if touched[index] {
                convert_entry(latest[index].as_ref().unwrap().as_deref())
              } else {
                Ok(WatchKeyOutput::Unchanged)
              }
            })
            .collect::<Result<Vec<_>, JsErrorBox>>()?;
          yield outputs;
        }
      }
      // The command channel outlives all subscriptions; the event channel
      // only closes when the manager (and thus the database) is dropped.
    }
  }
}

struct UnsubscribeOnDrop {
  id: SubscriptionId,
  commands: mpsc::UnboundedSender<Command>,
}

impl Drop for UnsubscribeOnDrop {
  fn drop(&mut self) {
    let _ = self.commands.send(Command::Unsubscribe { id: self.id });
  }
}

/// Convert a protobuf entry (`None` = key not present) to the
/// caller-facing output. Also used by the protocol version 3 watch path in
/// `lib.rs`, so both paths decode entries identically.
pub(crate) fn convert_entry(
  entry: Option<&pb::KvEntry>,
) -> Result<WatchKeyOutput, JsErrorBox> {
  let entry = match entry {
    Some(entry) => {
      let value = decode_value(entry.value.clone(), entry.encoding as i64)
        .ok_or_else(|| {
          JsErrorBox::from_err(WatchError::UnknownEncoding(entry.encoding))
        })?;
      Some(KvEntry {
        key: entry.key.clone(),
        value,
        versionstamp: <[u8; 10]>::try_from(&entry.versionstamp[..])
          .map_err(|e| JsErrorBox::from_err(WatchError::TryFromSlice(e)))?,
      })
    }
    None => None,
  };
  Ok(WatchKeyOutput::Changed { entry })
}

struct Driver<T: RemoteTransport> {
  client: T,
  metadata: watch::Receiver<MetadataState>,
  refresh_metadata: Arc<Notify>,
  approved_endpoints: Arc<Mutex<HashSet<String>>>,
  keys: HashMap<Vec<u8>, KeyState>,
  subscriptions: HashMap<SubscriptionId, SubscriptionState>,
}

enum ConnectionEnd {
  /// All command senders are gone; the manager was dropped.
  Shutdown,
  /// The connection was lost; reconnect.
  Reconnect,
  /// The server violated the protocol; reconnect, but always count it as a
  /// failure so a repeating poison frame cannot loop without backoff (the
  /// connection may have been long-lived before the bad frame arrived).
  ProtocolError,
  /// There is nothing (left) to watch, or the failure was already
  /// reported via fail_all; wait for new subscriptions.
  Idle,
}

impl<T: RemoteTransport> Driver<T> {
  async fn run(mut self, mut commands: mpsc::UnboundedReceiver<Command>) {
    let mut consecutive_failures: u64 = 0;
    loop {
      // With nothing to watch there is nothing to connect for.
      while self.keys.is_empty() {
        match commands.recv().await {
          Some(command) => self.handle_command_offline(command),
          None => return,
        }
      }

      if consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
        // The channel does not work; tell every subscription so their
        // watch() calls fall back to per-watch streaming. New
        // subscriptions get a fresh chance.
        self.fail_all(format!(
          "{CHANNEL_UNAVAILABLE_PREFIX}giving up after {consecutive_failures} consecutive failed connections"
        ));
        consecutive_failures = 0;
        continue;
      }

      if consecutive_failures > 0 {
        randomized_exponential_backoff(
          DATAPATH_BACKOFF_BASE,
          consecutive_failures,
        )
        .await;
      }

      let connected_at = Instant::now();
      match self.run_connection(&mut commands).await {
        ConnectionEnd::Shutdown => return,
        ConnectionEnd::Idle => {
          consecutive_failures = 0;
        }
        ConnectionEnd::Reconnect => {
          if connected_at.elapsed() >= HEALTHY_CONNECTION_DURATION {
            consecutive_failures = 0;
          } else {
            consecutive_failures += 1;
          }
        }
        ConnectionEnd::ProtocolError => {
          consecutive_failures += 1;
        }
      }
    }
  }

  /// Register a subscription. Returns the keys that must be (re-)announced
  /// to the server. Baselines for the subscription's keys are reset to
  /// `Unknown` — even for keys that were already watched — so that the
  /// state of every key is re-sent: the new subscription needs it for its
  /// initial snapshot, and the reset (rather than only a one-shot
  /// add-without-baseline message) guarantees the request survives a
  /// reconnect that swallows the in-flight add.
  fn register_subscription(
    &mut self,
    id: SubscriptionId,
    keys: Vec<Vec<u8>>,
    tx: mpsc::UnboundedSender<SubscriptionEvent>,
  ) -> Vec<Vec<u8>> {
    for key in &keys {
      let state = self.keys.entry(key.clone()).or_insert(KeyState {
        baseline: Baseline::Unknown,
        subscribers: HashSet::new(),
      });
      state.subscribers.insert(id);
      state.baseline = Baseline::Unknown;
    }
    let announce = keys.clone();
    self
      .subscriptions
      .insert(id, SubscriptionState { keys, tx });
    announce
  }

  /// Unregister a subscription. Returns the keys that are no longer
  /// watched by anyone.
  fn unregister_subscription(&mut self, id: SubscriptionId) -> Vec<Vec<u8>> {
    let Some(subscription) = self.subscriptions.remove(&id) else {
      return Vec::new();
    };
    let mut removed = Vec::new();
    for key in subscription.keys {
      if let Some(state) = self.keys.get_mut(&key) {
        state.subscribers.remove(&id);
        if state.subscribers.is_empty() {
          self.keys.remove(&key);
          removed.push(key);
        }
      }
    }
    removed
  }

  /// Apply a command without a live connection; adds and removes are
  /// reconciled when the next connection is established.
  fn handle_command_offline(&mut self, command: Command) {
    match command {
      Command::Subscribe { id, keys, tx } => {
        self.register_subscription(id, keys, tx);
      }
      Command::Unsubscribe { id } => {
        self.unregister_subscription(id);
      }
    }
  }

  /// Establish one connection and pump it until it dies, goes idle, or the
  /// manager is dropped.
  async fn run_connection(
    &mut self,
    commands: &mut mpsc::UnboundedReceiver<Command>,
  ) -> ConnectionEnd {
    // Wait for usable metadata.
    let metadata = {
      let mut metadata_rx = self.metadata.clone();
      loop {
        match &*metadata_rx.borrow_and_update() {
          MetadataState::Pending => {}
          MetadataState::Ok(metadata) => break metadata.clone(),
          MetadataState::Error(error) => {
            self.fail_all(format!(
              "{CHANNEL_UNAVAILABLE_PREFIX}metadata error: {error}"
            ));
            return ConnectionEnd::Idle;
          }
        }
        if metadata_rx.changed().await.is_err() {
          return ConnectionEnd::Shutdown;
        }
      }
    };

    let Some(endpoint) = metadata
      .endpoints
      .iter()
      .find(|endpoint| endpoint.consistency == DataPathConsistency::Strong)
    else {
      self.fail_all(format!(
        "{CHANNEL_UNAVAILABLE_PREFIX}no strong consistency endpoints available"
      ));
      return ConnectionEnd::Idle;
    };

    let http_url = match Url::parse(&format!("{}/watch_channel", endpoint.url))
    {
      Ok(url) => url,
      Err(error) => {
        self.fail_all(format!(
          "{CHANNEL_UNAVAILABLE_PREFIX}invalid endpoint URL: {error}"
        ));
        return ConnectionEnd::Idle;
      }
    };
    // Only connect to endpoints the caller permission-checked in watch().
    // Metadata refreshes can introduce new endpoints; those have not been
    // checked, so the channel is declared unavailable and watch() falls
    // back to the per-watch path, which checks permissions on every
    // request.
    if !self
      .approved_endpoints
      .lock()
      .unwrap()
      .contains(http_url.as_str())
    {
      self.fail_all(format!(
        "{CHANNEL_UNAVAILABLE_PREFIX}endpoint '{http_url}' has not been permission-checked"
      ));
      return ConnectionEnd::Idle;
    }
    // http -> ws, https -> wss.
    let ws_url = match Url::parse(&format!(
      "ws{}",
      &http_url.as_str()[4..] // strip "http"
    )) {
      Ok(url) => url,
      Err(error) => {
        self.fail_all(format!(
          "{CHANNEL_UNAVAILABLE_PREFIX}invalid websocket URL: {error}"
        ));
        return ConnectionEnd::Idle;
      }
    };

    let (mut sink, stream) = match self
      .client
      .websocket(ws_url.clone(), metadata.headers())
      .await
    {
      Ok(connection) => connection,
      Err(error) => {
        debug!(
          "KV Connect watch channel connect to '{ws_url}' failed: {error}"
        );
        return ConnectionEnd::Reconnect;
      }
    };
    let mut stream = pin!(stream);

    // Register every key, resuming from its baseline.
    for (key, state) in &self.keys {
      let message = state.baseline.to_add_message(key);
      if let Err(error) = sink.send(message.encode_to_vec().into()).await {
        debug!("KV Connect watch channel send failed: {error}");
        return ConnectionEnd::Reconnect;
      }
    }

    loop {
      tokio::select! {
        command = commands.recv() => {
          match command {
            Some(command) => {
              if self.handle_command_online(command, &mut sink).await.is_err() {
                return ConnectionEnd::Reconnect;
              }
              if self.keys.is_empty() {
                // Nothing left to watch; drop the connection rather than
                // keeping an idle socket open.
                return ConnectionEnd::Idle;
              }
            }
            None => return ConnectionEnd::Shutdown,
          }
        }
        message = stream.next() => {
          match message {
            Some(Ok(bytes)) => {
              match self.handle_server_message(&bytes) {
                ServerMessageOutcome::Continue => {}
                ServerMessageOutcome::Reconnect => return ConnectionEnd::Reconnect,
                ServerMessageOutcome::ProtocolError => {
                  return ConnectionEnd::ProtocolError
                }
              }
            }
            Some(Err(error)) => {
              debug!("KV Connect watch channel error: {error}");
              return ConnectionEnd::Reconnect;
            }
            None => {
              debug!("KV Connect watch channel closed");
              return ConnectionEnd::Reconnect;
            }
          }
        }
      }
    }
  }

  /// Apply a command while connected, sending the corresponding add/remove
  /// messages. Returns Err(()) when the connection broke.
  async fn handle_command_online(
    &mut self,
    command: Command,
    sink: &mut crate::WebSocketMessageSink,
  ) -> Result<(), ()> {
    match command {
      Command::Subscribe { id, keys, tx } => {
        for key in self.register_subscription(id, keys, tx) {
          // The baseline was reset to Unknown, so the server re-sends the
          // current state, which the new subscription needs for its
          // initial snapshot. Existing subscriptions receive a redundant
          // (but harmless) update.
          let message = Baseline::Unknown.to_add_message(&key);
          if let Err(error) = sink.send(message.encode_to_vec().into()).await {
            debug!("KV Connect watch channel send failed: {error}");
            return Err(());
          }
        }
      }
      Command::Unsubscribe { id } => {
        for key in self.unregister_subscription(id) {
          if let Err(error) =
            sink.send(remove_message(&key).encode_to_vec().into()).await
          {
            debug!("KV Connect watch channel send failed: {error}");
            return Err(());
          }
        }
      }
    }
    Ok(())
  }

  fn handle_server_message(&mut self, bytes: &[u8]) -> ServerMessageOutcome {
    let message = match pb::WatchChannelServerMessage::decode(bytes) {
      Ok(message) => message,
      Err(error) => {
        // A malformed frame is a server bug, but baselines are intact, so
        // a reconnect is lossless. Counted as a failure regardless of
        // connection age so persistent garbage eventually declares the
        // channel unavailable.
        debug!("KV Connect watch channel sent an invalid message: {error}");
        return ServerMessageOutcome::ProtocolError;
      }
    };
    let Some(pb::watch_channel_server_message::Message::Output(output)) =
      message.message
    else {
      // Unknown message type from a newer server; ignore.
      return ServerMessageOutcome::Continue;
    };

    match output.status() {
      pb::SnapshotReadStatus::SrSuccess => {}
      pb::SnapshotReadStatus::SrReadDisabled => {
        // Endpoints changed; get fresh metadata and reconnect.
        self.refresh_metadata.notify_one();
        return ServerMessageOutcome::Reconnect;
      }
      pb::SnapshotReadStatus::SrUnspecified => {
        self.fail_all(format!(
          "KV Connect watch channel read error (code={})",
          output.status
        ));
        return ServerMessageOutcome::Reconnect;
      }
    }

    // Update baselines and fan updates out per subscription. Entries are
    // wrapped in Arc once so fan-out does not copy values.
    type Updates = Vec<(Vec<u8>, Option<Arc<pb::KvEntry>>)>;
    let mut per_subscription: HashMap<SubscriptionId, Updates> = HashMap::new();
    for key_output in output.keys {
      let Some(state) = self.keys.get_mut(&key_output.key) else {
        // Update for a key we no longer watch (remove still in flight).
        continue;
      };
      state.baseline = match &key_output.entry {
        Some(entry) => Baseline::Versionstamp(entry.versionstamp.clone()),
        None => Baseline::Absent,
      };
      let entry = key_output.entry.map(Arc::new);
      for id in &state.subscribers {
        per_subscription
          .entry(*id)
          .or_default()
          .push((key_output.key.clone(), entry.clone()));
      }
    }
    for (id, items) in per_subscription {
      if let Some(subscription) = self.subscriptions.get(&id) {
        // A send failure means the subscription stream was dropped; its
        // Unsubscribe command is already queued and will clean up.
        let _ = subscription.tx.send(SubscriptionEvent::Update(items));
      }
    }
    ServerMessageOutcome::Continue
  }

  /// Report an error to all subscriptions and drop all state. New
  /// subscriptions start with a clean slate.
  fn fail_all(&mut self, error: String) {
    for subscription in self.subscriptions.values() {
      let _ = subscription
        .tx
        .send(SubscriptionEvent::Error(error.clone()));
    }
    self.subscriptions.clear();
    self.keys.clear();
  }
}

enum ServerMessageOutcome {
  Continue,
  Reconnect,
  ProtocolError,
}
