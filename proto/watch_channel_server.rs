// Copyright 2023 the Deno authors. All rights reserved. MIT license.

//! Server-side state machine for the KV Connect watch channel (protocol
//! version 4). See `kv-connect.md`, "Watch Channel (version 4)".
//!
//! The state machine is transport- and backend-agnostic: it decodes client
//! messages, tracks what the connected client is known to have seen per
//! key, and diffs backend-observed key states against that knowledge so
//! only genuinely new state is sent. The caller owns the WebSocket and the
//! backend watcher; this type owns nothing but per-connection bookkeeping,
//! and is discarded with the connection (the server keeps no watch state
//! across connections).

use std::collections::BTreeMap;

use prost::Message;

use crate::datapath as pb;

/// What the connected client is known to have last seen for a key.
#[derive(Clone, PartialEq)]
enum KnownState {
  /// Nothing delivered yet (and no baseline supplied).
  Unknown,
  /// Last seen as not present.
  Absent,
  /// Last seen with this versionstamp.
  Versionstamp(Vec<u8>),
}

/// The effect of one client message on the channel.
#[derive(Debug, PartialEq)]
pub enum ClientMessageEffect {
  /// A key was added (or re-added); the backend watcher must be updated
  /// and the key's current state must be (re-)evaluated.
  KeyAdded(Vec<u8>),
  /// No change to the watched key set.
  None,
  /// The message violates the protocol; the connection must be closed
  /// (WebSocket close code 1008) with this reason.
  Reject(&'static str),
}

/// Per-connection watch channel state.
pub struct WatchChannelServer {
  known: BTreeMap<Vec<u8>, KnownState>,
  max_keys: usize,
  max_key_size: usize,
}

impl WatchChannelServer {
  pub fn new(max_keys: usize) -> Self {
    Self {
      known: BTreeMap::new(),
      max_keys,
      // Watchable keys are writable keys; this also keeps successor-key
      // range reads (key + 0x00) within MAX_READ_KEY_SIZE_BYTES.
      max_key_size: crate::limits::MAX_WRITE_KEY_SIZE_BYTES,
    }
  }

  /// Apply one binary client message (a `WatchChannelClientMessage`).
  pub fn apply_client_message(&mut self, bytes: &[u8]) -> ClientMessageEffect {
    let Ok(message) = pb::WatchChannelClientMessage::decode(bytes) else {
      return ClientMessageEffect::Reject("invalid message");
    };
    match message.message {
      Some(pb::watch_channel_client_message::Message::Add(add)) => {
        if add.key.len() > self.max_key_size {
          return ClientMessageEffect::Reject("key too large");
        }
        if !self.known.contains_key(&add.key)
          && self.known.len() >= self.max_keys
        {
          return ClientMessageEffect::Reject("too many watched keys");
        }
        let baseline = match add.baseline {
          Some(pb::watch_channel_add::Baseline::Versionstamp(v)) => {
            KnownState::Versionstamp(v)
          }
          Some(pb::watch_channel_add::Baseline::Absent(true)) => {
            KnownState::Absent
          }
          _ => KnownState::Unknown,
        };
        self.known.insert(add.key.clone(), baseline);
        ClientMessageEffect::KeyAdded(add.key)
      }
      Some(pb::watch_channel_client_message::Message::Remove(remove)) => {
        // The key set shrank, but the backend watcher does not need to be
        // updated for correctness: `diff` ignores keys that are no longer
        // watched.
        self.known.remove(&remove.key);
        ClientMessageEffect::None
      }
      // A message from a newer protocol revision; ignore.
      None => ClientMessageEffect::None,
    }
  }

  /// The currently watched keys, in sorted order.
  pub fn watched_keys(&self) -> Vec<Vec<u8>> {
    self.known.keys().cloned().collect()
  }

  pub fn key_count(&self) -> usize {
    self.known.len()
  }

  /// Diff observed key states against what the client has seen and return
  /// the encoded `WatchChannelServerMessage` to send, if anything changed.
  /// `states` items are (key, current entry; `None` = key not present).
  /// Keys that are not (or no longer) watched are ignored.
  pub fn diff(
    &mut self,
    states: impl IntoIterator<Item = (Vec<u8>, Option<pb::KvEntry>)>,
  ) -> Option<Vec<u8>> {
    let mut changed = Vec::new();
    for (key, entry) in states {
      let Some(state) = self.known.get_mut(&key) else {
        continue;
      };
      let new_state = match &entry {
        Some(entry) => KnownState::Versionstamp(entry.versionstamp.clone()),
        None => KnownState::Absent,
      };
      if *state == new_state {
        continue;
      }
      *state = new_state;
      changed.push(pb::WatchChannelKeyOutput { key, entry });
    }
    if changed.is_empty() {
      return None;
    }
    let message = pb::WatchChannelServerMessage {
      message: Some(pb::watch_channel_server_message::Message::Output(
        pb::WatchChannelOutput {
          status: pb::SnapshotReadStatus::SrSuccess as i32,
          keys: changed,
        },
      )),
    };
    Some(message.encode_to_vec())
  }

  /// Encode a server message that carries only a status (no key outputs),
  /// e.g. to tell the client that reads are disabled and it should refresh
  /// its metadata. Provided here so servers do not need a matching prost
  /// version to construct protocol messages.
  pub fn encode_status_only_message(status: i32) -> Vec<u8> {
    pb::WatchChannelServerMessage {
      message: Some(pb::watch_channel_server_message::Message::Output(
        pb::WatchChannelOutput {
          status,
          keys: Vec::new(),
        },
      )),
    }
    .encode_to_vec()
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn add(
    key: &[u8],
    baseline: Option<pb::watch_channel_add::Baseline>,
  ) -> Vec<u8> {
    pb::WatchChannelClientMessage {
      message: Some(pb::watch_channel_client_message::Message::Add(
        pb::WatchChannelAdd {
          key: key.to_vec(),
          baseline,
        },
      )),
    }
    .encode_to_vec()
  }

  fn remove(key: &[u8]) -> Vec<u8> {
    pb::WatchChannelClientMessage {
      message: Some(pb::watch_channel_client_message::Message::Remove(
        pb::WatchChannelRemove { key: key.to_vec() },
      )),
    }
    .encode_to_vec()
  }

  fn entry(versionstamp: &[u8]) -> pb::KvEntry {
    pb::KvEntry {
      key: b"k".to_vec(),
      value: b"v".to_vec(),
      encoding: 0,
      versionstamp: versionstamp.to_vec(),
    }
  }

  fn decoded_keys(message: &[u8]) -> Vec<(Vec<u8>, Option<pb::KvEntry>)> {
    let message = pb::WatchChannelServerMessage::decode(message).unwrap();
    let Some(pb::watch_channel_server_message::Message::Output(output)) =
      message.message
    else {
      panic!("expected output message");
    };
    assert_eq!(output.status, pb::SnapshotReadStatus::SrSuccess as i32);
    output.keys.into_iter().map(|k| (k.key, k.entry)).collect()
  }

  #[test]
  fn add_without_baseline_sends_current_state() {
    let mut server = WatchChannelServer::new(16);
    assert_eq!(
      server.apply_client_message(&add(b"a", None)),
      ClientMessageEffect::KeyAdded(b"a".to_vec())
    );
    // Present key.
    let message = server
      .diff(vec![(b"a".to_vec(), Some(entry(b"v1")))])
      .expect("state must be sent");
    assert_eq!(decoded_keys(&message).len(), 1);
    // Re-observing the same state sends nothing.
    assert!(server
      .diff(vec![(b"a".to_vec(), Some(entry(b"v1")))])
      .is_none());
  }

  #[test]
  fn add_without_baseline_reports_absent_keys() {
    let mut server = WatchChannelServer::new(16);
    server.apply_client_message(&add(b"a", None));
    let message = server
      .diff(vec![(b"a".to_vec(), None)])
      .expect("absence must be reported");
    let keys = decoded_keys(&message);
    assert_eq!(keys[0].0, b"a".to_vec());
    assert!(keys[0].1.is_none());
    assert!(server.diff(vec![(b"a".to_vec(), None)]).is_none());
  }

  #[test]
  fn matching_baseline_suppresses_initial_state() {
    let mut server = WatchChannelServer::new(16);
    server.apply_client_message(&add(
      b"a",
      Some(pb::watch_channel_add::Baseline::Versionstamp(
        b"v1".to_vec(),
      )),
    ));
    assert!(server
      .diff(vec![(b"a".to_vec(), Some(entry(b"v1")))])
      .is_none());
    // A different versionstamp is sent.
    assert!(server
      .diff(vec![(b"a".to_vec(), Some(entry(b"v2")))])
      .is_some());
  }

  #[test]
  fn absent_baseline_suppresses_absence() {
    let mut server = WatchChannelServer::new(16);
    server.apply_client_message(&add(
      b"a",
      Some(pb::watch_channel_add::Baseline::Absent(true)),
    ));
    assert!(server.diff(vec![(b"a".to_vec(), None)]).is_none());
    assert!(server
      .diff(vec![(b"a".to_vec(), Some(entry(b"v1")))])
      .is_some());
  }

  #[test]
  fn re_add_resets_the_baseline() {
    let mut server = WatchChannelServer::new(16);
    server.apply_client_message(&add(b"a", None));
    assert!(server
      .diff(vec![(b"a".to_vec(), Some(entry(b"v1")))])
      .is_some());
    // Re-add without baseline: current state must be re-sent.
    server.apply_client_message(&add(b"a", None));
    assert!(server
      .diff(vec![(b"a".to_vec(), Some(entry(b"v1")))])
      .is_some());
  }

  #[test]
  fn removed_keys_are_ignored() {
    let mut server = WatchChannelServer::new(16);
    server.apply_client_message(&add(b"a", None));
    assert_eq!(
      server.apply_client_message(&remove(b"a")),
      ClientMessageEffect::None
    );
    assert!(server
      .diff(vec![(b"a".to_vec(), Some(entry(b"v1")))])
      .is_none());
    assert_eq!(server.key_count(), 0);
  }

  #[test]
  fn key_cap_and_key_size_are_enforced() {
    let mut server = WatchChannelServer::new(1);
    server.apply_client_message(&add(b"a", None));
    assert_eq!(
      server.apply_client_message(&add(b"b", None)),
      ClientMessageEffect::Reject("too many watched keys")
    );
    // Re-adding an existing key is not a cap violation.
    assert_eq!(
      server.apply_client_message(&add(b"a", None)),
      ClientMessageEffect::KeyAdded(b"a".to_vec())
    );
    let huge = vec![0u8; crate::limits::MAX_WRITE_KEY_SIZE_BYTES + 1];
    assert_eq!(
      server.apply_client_message(&add(&huge, None)),
      ClientMessageEffect::Reject("key too large")
    );
  }

  #[test]
  fn garbage_is_rejected() {
    let mut server = WatchChannelServer::new(16);
    assert_eq!(
      server.apply_client_message(b"\xff\xff\xff"),
      ClientMessageEffect::Reject("invalid message")
    );
  }
}
