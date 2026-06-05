// Copyright 2023 the Deno authors. All rights reserved. MIT license.

//! Tests for the metadata refresh lifecycle: the background refresh
//! task must survive clones of `Remote` being dropped (e.g. by watch
//! streams), and the data path must recover from stale tokens by
//! forcing a refresh instead of surfacing an auth error.

use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use bytes::Bytes;
use chrono::Utc;
use deno_error::JsErrorBox;
use denokv_proto::datapath as pb;
use denokv_proto::Consistency;
use denokv_proto::Database;
use denokv_proto::ReadRange;
use denokv_proto::ReadRangeOutput;
use denokv_proto::SnapshotReadOptions;
use denokv_remote::MetadataEndpoint;
use denokv_remote::Remote;
use denokv_remote::RemoteResponse;
use denokv_remote::RemoteTransport;
use futures::Stream;
use http::HeaderMap;
use http::StatusCode;
use prost::Message;
use url::Url;

/// `chrono`'s "clock" feature is disabled in this workspace; this is
/// the same helper as `remote/time.rs`.
fn utc_now() -> chrono::DateTime<Utc> {
  let now = std::time::SystemTime::now()
    .duration_since(std::time::UNIX_EPOCH)
    .expect("system time before Unix epoch");
  let naive = chrono::NaiveDateTime::from_timestamp_opt(
    now.as_secs() as i64,
    now.subsec_nanos(),
  )
  .unwrap();
  chrono::DateTime::from_naive_utc_and_offset(naive, Utc)
}

struct MockState {
  metadata_fetches: u64,
  /// The data path accepts only `tok-{accepted_token}`; the metadata
  /// endpoint always issues that token. Bumping this value simulates
  /// outstanding tokens going stale server-side.
  accepted_token: u64,
}

#[derive(Clone)]
struct MockTransport(Arc<Mutex<MockState>>);

struct MockResponse(Bytes);

impl RemoteResponse for MockResponse {
  async fn bytes(self) -> Result<Bytes, JsErrorBox> {
    Ok(self.0)
  }
  async fn text(self) -> Result<String, JsErrorBox> {
    Ok(String::from_utf8(self.0.to_vec()).unwrap())
  }
  fn stream(
    self,
  ) -> impl Stream<Item = Result<Bytes, JsErrorBox>> + Send + Sync {
    futures::stream::iter([Ok(self.0)])
  }
}

impl RemoteTransport for MockTransport {
  type Response = MockResponse;
  async fn post(
    &self,
    url: Url,
    headers: HeaderMap,
    _body: Bytes,
  ) -> Result<(Url, StatusCode, Self::Response), JsErrorBox> {
    let mut state = self.0.lock().unwrap();
    if url.host_str() == Some("metadata.example") {
      state.metadata_fetches += 1;
      let body = serde_json::json!({
        "version": 3,
        "databaseId": "11111111-2222-3333-4444-555555555555",
        "endpoints": [{ "url": "http://data.example", "consistency": "strong" }],
        "token": format!("tok-{}", state.accepted_token),
        "expiresAt": (utc_now() + chrono::Duration::seconds(3600)).to_rfc3339(),
      });
      let response = MockResponse(Bytes::from(body.to_string()));
      return Ok((url, StatusCode::OK, response));
    }

    // Note: the double slash mirrors real KV Connect behavior; the
    // endpoint URL is normalized to have a trailing slash and the data
    // path call appends "/{method}".
    assert_eq!(url.as_str(), "http://data.example//snapshot_read");
    let authorization = headers
      .get("authorization")
      .and_then(|v| v.to_str().ok())
      .unwrap_or_default();
    if authorization != format!("Bearer tok-{}", state.accepted_token) {
      let body = Bytes::from_static(
        br#"{"code":"invalidAuthorizationHeader","message":"Invalid authorization header"}"#,
      );
      let response = MockResponse(body);
      return Ok((url, StatusCode::BAD_REQUEST, response));
    }

    let output = pb::SnapshotReadOutput {
      ranges: vec![pb::ReadRangeOutput { values: vec![] }],
      read_disabled: false,
      read_is_strongly_consistent: true,
      status: pb::SnapshotReadStatus::SrSuccess as i32,
    };
    let response = MockResponse(Bytes::from(output.encode_to_vec()));
    Ok((url, StatusCode::OK, response))
  }
}

#[derive(Clone)]
struct DummyPermissions;

impl denokv_remote::RemotePermissions for DummyPermissions {
  fn check_net_url(&self, _url: &Url) -> Result<(), JsErrorBox> {
    Ok(())
  }
}

fn setup() -> (
  Remote<DummyPermissions, MockTransport>,
  Arc<Mutex<MockState>>,
) {
  let state = Arc::new(Mutex::new(MockState {
    metadata_fetches: 0,
    accepted_token: 0,
  }));
  let metadata_endpoint = MetadataEndpoint {
    url: Url::parse("http://metadata.example/").unwrap(),
    access_token: "testtoken".to_string(),
  };
  let remote = Remote::new(
    MockTransport(state.clone()),
    DummyPermissions,
    metadata_endpoint,
  );
  (remote, state)
}

fn metadata_fetches(state: &Arc<Mutex<MockState>>) -> u64 {
  state.lock().unwrap().metadata_fetches
}

async fn snapshot_read(
  remote: &Remote<DummyPermissions, MockTransport>,
) -> Result<Vec<ReadRangeOutput>, JsErrorBox> {
  remote
    .snapshot_read(
      vec![ReadRange {
        start: vec![],
        end: vec![0xff],
        limit: NonZeroU32::new(1).unwrap(),
        reverse: false,
      }],
      SnapshotReadOptions {
        consistency: Consistency::Strong,
      },
    )
    .await
}

#[tokio::test(start_paused = true)]
async fn periodic_refresh_continues() {
  let (remote, state) = setup();
  snapshot_read(&remote).await.unwrap();
  assert_eq!(metadata_fetches(&state), 1);

  // The refresh task sleeps at most 60s between fetches.
  tokio::time::sleep(Duration::from_secs(61)).await;
  assert!(metadata_fetches(&state) >= 2);
}

#[tokio::test(start_paused = true)]
async fn dropped_clone_keeps_refresher_alive() {
  let (remote, state) = setup();
  snapshot_read(&remote).await.unwrap();
  assert_eq!(metadata_fetches(&state), 1);

  // Dropping a clone must not abort the shared metadata refresh task.
  #[allow(clippy::redundant_clone)]
  drop(remote.clone());

  // Invalidate the token held by the client; only a fresh metadata
  // fetch can make the next call succeed.
  state.lock().unwrap().accepted_token += 1;
  snapshot_read(&remote).await.unwrap();
  assert_eq!(metadata_fetches(&state), 2);
}

#[tokio::test(start_paused = true)]
async fn dropped_watch_stream_keeps_refresher_alive() {
  let (remote, state) = setup();
  snapshot_read(&remote).await.unwrap();
  assert_eq!(metadata_fetches(&state), 1);

  // The watch stream captures a clone of `Remote`; dropping it must
  // not abort the shared metadata refresh task.
  drop(remote.watch(vec![vec![1]]));

  state.lock().unwrap().accepted_token += 1;
  snapshot_read(&remote).await.unwrap();
  assert_eq!(metadata_fetches(&state), 2);
}

#[tokio::test(start_paused = true)]
async fn auth_error_forces_refresh_and_retry() {
  let (remote, state) = setup();
  snapshot_read(&remote).await.unwrap();
  assert_eq!(metadata_fetches(&state), 1);

  // Simulate server-side token expiry: the held token is no longer
  // accepted, but the metadata endpoint issues a valid replacement.
  // The call must transparently refresh and retry instead of
  // surfacing an auth error.
  state.lock().unwrap().accepted_token += 1;
  snapshot_read(&remote).await.unwrap();
  assert_eq!(metadata_fetches(&state), 2);
}
