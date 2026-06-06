// Copyright 2023 the Deno authors. All rights reserved. MIT license.

mod time;

use std::io;
use std::ops::Sub;
use std::pin::pin;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_stream::try_stream;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::DateTime;
use chrono::Utc;
use deno_error::JsError;
use deno_error::JsErrorBox;
use denokv_proto::decode_value;
use denokv_proto::AtomicWrite;
use denokv_proto::CommitResult;
use denokv_proto::Consistency;
use denokv_proto::Database;
use denokv_proto::DatabaseMetadata;
use denokv_proto::KvEntry;
use denokv_proto::KvValue;
use denokv_proto::MetadataExchangeRequest;
use denokv_proto::QueueMessageHandle;
use denokv_proto::ReadRange;
use denokv_proto::ReadRangeOutput;
use denokv_proto::SnapshotReadOptions;
use denokv_proto::WatchKeyOutput;
use futures::Future;
use futures::Stream;
use futures::StreamExt;
use futures::TryStreamExt;
use http::HeaderMap;
use http::HeaderValue;
use http::StatusCode;
use log::debug;
use log::error;
use log::warn;
use prost::Message;
use rand::Rng;
use serde::Deserialize;
use thiserror::Error;
use time::utc_now;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::codec::LengthDelimitedCodec;
use tokio_util::io::StreamReader;
use url::Url;
use uuid::Uuid;

use denokv_proto::datapath as pb;

const DATAPATH_BACKOFF_BASE: Duration = Duration::from_millis(200);
const METADATA_BACKOFF_BASE: Duration = Duration::from_secs(5);

pub struct MetadataEndpoint {
  pub url: Url,
  pub access_token: String,
}

impl MetadataEndpoint {
  pub fn headers(&self) -> HeaderMap {
    let mut headers = HeaderMap::with_capacity(2);
    headers.insert(
      "authorization",
      format!("Bearer {}", self.access_token).try_into().unwrap(),
    );
    headers.insert("content-type", "application/json".try_into().unwrap());
    headers
  }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ProtocolVersion {
  V1,
  V2,
  V3,
}

#[derive(PartialEq, Eq)]
enum DataPathConsistency {
  Strong,
  Eventual,
}

struct DataPathEndpoint {
  url: Url,
  consistency: DataPathConsistency,
}

struct Metadata {
  version: ProtocolVersion,
  database_id: Uuid,
  endpoints: Vec<DataPathEndpoint>,
  token: String,
  expires_at: DateTime<Utc>,
}

impl Metadata {
  pub fn headers(&self) -> HeaderMap {
    let mut headers = HeaderMap::with_capacity(3);
    headers.insert(
      "authorization",
      format!("Bearer {}", self.token).try_into().unwrap(),
    );
    match self.version {
      ProtocolVersion::V1 => {
        headers.insert(
          "x-transaction-domain-id",
          self.database_id.to_string().try_into().unwrap(),
        );
      }
      ProtocolVersion::V2 => {
        headers.insert(
          "x-denokv-database-id",
          self.database_id.to_string().try_into().unwrap(),
        );
        headers.insert("x-denokv-version", HeaderValue::from_static("2"));
      }
      ProtocolVersion::V3 => {
        headers.insert(
          "x-denokv-database-id",
          self.database_id.to_string().try_into().unwrap(),
        );
        headers.insert("x-denokv-version", HeaderValue::from_static("3"));
      }
    };
    headers
  }
}

#[derive(Clone)]
enum MetadataState {
  Pending,
  Ok(Arc<Metadata>),
  Error(Arc<String>),
}

pub trait RemotePermissions: Clone + 'static {
  fn check_net_url(&self, url: &Url) -> Result<(), JsErrorBox>;
}

/// Implements a transport that can POST bytes to a remote service.
pub trait RemoteTransport: Clone + Send + Sync + 'static {
  type Response: RemoteResponse;

  /// Perform an HTTP POST with the given body and headers, returning the final URL,
  /// status code and response object.
  fn post(
    &self,
    url: Url,
    headers: http::HeaderMap,
    body: Bytes,
  ) -> impl Future<
    Output = Result<(Url, http::StatusCode, Self::Response), JsErrorBox>,
  > + Send
       + Sync;
}

/// A response object.
pub trait RemoteResponse: Send + Sync {
  /// The bytes associated with this response.
  fn bytes(
    self,
  ) -> impl Future<Output = Result<Bytes, JsErrorBox>> + Send + Sync;
  /// The text associated with this response.
  fn text(
    self,
  ) -> impl Future<Output = Result<String, JsErrorBox>> + Send + Sync;
  /// The stream of bytes associated with this response.
  fn stream(
    self,
  ) -> impl Stream<Item = Result<Bytes, JsErrorBox>> + Send + Sync;
}

enum RetryableResult<T, E> {
  Ok(T),
  Retry,
  Err(E),
}

#[derive(Debug, Error, JsError)]
pub enum CallRawError {
  #[class(generic)]
  #[error("Database is closed")]
  DatabaseClosed,
  #[class(generic)]
  #[error("strong consistency endpoints available")]
  MissingStrongConsistencyEndpoints,
  #[class(inherit)]
  #[error(transparent)]
  Url(#[from] url::ParseError),
  #[class(generic)]
  #[error("KV Connect failed to call '{url}' (status={status}): {body}")]
  CallFailed {
    url: Url,
    status: StatusCode,
    body: String,
  },
  #[class(inherit)]
  #[error("{0}")]
  Permission(JsErrorBox),
  #[class(generic)]
  #[error("{0}")]
  Other(String),
}

#[derive(Debug, Error, JsError)]
pub enum CallDataError {
  #[class(inherit)]
  #[error(transparent)]
  CallRaw(#[from] CallRawError),
  #[class(generic)]
  #[error("KV Connect failed to read response body: {0}")]
  ReadResponse(JsErrorBox),
  #[class(generic)]
  #[error("KV Connect failed to decode response")]
  Decode(#[source] prost::DecodeError),
}

#[derive(Clone)]
pub struct Remote<P: RemotePermissions, T: RemoteTransport> {
  permissions: P,
  client: T,
  metadata_refresher: Arc<JoinHandle<()>>,
  metadata: watch::Receiver<MetadataState>,
}

impl<P: RemotePermissions, T: RemoteTransport> Remote<P, T> {
  pub fn new(
    client: T,
    permissions: P,
    metadata_endpoint: MetadataEndpoint,
  ) -> Self {
    let (tx, rx) = watch::channel(MetadataState::Pending);
    let metadata_refresher = tokio::spawn(metadata_refresh_task(
      client.clone(),
      metadata_endpoint,
      tx,
    ));
    Self {
      client,
      permissions,
      metadata_refresher: Arc::new(metadata_refresher),
      metadata: rx,
    }
  }

  async fn call_raw<Req: prost::Message>(
    &self,
    method: &'static str,
    req: Req,
  ) -> Result<(T::Response, ProtocolVersion), CallRawError> {
    let attempt = 0;
    let req_body = Bytes::from(req.encode_to_vec());
    loop {
      let metadata = loop {
        let mut metadata_rx = self.metadata.clone();
        match &*metadata_rx.borrow() {
          MetadataState::Pending => {}
          MetadataState::Ok(metadata) => break metadata.clone(),
          MetadataState::Error(e) => {
            return Err(CallRawError::Other(e.to_string()));
          }
        };
        if metadata_rx.changed().await.is_err() {
          return Err(CallRawError::DatabaseClosed);
        }
      };

      let endpoint = match metadata
        .endpoints
        .iter()
        .find(|endpoint| endpoint.consistency == DataPathConsistency::Strong)
      {
        Some(endpoint) => endpoint,
        None => {
          return Err(CallRawError::MissingStrongConsistencyEndpoints);
        }
      };

      let url = Url::parse(&format!("{}/{}", endpoint.url, method))?;
      self
        .permissions
        .check_net_url(&url)
        .map_err(CallRawError::Permission)?;

      let req = self
        .client
        .post(url.clone(), metadata.headers(), req_body.clone())
        .await;

      let resp = match req {
        Ok(resp) if resp.1 == StatusCode::OK => resp,
        Ok(resp) if resp.1.is_server_error() => {
          let status = resp.1;
          let b = resp.2.bytes().await.unwrap_or_default();
          let body = String::from_utf8_lossy(&b);
          error!(
            "KV Connect failed to call '{}' (status={}): {}",
            url, status, body
          );
          randomized_exponential_backoff(DATAPATH_BACKOFF_BASE, attempt).await;
          continue;
        }
        Ok(resp) => {
          let status = resp.1;
          let b = resp.2.bytes().await.unwrap_or_default();
          let body = String::from_utf8_lossy(&b);
          return Err(CallRawError::CallFailed {
            url,
            status,
            body: body.to_string(),
          });
        }
        Err(err) => {
          error!("KV Connect failed to call '{}': {}", url, err);
          randomized_exponential_backoff(DATAPATH_BACKOFF_BASE, attempt).await;
          continue;
        }
      };

      return Ok((resp.2, metadata.version));
    }
  }

  async fn call_stream<Req: prost::Message>(
    &self,
    method: &'static str,
    req: Req,
  ) -> Result<
    (
      impl Stream<Item = Result<Bytes, io::Error>>,
      ProtocolVersion,
    ),
    JsErrorBox,
  > {
    let (resp, version) = self
      .call_raw(method, req)
      .await
      .map_err(JsErrorBox::from_err)?;
    let stream = resp.stream().map_err(io::Error::other);
    Ok((stream, version))
  }

  async fn call_data<Req: prost::Message, Resp: prost::Message + Default>(
    &self,
    method: &'static str,
    req: Req,
  ) -> Result<(Resp, ProtocolVersion), CallDataError> {
    let (resp, version) = self.call_raw(method, req).await?;

    let resp_body = match resp.bytes().await {
      Ok(resp_body) => resp_body,
      Err(err) => {
        return Err(CallDataError::ReadResponse(err));
      }
    };

    let resp = Resp::decode(resp_body).map_err(CallDataError::Decode)?;

    Ok((resp, version))
  }
}

impl<P: RemotePermissions, T: RemoteTransport> Drop for Remote<P, T> {
  fn drop(&mut self) {
    self.metadata_refresher.abort();
  }
}

async fn randomized_exponential_backoff(base: Duration, attempt: u64) {
  let attempt = attempt.min(12);
  let delay = base.as_millis() as u64 + (2 << attempt);
  let delay = delay + rand::thread_rng().gen_range(0..(delay / 2) + 1);
  tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
}

async fn metadata_refresh_task<T: RemoteTransport>(
  client: T,
  metadata_endpoint: MetadataEndpoint,
  tx: watch::Sender<MetadataState>,
) {
  let mut attempts = 0;
  loop {
    let res = fetch_metadata(&client, &metadata_endpoint).await;
    match res {
      RetryableResult::Ok(metadata) => {
        attempts = 0;
        let expires_in = metadata.expires_at.signed_duration_since(utc_now());

        if tx.send(MetadataState::Ok(Arc::new(metadata))).is_err() {
          // The receiver has been dropped, so we can stop now.
          return;
        }

        // Sleep until the token expires, minus a 10 minute buffer, but at
        // minimum one minute.
        let sleep_time = expires_in
          .sub(chrono::Duration::seconds(10))
          .to_std()
          .unwrap_or_default()
          .min(Duration::from_secs(60));

        tokio::time::sleep(sleep_time).await;
      }
      RetryableResult::Retry => {
        attempts += 1;
        if tx.is_closed() {
          // The receiver has been dropped, so we can stop now.
          return;
        }
        randomized_exponential_backoff(METADATA_BACKOFF_BASE, attempts).await;
      }
      RetryableResult::Err(err) => {
        attempts += 1;
        if tx.send(MetadataState::Error(Arc::new(err))).is_err() {
          // The receiver has been dropped, so we can stop now.
          return;
        }
        randomized_exponential_backoff(METADATA_BACKOFF_BASE, attempts).await;
      }
    }
  }
}

async fn fetch_metadata<T: RemoteTransport>(
  client: &T,
  metadata_endpoint: &MetadataEndpoint,
) -> RetryableResult<Metadata, String> {
  let body = serde_json::to_vec(&MetadataExchangeRequest {
    supported_versions: vec![1, 2, 3],
  })
  .unwrap();
  let res = match client
    .post(
      metadata_endpoint.url.clone(),
      metadata_endpoint.headers(),
      body.into(),
    )
    .await
  {
    Ok(res) => res,
    Err(err) => {
      error!(
        "KV Connect to '{}' failed to fetch metadata: {}",
        metadata_endpoint.url, err
      );
      return RetryableResult::Retry;
    }
  };

  let res = match res.1 {
    StatusCode::OK => res,
    status if status.is_client_error() => {
      let body = res.2.text().await.unwrap_or_else(|_| String::new());
      return RetryableResult::Err(format!("Failed to fetch metadata: {body}"));
    }
    status if status.is_server_error() => {
      let body = res.2.text().await.unwrap_or_else(|_| String::new());
      error!(
        "KV Connect to '{}' failed to fetch metadata (status={}): {}",
        metadata_endpoint.url, status, body
      );
      return RetryableResult::Retry;
    }
    status => {
      return RetryableResult::Err(format!(
        "Failed to fetch metadata (status={status})"
      ));
    }
  };

  let base_url = res.0;

  let body = match res.2.text().await {
    Ok(body) => body,
    Err(err) => {
      return RetryableResult::Err(format!(
        "Metadata response body invalid: {err}"
      ));
    }
  };

  let metadata = match parse_metadata(&base_url, &body) {
    Ok(metadata) => metadata,
    Err(err) => {
      return RetryableResult::Err(format!("Failed to parse metadata: {err}"));
    }
  };

  RetryableResult::Ok(metadata)
}

fn parse_metadata(base_url: &Url, body: &str) -> Result<Metadata, String> {
  #[derive(Deserialize)]
  struct Version {
    version: u64,
  }

  let version: Version = match serde_json::from_str(body) {
    Ok(metadata) => metadata,
    Err(err) => {
      return Err(format!("could not get 'version' field: {err}"));
    }
  };

  let version = match version.version {
    1 => ProtocolVersion::V1,
    2 => ProtocolVersion::V2,
    3 => ProtocolVersion::V3,
    version => {
      return Err(format!("unsupported metadata version: {version}"));
    }
  };

  // V1, V2, and V3 have the same shape
  let metadata: DatabaseMetadata = match serde_json::from_str(body) {
    Ok(metadata) => metadata,
    Err(err) => {
      return Err(format!("{err}"));
    }
  };

  let mut endpoints = Vec::new();
  for endpoint in metadata.endpoints {
    let url = match version {
      ProtocolVersion::V1 => Url::parse(&endpoint.url),
      ProtocolVersion::V2 | ProtocolVersion::V3 => {
        Url::options().base_url(Some(base_url)).parse(&endpoint.url)
      }
    }
    .map_err(|err| format!("invalid endpoint URL: {err}"))?;

    if endpoint.url.ends_with('/') {
      return Err(format!("endpoint URL must not end with '/': {url}"));
    }

    let consistency = match &*endpoint.consistency {
      "strong" => DataPathConsistency::Strong,
      "eventual" => DataPathConsistency::Eventual,
      consistency => {
        return Err(format!("unsupported consistency level: {consistency}"));
      }
    };

    endpoints.push(DataPathEndpoint { url, consistency });
  }

  Ok(Metadata {
    version,
    endpoints,
    database_id: metadata.database_id,
    token: metadata.token.into_owned(),
    expires_at: metadata.expires_at,
  })
}

#[derive(Debug, Error, JsError)]
pub enum SnapshotReadError {
  #[class(inherit)]
  #[error(transparent)]
  CallData(#[from] CallDataError),
  #[class(generic)]
  #[error("Reads are disabled for this database")]
  ReadsDisabled,
  #[class(generic)]
  #[error("Unspecified read error (code={0})")]
  UnspecifiedRead(i32),
  #[class(generic)]
  #[error("Strong consistency reads are not available for this database")]
  StrongConsistencyReadsUnavailable,
  #[class(generic)]
  #[error("Unknown encoding {0}")]
  UnknownEncoding(i32),
  #[class(generic)]
  #[error(transparent)]
  TryFromSlice(std::array::TryFromSliceError),
}

#[derive(Debug, Error, JsError)]
pub enum AtomicWriteError {
  #[class(generic)]
  #[error("Enqueue operations are not supported in KV Connect")]
  EnqueueOperationsUnsupported,
  #[class(inherit)]
  #[error(transparent)]
  CallData(#[from] CallDataError),
  #[class(generic)]
  #[error("Writes are disabled for this database")]
  WritesDisabled,
  #[class(generic)]
  #[error("Unspecified write error")]
  UnspecifiedWrite,
  #[class(generic)]
  #[error(transparent)]
  TryFromSlice(std::array::TryFromSliceError),
}

#[derive(Debug, Error, JsError)]
pub enum WatchError {
  #[class(generic)]
  #[error("Failed to decode watch output")]
  Decode(#[source] prost::DecodeError),
  #[class(generic)]
  #[error("Reads are disabled for this database")]
  ReadsDisabled,
  #[class(generic)]
  #[error("Unspecified read error (code={0})")]
  UnspecifiedRead(i32),
  #[class(generic)]
  #[error("Unknown encoding {0}")]
  UnknownEncoding(i32),
  #[class(generic)]
  #[error(transparent)]
  TryFromSlice(std::array::TryFromSliceError),
}

#[async_trait(?Send)]
impl<P: RemotePermissions, T: RemoteTransport> Database for Remote<P, T> {
  type QMH = DummyQueueMessageHandle;

  async fn snapshot_read(
    &self,
    requests: Vec<ReadRange>,
    options: SnapshotReadOptions,
  ) -> Result<Vec<ReadRangeOutput>, JsErrorBox> {
    let ranges = requests
      .into_iter()
      .map(|r| pb::ReadRange {
        start: r.start,
        end: r.end,
        limit: r.limit.get() as _,
        reverse: r.reverse,
      })
      .collect();
    let req = pb::SnapshotRead { ranges };

    let (res, version): (pb::SnapshotReadOutput, _) = self
      .call_data("snapshot_read", req)
      .await
      .map_err(|e| JsErrorBox::from_err(SnapshotReadError::CallData(e)))?;

    match version {
      ProtocolVersion::V1 | ProtocolVersion::V2 => {
        if res.read_disabled {
          // TODO: this should result in a retry after a forced metadata refresh.
          return Err(JsErrorBox::from_err(SnapshotReadError::ReadsDisabled));
        }
      }
      ProtocolVersion::V3 => match res.status() {
        pb::SnapshotReadStatus::SrSuccess => {}
        pb::SnapshotReadStatus::SrReadDisabled => {
          // TODO: this should result in a retry after a forced metadata refresh.
          return Err(JsErrorBox::from_err(SnapshotReadError::ReadsDisabled));
        }
        pb::SnapshotReadStatus::SrUnspecified => {
          return Err(JsErrorBox::from_err(
            SnapshotReadError::UnspecifiedRead(res.status),
          ));
        }
      },
    }

    if !res.read_is_strongly_consistent
      && options.consistency == Consistency::Strong
    {
      // TODO: this should result in a retry after a forced metadata refresh.
      return Err(JsErrorBox::from_err(
        SnapshotReadError::StrongConsistencyReadsUnavailable,
      ));
    }

    let ranges = res
      .ranges
      .into_iter()
      .map(|r| {
        Ok(ReadRangeOutput {
          entries: r
            .values
            .into_iter()
            .map(|e| {
              Ok(KvEntry {
                key: e.key,
                value: decode_value(e.value, e.encoding as i64).ok_or_else(
                  || SnapshotReadError::UnknownEncoding(e.encoding),
                )?,
                versionstamp: <[u8; 10]>::try_from(&e.versionstamp[..])
                  .map_err(SnapshotReadError::TryFromSlice)?,
              })
            })
            .collect::<Result<_, SnapshotReadError>>()?,
        })
      })
      .collect::<Result<_, SnapshotReadError>>()
      .map_err(JsErrorBox::from_err)?;

    Ok(ranges)
  }

  async fn atomic_write(
    &self,
    write: AtomicWrite,
  ) -> Result<Option<CommitResult>, JsErrorBox> {
    if !write.enqueues.is_empty() {
      return Err(JsErrorBox::from_err(
        AtomicWriteError::EnqueueOperationsUnsupported,
      ));
    }

    let mut checks = Vec::new();
    for check in write.checks {
      checks.push(pb::Check {
        key: check.key,
        versionstamp: check
          .versionstamp
          .map(|v| v.to_vec())
          .unwrap_or_default(),
      });
    }

    let mut mutations = Vec::new();
    for mutation in write.mutations {
      let expire_at_ms = mutation
        .expire_at
        .map(|t| {
          let ts = t.timestamp_millis();
          if ts <= 0 {
            1
          } else {
            ts
          }
        })
        .unwrap_or(0);
      match mutation.kind {
        denokv_proto::MutationKind::Set(value) => {
          mutations.push(pb::Mutation {
            key: mutation.key,
            value: Some(encode_value_to_pb(value)),
            mutation_type: pb::MutationType::MSet as _,
            expire_at_ms,
            ..Default::default()
          });
        }
        denokv_proto::MutationKind::Delete => {
          mutations.push(pb::Mutation {
            key: mutation.key,
            value: Some(encode_value_to_pb(KvValue::Bytes(vec![]))),
            mutation_type: pb::MutationType::MDelete as _,
            expire_at_ms,
            ..Default::default()
          });
        }
        denokv_proto::MutationKind::Sum {
          value,
          min_v8,
          max_v8,
          clamp,
        } => {
          mutations.push(pb::Mutation {
            key: mutation.key,
            value: Some(encode_value_to_pb(value)),
            mutation_type: pb::MutationType::MSum as _,
            expire_at_ms,
            sum_min: min_v8,
            sum_max: max_v8,
            sum_clamp: clamp,
          });
        }
        denokv_proto::MutationKind::Max(value) => {
          mutations.push(pb::Mutation {
            key: mutation.key,
            value: Some(encode_value_to_pb(value)),
            mutation_type: pb::MutationType::MMax as _,
            expire_at_ms,
            ..Default::default()
          });
        }
        denokv_proto::MutationKind::Min(value) => {
          mutations.push(pb::Mutation {
            key: mutation.key,
            value: Some(encode_value_to_pb(value)),
            mutation_type: pb::MutationType::MMin as _,
            expire_at_ms,
            ..Default::default()
          });
        }
        denokv_proto::MutationKind::SetSuffixVersionstampedKey(value) => {
          mutations.push(pb::Mutation {
            key: mutation.key,
            value: Some(encode_value_to_pb(value)),
            mutation_type: pb::MutationType::MSetSuffixVersionstampedKey as _,
            expire_at_ms,
            ..Default::default()
          });
        }
      }
    }

    assert!(write.enqueues.is_empty());

    let req = pb::AtomicWrite {
      checks,
      mutations,
      enqueues: Vec::new(),
    };

    let (res, _): (pb::AtomicWriteOutput, _) = self
      .call_data("atomic_write", req)
      .await
      .map_err(|e| JsErrorBox::from_err(AtomicWriteError::CallData(e)))?;

    match res.status() {
      pb::AtomicWriteStatus::AwSuccess => Ok(Some(CommitResult {
        versionstamp: <[u8; 10]>::try_from(&res.versionstamp[..]).map_err(
          |e| JsErrorBox::from_err(AtomicWriteError::TryFromSlice(e)),
        )?,
      })),
      pb::AtomicWriteStatus::AwCheckFailure => Ok(None),
      pb::AtomicWriteStatus::AwWriteDisabled => {
        Err(JsErrorBox::from_err(AtomicWriteError::WritesDisabled))
      }
      pb::AtomicWriteStatus::AwUnspecified => {
        Err(JsErrorBox::from_err(AtomicWriteError::UnspecifiedWrite))
      }
    }
  }

  async fn dequeue_next_message(
    &self,
  ) -> Result<Option<Self::QMH>, JsErrorBox> {
    warn!("KV Connect does not support queues.");
    std::future::pending().await
  }

  fn watch(
    &self,
    keys: Vec<Vec<u8>>,
  ) -> Pin<Box<dyn Stream<Item = Result<Vec<WatchKeyOutput>, JsErrorBox>>>> {
    let this = self.clone();
    let stream = try_stream! {
      let mut attempt = 0;
       loop {
        attempt += 1;
        let req = pb::Watch {
          keys: keys.iter().map(|key| pb::WatchKey { key: key.clone() }).collect(),
        };

        let (stream, _) = this.call_stream("watch", req).await?;
        let stream = pin!(stream);
        let reader = StreamReader::new(stream);
        let codec = LengthDelimitedCodec::builder()
          .little_endian()
          .length_field_length(4)
          .max_frame_length(16 * 1048576)
          .new_codec();

        let mut frames = tokio_util::codec::FramedRead::new(reader, codec);
        'decode: loop {
          let res = frames.next().await;
          let frame = match res {
            Some(Ok(frame)) if frame.is_empty() => continue, // ping, ignore
            Some(Ok(frame)) => frame,
            Some(Err(err)) => {
              debug!("KV Connect watch disconnected (attempt={}): {}", attempt, err);
              break 'decode;
            }
            None => {
              break 'decode;
            }
          };

          let data = pb::WatchOutput::decode(frame).map_err(|e: prost::DecodeError| JsErrorBox::from_err(WatchError::Decode(e)))?;
          match data.status() {
            pb::SnapshotReadStatus::SrSuccess => {}
            pb::SnapshotReadStatus::SrReadDisabled => {
              // TODO: this should result in a retry after a forced metadata refresh.
              Err(JsErrorBox::from_err(WatchError::ReadsDisabled))?;
              unreachable!();
            }
            pb::SnapshotReadStatus::SrUnspecified => {
              Err(JsErrorBox::from_err(WatchError::UnspecifiedRead(data.status)))?;
              unreachable!();
            }
          }

          let mut outputs = Vec::new();
          for key in data.keys {
            if !key.changed {
              outputs.push(WatchKeyOutput::Unchanged);
            } else {
              let entry = match key.entry_if_changed {
                Some(entry) => {
                  let value = decode_value(entry.value, entry.encoding as i64)
                    .ok_or_else(|| JsErrorBox::from_err(WatchError::UnknownEncoding(entry.encoding)))?;
                  Some(KvEntry {
                    key: entry.key,
                    value,
                    versionstamp: <[u8; 10]>::try_from(&entry.versionstamp[..]).map_err(|e| JsErrorBox::from_err(WatchError::TryFromSlice(e)))?,
                  })
                },
                None => None,
              };
              outputs.push(WatchKeyOutput::Changed { entry });
            }
          }
          yield outputs;
        }

        // The stream disconnected, so retry after a short delay.
        randomized_exponential_backoff(DATAPATH_BACKOFF_BASE, attempt).await;
      }
    };
    Box::pin(stream)
  }

  fn close(&self) {}
}

pub struct DummyQueueMessageHandle {}

#[async_trait(?Send)]
impl QueueMessageHandle for DummyQueueMessageHandle {
  async fn take_payload(&mut self) -> Result<Vec<u8>, JsErrorBox> {
    unimplemented!()
  }

  async fn finish(&self, _success: bool) -> Result<(), JsErrorBox> {
    unimplemented!()
  }
}

fn encode_value_to_pb(value: KvValue) -> pb::KvValue {
  match value {
    KvValue::V8(data) => pb::KvValue {
      encoding: pb::ValueEncoding::VeV8 as _,
      data,
    },
    KvValue::Bytes(data) => pb::KvValue {
      encoding: pb::ValueEncoding::VeBytes as _,
      data,
    },
    KvValue::U64(x) => pb::KvValue {
      data: x.to_le_bytes().to_vec(),
      encoding: pb::ValueEncoding::VeLe64 as _,
    },
  }
}
