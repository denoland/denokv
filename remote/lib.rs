// Copyright 2023 the Deno authors. All rights reserved. MIT license.

mod time;

use std::ops::Sub;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::DateTime;
use chrono::Utc;
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
use log::error;
use log::warn;
use rand::Rng;
use reqwest::StatusCode;
use serde::Deserialize;
use time::utc_now;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use url::Url;
use uuid::Uuid;

use denokv_proto::datapath as pb;

pub struct MetadataEndpoint {
  pub url: Url,
  pub access_token: String,
}

enum ProtocolVersion {
  V1,
  V2,
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

#[derive(Clone)]
enum MetadataState {
  Pending,
  Ok(Arc<Metadata>),
  Error(Arc<String>),
}

pub trait RemotePermissions {
  fn check_net_url(&self, url: &Url) -> Result<(), anyhow::Error>;
}

enum RetryableResult<T, E> {
  Ok(T),
  Retry,
  Err(E),
}

pub struct Remote<P: RemotePermissions> {
  permissions: P,
  client: reqwest::Client,
  metadata_refresher: JoinHandle<()>,
  metadata: watch::Receiver<MetadataState>,
}

impl<P: RemotePermissions> Remote<P> {
  pub fn new(
    client: reqwest::Client,
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
      metadata_refresher,
      metadata: rx,
    }
  }

  pub async fn call_data<
    Req: prost::Message,
    Resp: prost::Message + Default,
  >(
    &self,
    method: &'static str,
    req: Req,
  ) -> Result<Resp, anyhow::Error> {
    let attempt = 0;
    let req_body = Bytes::from(req.encode_to_vec());
    loop {
      let metadata = loop {
        let mut metadata_rx = self.metadata.clone();
        match &*metadata_rx.borrow() {
          MetadataState::Pending => {}
          MetadataState::Ok(metadata) => break metadata.clone(),
          MetadataState::Error(e) => {
            return Err(anyhow::anyhow!("{}", e));
          }
        };
        if metadata_rx.changed().await.is_err() {
          return Err(anyhow::anyhow!("Database is closed."));
        }
      };

      let endpoint = match metadata
        .endpoints
        .iter()
        .find(|endpoint| endpoint.consistency == DataPathConsistency::Strong)
      {
        Some(endpoint) => endpoint,
        None => {
          return Err(anyhow::anyhow!(
            "No strong consistency endpoints available."
          ));
        }
      };

      let url = Url::parse(&format!("{}/{}", endpoint.url, method))?;
      self.permissions.check_net_url(&url)?;

      let req = self
        .client
        .post(url.clone())
        .body(req_body.clone())
        .bearer_auth(&metadata.token);

      let req = match metadata.version {
        ProtocolVersion::V1 => req
          .header("x-transaction-domain-id", metadata.database_id.to_string()),
        ProtocolVersion::V2 => req
          .header("x-denokv-database-id", metadata.database_id.to_string())
          .header("x-denokv-version", "2"),
      };

      let resp = match req.send().await {
        Ok(resp) if resp.status() == StatusCode::OK => resp,
        Ok(resp) if resp.status().is_server_error() => {
          let status = resp.status();
          let body = resp.text().await.unwrap_or_else(|_| String::new());
          error!(
            "KV Connect failed to call '{}' (status={}): {}",
            url, status, body
          );
          randomized_exponential_backoff(Duration::from_secs(5), attempt).await;
          continue;
        }
        Ok(resp) => {
          let status = resp.status();
          let body = resp.text().await.unwrap_or_else(|_| String::new());
          return Err(anyhow::anyhow!(
            "KV Connect failed to call '{}' (status={}): {}",
            url,
            status,
            body
          ));
        }
        Err(err) => {
          error!("KV Connect failed to call '{}': {}", url, err);
          randomized_exponential_backoff(Duration::from_secs(5), attempt).await;
          continue;
        }
      };

      let resp_body = match resp.bytes().await {
        Ok(resp_body) => resp_body,
        Err(err) => {
          return Err(anyhow::anyhow!(
            "KV Connect failed to read response body: {}",
            err
          ));
        }
      };

      let resp = Resp::decode(resp_body)
        .context("KV Connect failed to decode response")?;

      return Ok(resp);
    }
  }
}

impl<P: RemotePermissions> Drop for Remote<P> {
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

async fn metadata_refresh_task(
  client: reqwest::Client,
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
        randomized_exponential_backoff(Duration::from_secs(5), attempts).await;
      }
      RetryableResult::Err(err) => {
        attempts += 1;
        if tx.send(MetadataState::Error(Arc::new(err))).is_err() {
          // The receiver has been dropped, so we can stop now.
          return;
        }
        randomized_exponential_backoff(Duration::from_secs(5), attempts).await;
      }
    }
  }
}

async fn fetch_metadata(
  client: &reqwest::Client,
  metadata_endpoint: &MetadataEndpoint,
) -> RetryableResult<Metadata, String> {
  let res = match client
    .post(metadata_endpoint.url.clone())
    .header(
      "authorization",
      format!("Bearer {}", metadata_endpoint.access_token),
    )
    .json(&MetadataExchangeRequest {
      supported_versions: vec![1, 2],
    })
    .send()
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

  let res = match res.status() {
    StatusCode::OK => res,
    status if status.is_client_error() => {
      let body = res.text().await.unwrap_or_else(|_| String::new());
      return RetryableResult::Err(format!(
        "Failed to fetch metadata: {}",
        body
      ));
    }
    status if status.is_server_error() => {
      let body = res.text().await.unwrap_or_else(|_| String::new());
      error!(
        "KV Connect to '{}' failed to fetch metadata (status={}): {}",
        metadata_endpoint.url, status, body
      );
      return RetryableResult::Retry;
    }
    status => {
      return RetryableResult::Err(format!(
        "Failed to fetch metadata (status={})",
        status
      ));
    }
  };

  let base_url = res.url().clone();

  let body = match res.text().await {
    Ok(body) => body,
    Err(err) => {
      return RetryableResult::Err(format!(
        "Metadata response body invalid: {}",
        err
      ));
    }
  };

  let metadata = match parse_metadata(&base_url, &body) {
    Ok(metadata) => metadata,
    Err(err) => {
      return RetryableResult::Err(format!(
        "Failed to parse metadata: {}",
        err
      ));
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
      return Err(format!("could not get 'version' field: {}", err));
    }
  };

  let version = match version.version {
    1 => ProtocolVersion::V1,
    2 => ProtocolVersion::V2,
    version => {
      return Err(format!("unsupported metadata version: {}", version));
    }
  };

  // V1 and V2 have the same shape
  let metadata: DatabaseMetadata = match serde_json::from_str(body) {
    Ok(metadata) => metadata,
    Err(err) => {
      return Err(format!("{}", err));
    }
  };

  let mut endpoints = Vec::new();
  for endpoint in metadata.endpoints {
    let url = match version {
      ProtocolVersion::V1 => Url::parse(&endpoint.url),
      ProtocolVersion::V2 => {
        Url::options().base_url(Some(base_url)).parse(&endpoint.url)
      }
    }
    .map_err(|err| format!("invalid endpoint URL: {}", err))?;

    if endpoint.url.ends_with('/') {
      return Err(format!("endpoint URL must not end with '/': {}", url));
    }

    let consistency = match &*endpoint.consistency {
      "strong" => DataPathConsistency::Strong,
      "eventual" => DataPathConsistency::Eventual,
      consistency => {
        return Err(format!("unsupported consistency level: {}", consistency));
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

#[async_trait(?Send)]
impl<P: RemotePermissions> Database for Remote<P> {
  type QMH = DummyQueueMessageHandle;

  async fn snapshot_read(
    &self,
    requests: Vec<ReadRange>,
    options: SnapshotReadOptions,
  ) -> Result<Vec<ReadRangeOutput>, anyhow::Error> {
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

    let res: pb::SnapshotReadOutput =
      self.call_data("snapshot_read", req).await?;

    if res.read_disabled {
      // TODO: this should result in a retry after a forced metadata refresh.
      return Err(anyhow::anyhow!("Reads are disabled for this database."));
    }

    if !res.read_is_strongly_consistent
      && options.consistency == Consistency::Strong
    {
      // TODO: this should result in a retry after a forced metadata refresh.
      return Err(anyhow::anyhow!(
        "Strong consistency reads are not available for this database."
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
                  || anyhow::anyhow!("Unknown encoding {}", e.encoding),
                )?,
                versionstamp: <[u8; 10]>::try_from(&e.versionstamp[..])?,
              })
            })
            .collect::<Result<_, anyhow::Error>>()?,
        })
      })
      .collect::<Result<_, anyhow::Error>>()?;

    Ok(ranges)
  }

  async fn atomic_write(
    &self,
    write: AtomicWrite,
  ) -> Result<Option<CommitResult>, anyhow::Error> {
    if !write.enqueues.is_empty() {
      return Err(anyhow::anyhow!(
        "Enqueue operations are not supported in KV Connect.",
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
          });
        }
        denokv_proto::MutationKind::Delete => {
          mutations.push(pb::Mutation {
            key: mutation.key,
            value: Some(encode_value_to_pb(KvValue::Bytes(vec![]))),
            mutation_type: pb::MutationType::MDelete as _,
            expire_at_ms,
          });
        }
        denokv_proto::MutationKind::Sum(value) => {
          mutations.push(pb::Mutation {
            key: mutation.key,
            value: Some(encode_value_to_pb(value)),
            mutation_type: pb::MutationType::MSum as _,
            expire_at_ms,
          });
        }
        denokv_proto::MutationKind::Max(value) => {
          mutations.push(pb::Mutation {
            key: mutation.key,
            value: Some(encode_value_to_pb(value)),
            mutation_type: pb::MutationType::MMax as _,
            expire_at_ms,
          });
        }
        denokv_proto::MutationKind::Min(value) => {
          mutations.push(pb::Mutation {
            key: mutation.key,
            value: Some(encode_value_to_pb(value)),
            mutation_type: pb::MutationType::MMin as _,
            expire_at_ms,
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

    let res: pb::AtomicWriteOutput =
      self.call_data("atomic_write", req).await?;

    match res.status() {
      pb::AtomicWriteStatus::AwSuccess => Ok(Some(CommitResult {
        versionstamp: <[u8; 10]>::try_from(&res.versionstamp[..])?,
      })),
      pb::AtomicWriteStatus::AwCheckFailure => Ok(None),
      pb::AtomicWriteStatus::AwWriteDisabled => {
        Err(anyhow::anyhow!("Writes are disabled for this database."))
      }
      pb::AtomicWriteStatus::AwUnspecified => {
        Err(anyhow::anyhow!("Unspecified write error."))
      }
    }
  }

  async fn dequeue_next_message(
    &self,
  ) -> Result<Option<Self::QMH>, anyhow::Error> {
    warn!("KV Connect does not support queues.");
    std::future::pending().await
  }

  fn close(&self) {}
}

pub struct DummyQueueMessageHandle {}

#[async_trait(?Send)]
impl QueueMessageHandle for DummyQueueMessageHandle {
  async fn take_payload(&mut self) -> Result<Vec<u8>, anyhow::Error> {
    unimplemented!()
  }

  async fn finish(&self, _success: bool) -> Result<(), anyhow::Error> {
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
