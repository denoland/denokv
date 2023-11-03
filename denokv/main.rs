use std::borrow::Cow;
use std::num::NonZeroU32;
use std::path::Path;
use std::sync::Arc;

use anyhow::Context;
use axum::async_trait;
use axum::body::Body;
use axum::body::Bytes;
use axum::debug_handler;
use axum::extract::FromRequest;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::Request;
use axum::http::StatusCode;
use axum::middleware;
use axum::middleware::Next;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::routing::post;
use axum::Json;
use axum::Router;
use chrono::Duration;
use clap::Parser;
use config::Config;
use constant_time_eq::constant_time_eq;
use denokv_proto::datapath as pb;
use denokv_proto::decode_value;
use denokv_proto::encode_value;
use denokv_proto::AtomicWrite;
use denokv_proto::Check;
use denokv_proto::Consistency;
use denokv_proto::DatabaseMetadata;
use denokv_proto::EndpointInfo;
use denokv_proto::Enqueue;
use denokv_proto::MetadataExchangeRequest;
use denokv_proto::Mutation;
use denokv_proto::MutationKind;
use denokv_proto::ReadRange;
use denokv_proto::SnapshotReadOptions;
use denokv_sqlite::Connection;
use denokv_sqlite::Sqlite;
use log::error;
use log::info;
use prost::DecodeError;
use rand::SeedableRng;
use thiserror::Error;
use time::utc_now;
use tokio::sync::Notify;
use uuid::Uuid;

mod config;
mod limits;
mod time;

#[derive(Clone)]
struct AppState {
  sqlite: Sqlite,
  access_token: &'static str,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
  let config = Config::parse();
  std::env::set_var("RUST_LOG", "info");
  env_logger::init();

  if config.access_token.len() < 12 {
    anyhow::bail!("Access token must be at minimum 12 chars long.");
  }

  let path = Path::new(&config.sqlite_path);
  let (sqlite, path) = open_sqlite(path)?;
  info!("Opened database at {}", path);

  let access_token = Box::leak(config.access_token.into_boxed_str());

  let state = AppState {
    sqlite,
    access_token,
  };

  let v1 = Router::new()
    .route("/snapshot_read", post(snapshot_read_endpoint))
    .route("/atomic_write", post(atomic_write_endpoint))
    .route_layer(middleware::from_fn_with_state(
      state.clone(),
      authentication_middleware,
    ));

  let app = Router::new()
    .route("/", post(metadata_endpoint))
    .nest("/v2", v1)
    .fallback(fallback_handler)
    .with_state(state);

  let listener = std::net::TcpListener::bind(config.addr)
    .context("Failed to start server")?;
  info!("Listening on http://{}", listener.local_addr().unwrap());

  axum::Server::from_tcp(listener)?
    .serve(app.into_make_service())
    .await?;

  Ok(())
}

fn open_sqlite(path: &Path) -> Result<(Sqlite, String), anyhow::Error> {
  let conn = Connection::open(path)?;
  let notify = Arc::new(Notify::new());
  let rng: Box<_> = Box::new(rand::rngs::StdRng::from_entropy());
  let path = conn.path().unwrap().to_owned();
  let sqlite = Sqlite::new(conn, notify, rng)?;
  Ok((sqlite, path))
}

#[axum::debug_handler]
async fn metadata_endpoint(
  State(state): State<AppState>,
  headers: HeaderMap,
  maybe_req: Option<Json<MetadataExchangeRequest>>,
) -> Result<Json<DatabaseMetadata>, ApiError> {
  let Some(Json(req)) = maybe_req else {
    return Err(ApiError::MinumumProtocolVersion);
  };
  if !req.supported_versions.contains(&2) {
    return Err(ApiError::NoMatchingProtocolVersion);
  }
  let Some(authorization) =
    headers.get("authorization").and_then(|v| v.to_str().ok())
  else {
    return Err(ApiError::MalformedAuthorizationHeader);
  };
  let Some((bearer, token)) = authorization.split_once(' ') else {
    return Err(ApiError::MalformedAuthorizationHeader);
  };
  if bearer.to_lowercase() != "bearer"
    || !constant_time_eq(token.as_bytes(), state.access_token.as_bytes())
  {
    return Err(ApiError::InvalidAccessToken);
  }
  let expires_at = utc_now() + Duration::days(1);
  Ok(Json(DatabaseMetadata {
    version: 2,
    database_id: Uuid::nil(),
    endpoints: vec![EndpointInfo {
      url: Cow::Borrowed("/v2"),
      consistency: Cow::Borrowed("strong"),
    }],
    token: Cow::Borrowed(state.access_token),
    expires_at,
  }))
}

// #[axum::debug_handler]
async fn authentication_middleware(
  State(state): State<AppState>,
  req: Request<Body>,
  next: Next<Body>,
) -> Result<Response, ApiError> {
  let Some(protocol_version) = req
    .headers()
    .get("x-denokv-version")
    .and_then(|v| v.to_str().ok())
  else {
    return Err(ApiError::InvalidProtocolVersion);
  };
  if protocol_version != "2" {
    return Err(ApiError::InvalidProtocolVersion);
  }
  let Some(authorization) = req
    .headers()
    .get("authorization")
    .and_then(|v| v.to_str().ok())
  else {
    return Err(ApiError::MalformedAuthorizationHeader);
  };
  let Some((bearer, token)) = authorization.split_once(' ') else {
    return Err(ApiError::MalformedAuthorizationHeader);
  };
  if bearer.to_lowercase() != "bearer" || token != state.access_token {
    return Err(ApiError::InvalidAccessToken);
  }
  let Some(td_id) = req
    .headers()
    .get("x-denokv-database-id")
    .and_then(|v| v.to_str().ok())
  else {
    return Err(ApiError::InvalidDatabaseId);
  };
  let td_id = match Uuid::parse_str(td_id) {
    Ok(td_id) => td_id,
    Err(_) => return Err(ApiError::InvalidDatabaseId),
  };
  if !td_id.is_nil() {
    return Err(ApiError::InvalidDatabaseId);
  }

  Ok(next.run(req).await)
}

#[axum::debug_handler]
async fn snapshot_read_endpoint(
  State(state): State<AppState>,
  Protobuf(snapshot_read): Protobuf<pb::SnapshotRead>,
) -> Result<Protobuf<pb::SnapshotReadOutput>, ApiError> {
  if snapshot_read.ranges.len() > limits::MAX_READ_RANGES {
    return Err(ApiError::TooManyReadRanges);
  }
  let mut requests = Vec::with_capacity(snapshot_read.ranges.len());
  let mut total_limit: usize = 0;
  for range in snapshot_read.ranges {
    let limit: NonZeroU32 = u32::try_from(range.limit)
      .map_err(|_| ApiError::InvalidReadRangeLimit)?
      .try_into()
      .map_err(|_| ApiError::InvalidReadRangeLimit)?;
    if range.start.len() > limits::MAX_READ_KEY_SIZE_BYTES {
      return Err(ApiError::KeyTooLong);
    }
    if range.end.len() > limits::MAX_READ_KEY_SIZE_BYTES {
      return Err(ApiError::KeyTooLong);
    }
    total_limit += limit.get() as usize;
    requests.push(ReadRange {
      start: range.start,
      end: range.end,
      reverse: range.reverse,
      limit,
    });
  }
  if total_limit > limits::MAX_READ_ENTRIES {
    return Err(ApiError::ReadRangeTooLarge);
  }
  let options = SnapshotReadOptions {
    consistency: Consistency::Strong,
  };

  let result_ranges = state
    .sqlite
    .snapshot_read(requests, options)
    .await
    .map_err(|err| {
      log::error!("Failed to read from the database: {err}");
      ApiError::InternalServerError
    })?;

  let mut ranges = Vec::with_capacity(result_ranges.len());
  for range in result_ranges {
    let values = range
      .entries
      .into_iter()
      .map(|entry| {
        let (value, encoding) = encode_value(&entry.value);
        pb::KvEntry {
          key: entry.key,
          value: value.into_owned(),
          encoding: encoding as i32,
          versionstamp: entry.versionstamp.to_vec(),
        }
      })
      .collect();
    ranges.push(pb::ReadRangeOutput { values });
  }

  Ok(Protobuf(pb::SnapshotReadOutput {
    ranges,
    read_disabled: false,
    read_is_strongly_consistent: true,
  }))
}

#[debug_handler]
async fn atomic_write_endpoint(
  State(state): State<AppState>,
  Protobuf(atomic_write): Protobuf<pb::AtomicWrite>,
) -> Result<Protobuf<pb::AtomicWriteOutput>, ApiError> {
  if atomic_write.checks.len() > limits::MAX_CHECKS {
    return Err(ApiError::TooManyChecks);
  }
  if atomic_write.mutations.len() + atomic_write.enqueues.len()
    > limits::MAX_MUTATIONS
  {
    return Err(ApiError::TooManyMutations);
  }

  let mut total_payload_size = 0;

  let mut checks = Vec::with_capacity(atomic_write.checks.len());
  for check in atomic_write.checks {
    if check.key.len() > limits::MAX_READ_KEY_SIZE_BYTES {
      return Err(ApiError::KeyTooLong);
    }
    total_payload_size += check.key.len();
    checks.push(Check {
      key: check.key,
      versionstamp: match check.versionstamp.len() {
        0 => None,
        10 => {
          let mut versionstamp = [0; 10];
          versionstamp.copy_from_slice(&check.versionstamp);
          Some(versionstamp)
        }
        _ => return Err(ApiError::InvalidVersionstamp),
      },
    });
  }

  let mut mutations = Vec::with_capacity(atomic_write.mutations.len());
  for mutation in atomic_write.mutations {
    if mutation.key.len() > limits::MAX_WRITE_KEY_SIZE_BYTES {
      return Err(ApiError::KeyTooLong);
    }
    total_payload_size += mutation.key.len();
    let value_size = mutation.value.as_ref().map(|v| v.data.len()).unwrap_or(0);
    if value_size > limits::MAX_VALUE_SIZE_BYTES {
      return Err(ApiError::ValueTooLong);
    }
    total_payload_size += value_size;

    let kind = match (mutation.mutation_type(), mutation.value) {
      (pb::MutationType::MSet, Some(value)) => {
        let value = decode_value(value.data, value.encoding as i64)
          .ok_or_else(|| {
            error!(
              "Failed to decode value with invalid encoding {}",
              value.encoding
            );
            ApiError::InternalServerError
          })?;
        MutationKind::Set(value)
      }
      (pb::MutationType::MDelete, _) => MutationKind::Delete,
      (pb::MutationType::MSum, Some(value)) => {
        let value = decode_value(value.data, value.encoding as i64)
          .ok_or_else(|| {
            error!(
              "Failed to decode value with invalid encoding {}",
              value.encoding
            );
            ApiError::InternalServerError
          })?;
        MutationKind::Sum(value)
      }
      (pb::MutationType::MMin, Some(value)) => {
        let value = decode_value(value.data, value.encoding as i64)
          .ok_or_else(|| {
            error!(
              "Failed to decode value with invalid encoding {}",
              value.encoding
            );
            ApiError::InternalServerError
          })?;
        MutationKind::Min(value)
      }
      (pb::MutationType::MMax, Some(value)) => {
        let value = decode_value(value.data, value.encoding as i64)
          .ok_or_else(|| {
            error!(
              "Failed to decode value with invalid encoding {}",
              value.encoding
            );
            ApiError::InternalServerError
          })?;
        MutationKind::Max(value)
      }
      _ => return Err(ApiError::InvalidMutationKind),
    };
    let expire_at = match mutation.expire_at_ms {
      -1 | 0 => None,
      millis @ 1.. => Some(
        chrono::DateTime::UNIX_EPOCH
          + std::time::Duration::from_millis(
            millis
              .try_into()
              .map_err(|_| ApiError::InvalidMutationExpireAt)?,
          ),
      ),
      _ => return Err(ApiError::InvalidMutationExpireAt),
    };
    mutations.push(Mutation {
      key: mutation.key,
      expire_at,
      kind,
    })
  }

  let mut enqueues = Vec::with_capacity(atomic_write.enqueues.len());
  for enqueue in atomic_write.enqueues {
    if enqueue.payload.len() > limits::MAX_VALUE_SIZE_BYTES {
      return Err(ApiError::ValueTooLong);
    }
    total_payload_size += enqueue.payload.len();
    if enqueue.kv_keys_if_undelivered.len() > limits::MAX_QUEUE_UNDELIVERED_KEYS
    {
      return Err(ApiError::TooManyQueueUndeliveredKeys);
    }
    for key in &enqueue.kv_keys_if_undelivered {
      if key.len() > limits::MAX_WRITE_KEY_SIZE_BYTES {
        return Err(ApiError::KeyTooLong);
      }
      total_payload_size += key.len();
    }
    if enqueue.backoff_schedule.len() > limits::MAX_QUEUE_BACKOFF_INTERVALS {
      return Err(ApiError::TooManyQueueBackoffIntervals);
    }
    for interval in &enqueue.backoff_schedule {
      if *interval > limits::MAX_QUEUE_BACKOFF_MS {
        return Err(ApiError::QueueBackoffIntervalTooLarge);
      }
      total_payload_size += 4;
    }
    let deadline = chrono::DateTime::UNIX_EPOCH
      + std::time::Duration::from_millis(
        enqueue
          .deadline_ms
          .try_into()
          .map_err(|_| ApiError::InvalidMutationEnqueueDeadline)?,
      );
    if utc_now().signed_duration_since(deadline).num_milliseconds()
      > limits::MAX_QUEUE_DELAY_MS as i64
    {
      return Err(ApiError::InvalidMutationEnqueueDeadline);
    }
    enqueues.push(Enqueue {
      payload: enqueue.payload,
      backoff_schedule: if enqueue.backoff_schedule.is_empty() {
        None
      } else {
        Some(enqueue.backoff_schedule)
      },
      deadline,
      keys_if_undelivered: enqueue.kv_keys_if_undelivered,
    });
  }

  if total_payload_size > limits::MAX_TOTAL_MUTATION_SIZE_BYTES {
    return Err(ApiError::AtomicWriteTooLarge);
  }

  let atomic_write = AtomicWrite {
    checks,
    mutations,
    enqueues,
  };

  let res = state.sqlite.atomic_write(atomic_write).await;

  match res {
    Ok(None) => Ok(Protobuf(pb::AtomicWriteOutput {
      status: pb::AtomicWriteStatus::AwCheckFailure as i32,
      failed_checks: vec![], // todo!
      ..Default::default()
    })),
    Ok(Some(commit_result)) => Ok(Protobuf(pb::AtomicWriteOutput {
      status: pb::AtomicWriteStatus::AwSuccess as i32,
      versionstamp: commit_result.versionstamp.to_vec(),
      ..Default::default()
    })),
    Err(err) => {
      error!("Failed to write to database: {}", err);
      Err(ApiError::InternalServerError)
    }
  }
}

#[debug_handler]
async fn fallback_handler() -> ApiError {
  ApiError::NotFound
}

#[derive(Error, Debug)]
enum ApiError {
  #[error("Resource not found.")]
  NotFound,
  #[error("Malformed authorization header.")]
  MalformedAuthorizationHeader,
  #[error("Invalid access token.")]
  InvalidAccessToken,
  #[error("Invalid database id.")]
  InvalidDatabaseId,
  #[error("Expected protocol version 2.")]
  InvalidProtocolVersion,
  #[error("Request protobuf is invalid: {}.", .0)]
  InvalidRequestProto(DecodeError),
  #[error("A key exceeds the key size limit.")]
  KeyTooLong,
  #[error("A value exceeds the value size limit.")]
  ValueTooLong,
  #[error("The total number of entries requested across read ranges in the read request is too large.")]
  ReadRangeTooLarge,
  #[error("The total size of the atomic write is too large.")]
  AtomicWriteTooLarge,
  #[error("Too many read ranges requested in one read request.")]
  TooManyReadRanges,
  #[error("Too many checks included in atomic write.")]
  TooManyChecks,
  #[error("Too many mutations / enqueues included in atomic write.")]
  TooManyMutations,
  #[error("Too many dead letter keys for a single queue message.")]
  TooManyQueueUndeliveredKeys,
  #[error("Too many backoff intervals for a single queue message.")]
  TooManyQueueBackoffIntervals,
  #[error("A backoff interval exceeds the maximum allowed backoff interval.")]
  QueueBackoffIntervalTooLarge,
  #[error("The read range limit of the snapshot read request is invalid.")]
  InvalidReadRangeLimit,
  #[error("An internal server error occurred.")]
  InternalServerError,
  #[error("Invalid versionstamp.")]
  InvalidVersionstamp,
  #[error("Invalid mutation kind or arguments.")]
  InvalidMutationKind,
  #[error("Invalid mutation expire at.")]
  InvalidMutationExpireAt,
  #[error("Invalid mutation enqueue deadline.")]
  InvalidMutationEnqueueDeadline,
  #[error("The server requires at least protocol version 2. Use Deno 1.38.0 or newer.")]
  MinumumProtocolVersion,
  #[error("The server could not negotiate a protocol version. The server requires protocol version 2.")]
  NoMatchingProtocolVersion,
}

impl ApiError {
  fn status(&self) -> StatusCode {
    match self {
      ApiError::NotFound => StatusCode::NOT_FOUND,
      ApiError::MalformedAuthorizationHeader => StatusCode::UNAUTHORIZED,
      ApiError::InvalidAccessToken => StatusCode::UNAUTHORIZED,
      ApiError::InvalidDatabaseId => StatusCode::BAD_REQUEST,
      ApiError::InvalidProtocolVersion => StatusCode::BAD_REQUEST,
      ApiError::InvalidRequestProto(..) => StatusCode::BAD_REQUEST,
      ApiError::KeyTooLong => StatusCode::BAD_REQUEST,
      ApiError::ValueTooLong => StatusCode::BAD_REQUEST,
      ApiError::ReadRangeTooLarge => StatusCode::BAD_REQUEST,
      ApiError::AtomicWriteTooLarge => StatusCode::BAD_REQUEST,
      ApiError::TooManyReadRanges => StatusCode::BAD_REQUEST,
      ApiError::TooManyChecks => StatusCode::BAD_REQUEST,
      ApiError::TooManyMutations => StatusCode::BAD_REQUEST,
      ApiError::TooManyQueueUndeliveredKeys => StatusCode::BAD_REQUEST,
      ApiError::TooManyQueueBackoffIntervals => StatusCode::BAD_REQUEST,
      ApiError::QueueBackoffIntervalTooLarge => StatusCode::BAD_REQUEST,
      ApiError::InvalidReadRangeLimit => StatusCode::BAD_REQUEST,
      ApiError::InternalServerError => StatusCode::INTERNAL_SERVER_ERROR,
      ApiError::InvalidVersionstamp => StatusCode::BAD_REQUEST,
      ApiError::InvalidMutationKind => StatusCode::BAD_REQUEST,
      ApiError::InvalidMutationExpireAt => StatusCode::BAD_REQUEST,
      ApiError::InvalidMutationEnqueueDeadline => StatusCode::BAD_REQUEST,
      ApiError::MinumumProtocolVersion => StatusCode::BAD_REQUEST,
      ApiError::NoMatchingProtocolVersion => StatusCode::BAD_REQUEST,
    }
  }
}

impl IntoResponse for ApiError {
  fn into_response(self) -> Response {
    (self.status(), format!("{}", self)).into_response()
  }
}

struct Protobuf<T: prost::Message>(T);

impl<T: prost::Message> IntoResponse for Protobuf<T> {
  fn into_response(self) -> Response {
    let body = self.0.encode_to_vec();
    (
      StatusCode::OK,
      [("content-type", "application/protobuf")],
      body,
    )
      .into_response()
  }
}

#[async_trait]
impl<S: Send + Sync, T: prost::Message + Default> FromRequest<S, Body>
  for Protobuf<T>
{
  type Rejection = Response;

  async fn from_request(
    req: Request<Body>,
    state: &S,
  ) -> Result<Self, Self::Rejection> {
    let body = Bytes::from_request(req, state)
      .await
      .map_err(|e| e.into_response())?;
    let msg = T::decode(body)
      .map_err(|err| ApiError::InvalidRequestProto(err).into_response())?;
    Ok(Protobuf(msg))
  }
}
