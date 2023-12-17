use std::borrow::Cow;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use anyhow::Context;
use aws_smithy_async::rt::sleep::TokioSleep;
use aws_smithy_types::retry::RetryConfig;
use axum::async_trait;
use axum::body::Body;
use axum::body::Bytes;
use axum::body::StreamBody;
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
use chrono::DateTime;
use chrono::Duration;
use chrono::SecondsFormat;
use chrono::Utc;
use clap::Parser;
use config::Config;
use config::PitrOptions;
use config::ReplicaOptions;
use config::ServeOptions;
use config::SubCmd;
use constant_time_eq::constant_time_eq;
use denokv_proto::datapath as pb;
use denokv_proto::time::utc_now;
use denokv_proto::AtomicWrite;
use denokv_proto::Consistency;
use denokv_proto::ConvertError;
use denokv_proto::DatabaseMetadata;
use denokv_proto::EndpointInfo;
use denokv_proto::MetadataExchangeRequest;
use denokv_proto::ReadRange;
use denokv_proto::SnapshotReadOptions;
use denokv_sqlite::Connection;
use denokv_sqlite::Sqlite;
use denokv_sqlite::SqliteBackendError;
use denokv_sqlite::SqliteConfig;
use denokv_sqlite::SqliteNotifier;
use denokv_timemachine::backup_source_s3::DatabaseBackupSourceS3;
use denokv_timemachine::backup_source_s3::DatabaseBackupSourceS3Config;
use denokv_timemachine::time_travel::TimeTravelControl;
use futures::stream::unfold;
use futures::stream_select;
use futures::StreamExt;
use futures::TryStreamExt;
use hyper::client::HttpConnector;
use hyper_proxy::Intercept;
use hyper_proxy::Proxy;
use hyper_proxy::ProxyConnector;
use log::error;
use log::info;
use prost::DecodeError;
use prost::Message;
use rand::Rng;
use rand::SeedableRng;
use rusqlite::OpenFlags;
use std::env;
use thiserror::Error;
use tokio::sync::oneshot;
use tokio::time::MissedTickBehavior;
use uuid::Uuid;

use crate::config::PitrSubCmd;

mod config;

const SYNC_INTERVAL_BASE_MS: u64 = 10000;
const SYNC_INTERVAL_JITTER_MS: u64 = 5000;

#[derive(Clone)]
struct AppState {
  sqlite: Sqlite,
  access_token: &'static str,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
  let config: &'static Config = Box::leak(Box::new(Config::parse()));
  if std::env::var("RUST_LOG").is_err() {
    std::env::set_var("RUST_LOG", "info");
  }
  env_logger::init();

  match &config.subcommand {
    SubCmd::Serve(options) => {
      let (initial_sync_ok_tx, initial_sync_ok_rx) = oneshot::channel();

      let sync_fut = async move {
        if options.sync_from_s3 {
          run_sync(config, &options.replica, true, Some(initial_sync_ok_tx))
            .await
        } else {
          drop(initial_sync_ok_tx);
          futures::future::pending().await
        }
        .with_context(|| "Failed to sync from S3")
      };
      let serve_fut = async move {
        drop(initial_sync_ok_rx.await);
        run_serve(config, options).await
      };

      let sync_fut = std::pin::pin!(sync_fut);
      let serve_fut = std::pin::pin!(serve_fut);

      futures::future::try_join(sync_fut, serve_fut).await?;
    }
    SubCmd::Pitr(options) => {
      run_pitr(config, options).await?;
    }
  }

  Ok(())
}

async fn run_pitr(
  config: &'static Config,
  options: &'static PitrOptions,
) -> anyhow::Result<()> {
  match &options.subcommand {
    PitrSubCmd::Sync(options) => {
      run_sync(config, &options.replica, false, None).await?;
    }
    PitrSubCmd::List(options) => {
      let db = rusqlite::Connection::open(&config.sqlite_path)?;
      let mut ttc = TimeTravelControl::open(db)?;

      let start = if let Some(start) = &options.start {
        DateTime::parse_from_rfc3339(start)
          .ok()
          .with_context(|| format!("invalid start time {:?}", options.start))?
          .with_timezone(&Utc)
      } else {
        Default::default()
      };
      let end = if let Some(end) = &options.end {
        Some(
          DateTime::parse_from_rfc3339(end)
            .ok()
            .with_context(|| format!("invalid end time {:?}", options.end))?
            .with_timezone(&Utc),
        )
      } else {
        None
      };
      let versionstamps =
        ttc.lookup_versionstamps_around_timestamp(start, end)?;

      for (versionstamp, ts) in versionstamps {
        // Users will want to pipe output of this command into e.g. `less`
        if writeln!(
          &mut std::io::stdout(),
          "{}\t{}",
          hex::encode(versionstamp),
          ts.to_rfc3339_opts(SecondsFormat::Millis, true)
        )
        .is_err()
        {
          break;
        }
      }
    }
    PitrSubCmd::Info => {
      let db = rusqlite::Connection::open(&config.sqlite_path)?;
      let mut ttc = TimeTravelControl::open(db)?;

      let current_versionstamp = ttc.get_current_versionstamp()?;
      println!(
        "Current versionstamp: {}",
        hex::encode(current_versionstamp)
      );
    }
    PitrSubCmd::Checkout(options) => {
      let db = rusqlite::Connection::open(&config.sqlite_path)?;
      let mut ttc = TimeTravelControl::open(db)?;
      let versionstamp = hex::decode(&options.versionstamp)
        .ok()
        .and_then(|x| <[u8; 10]>::try_from(x).ok())
        .with_context(|| {
          format!("invalid versionstamp {}", options.versionstamp)
        })?;
      ttc.checkout(versionstamp)?;
      let versionstamp = ttc.get_current_versionstamp()?;
      println!(
        "Snapshot is now at versionstamp {}",
        hex::encode(versionstamp)
      );
    }
  }
  Ok(())
}

async fn run_serve(
  config: &'static Config,
  options: &'static ServeOptions,
) -> anyhow::Result<()> {
  if options.access_token.len() < 12 {
    anyhow::bail!("Access token must be at minimum 12 chars long.");
  }

  let path = Path::new(&config.sqlite_path);
  let read_only = options.read_only || options.sync_from_s3;
  let sqlite_config = SqliteConfig {
    batch_timeout: options
      .atomic_write_batch_timeout_ms
      .map(std::time::Duration::from_millis),
    num_workers: options.num_workers,
  };
  let sqlite = open_sqlite(path, read_only, sqlite_config.clone())?;
  info!(
    "Opened{} database at {}. Batch timeout: {:?}",
    if read_only { " read only" } else { "" },
    path.to_string_lossy(),
    sqlite_config.batch_timeout,
  );

  let access_token = options.access_token.as_str();

  let state = AppState {
    sqlite,
    access_token,
  };

  let v1 = Router::new()
    .route("/snapshot_read", post(snapshot_read_endpoint))
    .route("/atomic_write", post(atomic_write_endpoint))
    .route("/watch", post(watch_endpoint))
    .route_layer(middleware::from_fn_with_state(
      state.clone(),
      authentication_middleware,
    ));

  let app = Router::new()
    .route("/", post(metadata_endpoint))
    .nest("/v2", v1)
    .fallback(fallback_handler)
    .with_state(state);

  let listener = std::net::TcpListener::bind(options.addr)
    .context("Failed to start server")?;
  info!("Listening on http://{}", listener.local_addr().unwrap());

  axum::Server::from_tcp(listener)?
    .serve(app.into_make_service())
    .await?;

  Ok(())
}

async fn run_sync(
  config: &Config,
  options: &ReplicaOptions,
  continuous: bool,
  initial_sync_ok_tx: Option<oneshot::Sender<()>>,
) -> anyhow::Result<()> {
  let mut s3_config = aws_config::from_env()
    .sleep_impl(Arc::new(TokioSleep::new()))
    .retry_config(RetryConfig::standard().with_max_attempts(std::u32::MAX));

  if let Some(endpoint) = &options.s3_endpoint {
    s3_config = s3_config.endpoint_url(endpoint);
  }

  let https_proxy = env::var("https_proxy")
    .or_else(|_| env::var("HTTPS_PROXY"))
    .ok();
  if let Some(https_proxy) = https_proxy {
    let proxy = {
      let proxy_uri = https_proxy.parse().unwrap();
      let proxy = Proxy::new(Intercept::All, proxy_uri);
      let connector = HttpConnector::new();
      ProxyConnector::from_proxy(connector, proxy).unwrap()
    };
    let hyper_client =
      aws_smithy_client::hyper_ext::Adapter::builder().build(proxy);

    s3_config = s3_config.http_connector(hyper_client);
  }

  let s3_config = s3_config.load().await;
  let s3_client = aws_sdk_s3::Client::new(&s3_config);

  let db = rusqlite::Connection::open(&config.sqlite_path)?;
  let mut ttc = TimeTravelControl::open(db)?;
  let s3_config = DatabaseBackupSourceS3Config {
    bucket: options
      .s3_bucket
      .clone()
      .ok_or_else(|| anyhow::anyhow!("--s3-bucket not set"))?,
    prefix: options.s3_prefix.clone().unwrap_or_default(),
  };
  if !s3_config.prefix.ends_with('/') {
    anyhow::bail!("--s3-prefix must end with a slash")
  }

  ttc.init_s3(&s3_config)?;
  let source = DatabaseBackupSourceS3::new(s3_client, s3_config);
  ttc.ensure_initial_snapshot_completed(&source).await?;

  log::info!("Initial snapshot is complete, starting sync.");
  ttc.checkout([0xffu8; 10])?;
  drop(initial_sync_ok_tx);

  loop {
    ttc.sync(&source).await?;

    if !continuous {
      return Ok(());
    }

    // In continuous mode, always advance snapshot to latest version
    ttc.checkout([0xffu8; 10])?;

    let sleep_duration = std::time::Duration::from_millis(
      SYNC_INTERVAL_BASE_MS
        + rand::thread_rng().gen_range(0..SYNC_INTERVAL_JITTER_MS),
    );
    tokio::time::sleep(sleep_duration).await;
  }
}

fn open_sqlite(
  path: &Path,
  read_only: bool,
  config: SqliteConfig,
) -> Result<Sqlite, anyhow::Error> {
  let flags = if read_only {
    OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX
  } else {
    OpenFlags::SQLITE_OPEN_READ_WRITE
      | OpenFlags::SQLITE_OPEN_CREATE
      | OpenFlags::SQLITE_OPEN_NO_MUTEX
  };
  let notifier = SqliteNotifier::default();
  let sqlite = Sqlite::new(
    || {
      Ok((
        Connection::open_with_flags(path, flags)?,
        Box::new(rand::rngs::StdRng::from_entropy()),
      ))
    },
    notifier,
    config,
  )?;
  Ok(sqlite)
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
  let version = if req.supported_versions.contains(&3) {
    3
  } else if req.supported_versions.contains(&2) {
    2
  } else {
    return Err(ApiError::NoMatchingProtocolVersion);
  };
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
    version,
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
  if protocol_version != "2" && protocol_version != "3" {
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
  let requests: Vec<ReadRange> = snapshot_read.try_into()?;

  let options = SnapshotReadOptions {
    consistency: Consistency::Strong,
  };

  let result_ranges = state.sqlite.snapshot_read(requests, options).await?;

  let res = result_ranges.into();
  Ok(Protobuf(res))
}

#[debug_handler]
async fn atomic_write_endpoint(
  State(state): State<AppState>,
  Protobuf(atomic_write): Protobuf<pb::AtomicWrite>,
) -> Result<Protobuf<pb::AtomicWriteOutput>, ApiError> {
  let atomic_write: AtomicWrite = atomic_write.try_into()?;

  let res = state.sqlite.atomic_write(atomic_write).await?;

  Ok(Protobuf(res.into()))
}

async fn watch_endpoint(
  State(state): State<AppState>,
  Protobuf(watch): Protobuf<pb::Watch>,
) -> Result<impl IntoResponse, ApiError> {
  let keys = watch.try_into()?;

  let watcher = state.sqlite.watch(keys);

  let data_stream = watcher.map_ok(|outs| {
    let output = pb::WatchOutput::from(outs);
    output.encode_to_vec()
  });

  let mut timer = tokio::time::interval(std::time::Duration::from_secs(5));
  timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
  let ping_stream = unfold(timer, |mut timer| async move {
    timer.tick().await;
    Some((Ok::<_, anyhow::Error>(vec![]), timer))
  })
  .boxed();

  let body_stream = stream_select!(data_stream, ping_stream).map_ok(|data| {
    Bytes::from([&(data.len() as u32).to_le_bytes()[..], &data[..]].concat())
  });

  let mut res = StreamBody::new(body_stream).into_response();
  res
    .headers_mut()
    .insert("content-type", "application/octet-stream".parse().unwrap());
  Ok(res)
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
  #[error("Too many keys watched in a single watch request.")]
  TooManyWatchedKeys,
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
  #[error("The server could not negotiate a protocol version. The server requires protocol version 2 or 3.")]
  NoMatchingProtocolVersion,
  #[error("The server is temporarially unavailable, try again later.")]
  TryAgain,
  #[error("This database is read only.")]
  // TODO: this should not be used (write_disabled should be used instead)
  ReadOnly,
  #[error("The value encoding {0} is unknown.")]
  UnknownValueEncoding(i64),
  #[error("{0}")]
  TypeMismatch(String),
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
      ApiError::TooManyWatchedKeys => StatusCode::BAD_REQUEST,
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
      ApiError::TryAgain => StatusCode::SERVICE_UNAVAILABLE,
      ApiError::ReadOnly => StatusCode::BAD_REQUEST,
      ApiError::UnknownValueEncoding(_) => StatusCode::BAD_REQUEST,
      ApiError::TypeMismatch(_) => StatusCode::BAD_REQUEST,
    }
  }
}

impl From<SqliteBackendError> for ApiError {
  fn from(value: SqliteBackendError) -> Self {
    match value {
      SqliteBackendError::SqliteError(err) => {
        log::error!("Sqlite error: {}", err);
        ApiError::InternalServerError
      }
      SqliteBackendError::GenericError(err) => {
        log::error!("Generic error: {}", err);
        ApiError::InternalServerError
      }
      SqliteBackendError::DatabaseClosed => ApiError::TryAgain,
      SqliteBackendError::WriteDisabled => ApiError::ReadOnly,
      SqliteBackendError::UnknownValueEncoding(encoding) => {
        ApiError::UnknownValueEncoding(encoding)
      }
      SqliteBackendError::TypeMismatch(msg) => ApiError::TypeMismatch(msg),
    }
  }
}

impl IntoResponse for ApiError {
  fn into_response(self) -> Response {
    (self.status(), format!("{}", self)).into_response()
  }
}

impl From<ConvertError> for ApiError {
  fn from(err: ConvertError) -> ApiError {
    match err {
      ConvertError::KeyTooLong => ApiError::KeyTooLong,
      ConvertError::ValueTooLong => ApiError::ValueTooLong,
      ConvertError::ReadRangeTooLarge => ApiError::ReadRangeTooLarge,
      ConvertError::AtomicWriteTooLarge => ApiError::AtomicWriteTooLarge,
      ConvertError::TooManyReadRanges => ApiError::TooManyReadRanges,
      ConvertError::TooManyWatchedKeys => ApiError::TooManyWatchedKeys,
      ConvertError::TooManyChecks => ApiError::TooManyChecks,
      ConvertError::TooManyMutations => ApiError::TooManyMutations,
      ConvertError::TooManyQueueUndeliveredKeys => {
        ApiError::TooManyQueueUndeliveredKeys
      }
      ConvertError::TooManyQueueBackoffIntervals => {
        ApiError::TooManyQueueBackoffIntervals
      }
      ConvertError::QueueBackoffIntervalTooLarge => {
        ApiError::QueueBackoffIntervalTooLarge
      }
      ConvertError::InvalidReadRangeLimit => ApiError::InvalidReadRangeLimit,
      ConvertError::DecodeError => ApiError::InternalServerError,
      ConvertError::InvalidVersionstamp => ApiError::InvalidVersionstamp,
      ConvertError::InvalidMutationKind => ApiError::InvalidMutationKind,
      ConvertError::InvalidMutationExpireAt => {
        ApiError::InvalidMutationExpireAt
      }
      ConvertError::InvalidMutationEnqueueDeadline => {
        ApiError::InvalidMutationEnqueueDeadline
      }
    }
  }
}

struct Protobuf<T: prost::Message>(T);

impl<T: prost::Message> IntoResponse for Protobuf<T> {
  fn into_response(self) -> Response {
    let body = self.0.encode_to_vec();
    (
      StatusCode::OK,
      [("content-type", "application/x-protobuf")],
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
