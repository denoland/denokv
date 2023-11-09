use std::net::SocketAddr;

use clap::Parser;

#[derive(Parser)]
pub struct Config {
  /// The path to the SQLite database KV will persist to.
  #[clap(long, env = "DENO_KV_SQLITE_PATH")]
  pub sqlite_path: String,

  #[command(subcommand)]
  pub subcommand: SubCmd,
}

#[derive(Parser)]
pub enum SubCmd {
  /// Starts the Deno KV HTTP server.
  Serve(ServeOptions),

  /// Point-in-time recovery tools.
  Pitr(PitrOptions),
}

#[derive(Parser)]
pub struct ServeOptions {
  /// The access token used by the CLI to connect to this KV instance.
  #[clap(long, env = "DENO_KV_ACCESS_TOKEN")]
  pub access_token: String,

  /// The address to bind the Deno KV HTTP endpoint to.
  #[clap(long = "addr", default_value = "0.0.0.0:4512")]
  pub addr: SocketAddr,

  /// Open in read-only mode.
  #[clap(long)]
  pub read_only: bool,

  /// Sync changes from S3 continuously.
  #[clap(long, conflicts_with = "read_only")]
  pub sync_from_s3: bool,

  #[command(flatten)]
  pub replica: ReplicaOptions,
}

#[derive(Parser)]
pub struct ReplicaOptions {
  /// The name of the S3 bucket to sync changes from.
  #[clap(long, env = "DENO_KV_S3_BUCKET")]
  pub s3_bucket: Option<String>,

  /// Prefix in the S3 bucket.
  #[clap(long, env = "DENO_KV_S3_PREFIX")]
  pub s3_prefix: Option<String>,

  /// S3 endpoint URL like `https://storage.googleapis.com`.
  /// Only needed for S3-compatible services other than Amazon S3.
  #[clap(long, env = "DENO_KV_S3_ENDPOINT")]
  pub s3_endpoint: Option<String>,
}

#[derive(Parser)]
pub struct PitrOptions {
  #[command(subcommand)]
  pub subcommand: PitrSubCmd,
}

#[derive(Parser)]
pub enum PitrSubCmd {
  /// Sync changes from S3 to the SQLite database, without updating the current snapshot.
  Sync(SyncOptions),

  /// List available recovery points.
  List(PitrListOptions),

  /// Show information about the current snapshot.
  Info,

  /// Checkout the snapshot at a specific versionstamp.
  Checkout(PitrCheckoutOptions),
}

#[derive(Parser)]
pub struct SyncOptions {
  #[command(flatten)]
  pub replica: ReplicaOptions,
}

#[derive(Parser)]
pub struct PitrListOptions {
  /// Start time in RFC3339 format, e.g. 2021-01-01T00:00:00Z
  #[clap(long = "start")]
  pub start: Option<String>,

  /// End time in RFC3339 format, e.g. 2021-01-01T00:00:00Z
  #[clap(long = "end")]
  pub end: Option<String>,
}

#[derive(Parser)]
pub struct PitrCheckoutOptions {
  pub versionstamp: String,
}
