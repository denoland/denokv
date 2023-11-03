use std::net::SocketAddr;

use clap::Parser;

#[derive(Parser)]
pub struct Config {
  /// The access token used by the CLI to connect to this KV instance.
  #[clap(long, env = "DENO_KV_ACCESS_TOKEN")]
  pub access_token: String,

  /// The path to the SQLite database KV will persist to.
  #[clap(long, env = "DENO_KV_SQLITE_PATH")]
  pub sqlite_path: String,

  /// The address to bind the Deno KV HTTP endpoint to.
  #[clap(long = "addr", default_value = "0.0.0.0:4512")]
  pub addr: SocketAddr,
}
