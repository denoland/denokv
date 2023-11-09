use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::process::Stdio;

use denokv_proto::AtomicWrite;
use denokv_proto::Database;
use denokv_proto::ReadRange;
use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;

const ACCESS_TOKEN: &str = "1234abcd5678efgh";

fn denokv_exe() -> PathBuf {
  let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
  path.push("../target");
  if cfg!(debug_assertions) {
    path.push("debug");
  } else {
    path.push("release");
  }
  if cfg!(target_os = "windows") {
    path.push("denokv.exe");
  } else {
    path.push("denokv");
  }
  path
}

async fn start_server() -> (tokio::process::Child, SocketAddr) {
  let tmp_file = tempfile::NamedTempFile::new().unwrap().keep().unwrap().1;
  let mut child = tokio::process::Command::new(denokv_exe())
    .arg("--sqlite-path")
    .arg(tmp_file)
    .arg("serve")
    .arg("--addr")
    .arg("127.0.0.1:0")
    .env("DENO_KV_ACCESS_TOKEN", ACCESS_TOKEN)
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .kill_on_drop(true)
    .spawn()
    .unwrap();

  let stdout = child.stdout.take().unwrap();
  let stdout_buf = BufReader::new(stdout);
  let mut stdout_lines = stdout_buf.lines();

  tokio::spawn(async move {
    // Forward stdout to our stdout.
    while let Some(line) = stdout_lines.next_line().await.unwrap() {
      println!("{}", line);
    }
  });

  let stderr = child.stderr.take().unwrap();
  let stderr_buf = BufReader::new(stderr);
  let mut stderr_lines = stderr_buf.lines();

  let addr = loop {
    let line = stderr_lines
      .next_line()
      .await
      .expect("server died")
      .expect("server died");
    eprintln!("{}", line);
    if let Some((_, addr)) = line.split_once("Listening on http://") {
      break addr.parse().unwrap();
    }
  };

  tokio::spawn(async move {
    // Forward stderr to our stderr.
    while let Some(line) = stderr_lines.next_line().await.unwrap() {
      eprintln!("{}", line);
    }
  });

  println!("Server started and listening on {}", addr);

  (child, addr)
}

struct DummyPermissions;

impl denokv_remote::RemotePermissions for DummyPermissions {
  fn check_net_url(&self, _url: &reqwest::Url) -> Result<(), anyhow::Error> {
    Ok(())
  }
}

#[tokio::test]
async fn basics() {
  let (_child, addr) = start_server().await;
  let client = reqwest::Client::new();
  let url = format!("http://localhost:{}", addr.port()).parse().unwrap();

  let metadata_endpoint = denokv_remote::MetadataEndpoint {
    url,
    access_token: ACCESS_TOKEN.to_string(),
  };

  let remote =
    denokv_remote::Remote::new(client, DummyPermissions, metadata_endpoint);

  let ranges = remote
    .snapshot_read(
      vec![ReadRange {
        start: vec![],
        end: vec![0xff],
        limit: NonZeroU32::try_from(10).unwrap(),
        reverse: false,
      }],
      denokv_proto::SnapshotReadOptions {
        consistency: denokv_proto::Consistency::Strong,
      },
    )
    .await
    .unwrap();
  assert_eq!(ranges.len(), 1);
  let range = &ranges[0];
  assert_eq!(range.entries.len(), 0);

  let commit_result = remote
    .atomic_write(AtomicWrite {
      checks: vec![],
      mutations: vec![denokv_proto::Mutation {
        key: vec![1],
        kind: denokv_proto::MutationKind::Set(denokv_proto::KvValue::U64(1)),
        expire_at: None,
      }],
      enqueues: vec![],
    })
    .await
    .unwrap()
    .expect("commit success");
  assert_ne!(commit_result.versionstamp, [0; 10]);

  let ranges = remote
    .snapshot_read(
      vec![ReadRange {
        start: vec![],
        end: vec![0xff],
        limit: NonZeroU32::try_from(10).unwrap(),
        reverse: false,
      }],
      denokv_proto::SnapshotReadOptions {
        consistency: denokv_proto::Consistency::Strong,
      },
    )
    .await
    .unwrap();
  assert_eq!(ranges.len(), 1);
  let range = &ranges[0];
  assert_eq!(range.entries.len(), 1);
  assert_eq!(range.entries[0].key, vec![1]);
  assert!(matches!(
    range.entries[0].value,
    denokv_proto::KvValue::U64(1)
  ));
  assert_eq!(range.entries[0].versionstamp, commit_result.versionstamp);

  println!("remote");
}

#[tokio::test]
async fn no_auth() {
  let (_child, addr) = start_server().await;
  let client = reqwest::Client::new();
  let url = format!("http://localhost:{}", addr.port()).parse().unwrap();

  let metadata_endpoint = denokv_remote::MetadataEndpoint {
    url,
    access_token: "".to_string(),
  };

  let remote =
    denokv_remote::Remote::new(client, DummyPermissions, metadata_endpoint);

  let res = remote
    .snapshot_read(
      vec![ReadRange {
        start: vec![],
        end: vec![0xff],
        limit: NonZeroU32::try_from(10).unwrap(),
        reverse: false,
      }],
      denokv_proto::SnapshotReadOptions {
        consistency: denokv_proto::Consistency::Strong,
      },
    )
    .await;
  assert!(res.is_err());
}
