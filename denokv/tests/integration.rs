use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use bytes::Bytes;
use deno_error::JsErrorBox;
use denokv_proto::AtomicWrite;
use denokv_proto::Database;
use denokv_proto::KvValue;
use denokv_proto::ReadRange;
use denokv_proto::WatchKeyOutput;
use denokv_remote::RemotePermissions;
use denokv_remote::RemoteResponse;
use denokv_remote::RemoteTransport;
use futures::Stream;
use futures::StreamExt;
use futures::TryStreamExt;
use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;
use tokio::task::LocalSet;
use url::Url;
use v8_valueserializer::value_eq;
use v8_valueserializer::Heap;
use v8_valueserializer::Value;
use v8_valueserializer::ValueDeserializer;
use v8_valueserializer::ValueSerializer;

const ACCESS_TOKEN: &str = "1234abcd5678efgh";

#[derive(Clone)]
struct ReqwestClient(reqwest::Client);
struct ReqwestResponse(reqwest::Response);

impl RemoteTransport for ReqwestClient {
  type Response = ReqwestResponse;
  async fn post(
    &self,
    url: Url,
    headers: http::HeaderMap,
    body: Bytes,
  ) -> Result<(Url, http::StatusCode, Self::Response), JsErrorBox> {
    let res = self
      .0
      .post(url)
      .headers(headers)
      .body(body)
      .send()
      .await
      .map_err(|e| JsErrorBox::generic(e.to_string()))?;
    let url = res.url().clone();
    let status = res.status();
    Ok((url, status, ReqwestResponse(res)))
  }
}

impl RemoteResponse for ReqwestResponse {
  async fn bytes(self) -> Result<Bytes, JsErrorBox> {
    self
      .0
      .bytes()
      .await
      .map_err(|e| JsErrorBox::generic(e.to_string()))
  }
  fn stream(
    self,
  ) -> impl Stream<Item = Result<Bytes, JsErrorBox>> + Send + Sync {
    self
      .0
      .bytes_stream()
      .map_err(|e| JsErrorBox::generic(e.to_string()))
  }
  async fn text(self) -> Result<String, JsErrorBox> {
    self
      .0
      .text()
      .await
      .map_err(|e| JsErrorBox::generic(e.to_string()))
  }
}

/// Like [`ReqwestClient`], but capable of opening WebSocket connections, so
/// it negotiates protocol version 4 and watches go through the multiplexed
/// watch channel.
#[derive(Clone)]
struct WsReqwestClient(reqwest::Client);

impl RemoteTransport for WsReqwestClient {
  type Response = ReqwestResponse;
  async fn post(
    &self,
    url: Url,
    headers: http::HeaderMap,
    body: Bytes,
  ) -> Result<(Url, http::StatusCode, Self::Response), JsErrorBox> {
    ReqwestClient(self.0.clone()).post(url, headers, body).await
  }

  fn supports_watch_channel(&self) -> bool {
    true
  }

  async fn websocket(
    &self,
    url: Url,
    headers: http::HeaderMap,
  ) -> Result<
    (
      denokv_remote::WebSocketMessageSink,
      denokv_remote::WebSocketMessageStream,
    ),
    JsErrorBox,
  > {
    use futures::SinkExt;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::tungstenite::Message;

    let mut request = url
      .as_str()
      .into_client_request()
      .map_err(|e| JsErrorBox::generic(e.to_string()))?;
    for (name, value) in headers.iter() {
      request.headers_mut().insert(name.clone(), value.clone());
    }
    let (socket, _response) =
      tokio_tungstenite::connect_async(request)
        .await
        .map_err(|e| JsErrorBox::generic(e.to_string()))?;
    let (sink, stream) = futures::StreamExt::split(socket);
    let sink = Box::pin(
      sink
        .with(|bytes: Bytes| async move {
          Ok::<_, tokio_tungstenite::tungstenite::Error>(Message::Binary(bytes))
        })
        .sink_map_err(|e: tokio_tungstenite::tungstenite::Error| {
          JsErrorBox::generic(e.to_string())
        }),
    );
    let stream =
      Box::pin(futures::stream::unfold(stream, |mut stream| async move {
        loop {
          match stream.next().await {
            Some(Ok(Message::Binary(bytes))) => {
              return Some((Ok(bytes), stream))
            }
            // Control frames are handled by tungstenite.
            Some(Ok(Message::Ping(_)))
            | Some(Ok(Message::Pong(_)))
            | Some(Ok(Message::Frame(_))) => continue,
            Some(Ok(Message::Text(_))) => {
              return Some((
                Err(JsErrorBox::generic("unexpected text message")),
                stream,
              ))
            }
            Some(Ok(Message::Close(_))) | None => return None,
            Some(Err(e)) => {
              return Some((Err(JsErrorBox::generic(e.to_string())), stream))
            }
          }
        }
      }));
    Ok((sink, stream))
  }
}

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
      println!("{line}");
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
    eprintln!("{line}");
    if let Some((_, addr)) = line.split_once("Listening on http://") {
      break addr.parse().unwrap();
    }
  };

  tokio::spawn(async move {
    // Forward stderr to our stderr.
    while let Some(line) = stderr_lines.next_line().await.unwrap() {
      eprintln!("{line}");
    }
  });

  println!("Server started and listening on {addr}");

  (child, addr)
}

#[derive(Clone)]
struct DummyPermissions;

impl denokv_remote::RemotePermissions for DummyPermissions {
  fn check_net_url(&self, _url: &reqwest::Url) -> Result<(), JsErrorBox> {
    Ok(())
  }
}

#[tokio::test]
async fn basics() {
  let (_child, addr) = start_server().await;
  let client = ReqwestClient(reqwest::Client::new());
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
async fn watch() {
  let (_child, addr) = start_server().await;
  let client = ReqwestClient(reqwest::Client::new());
  let url = format!("http://localhost:{}", addr.port()).parse().unwrap();

  let metadata_endpoint = denokv_remote::MetadataEndpoint {
    url,
    access_token: ACCESS_TOKEN.to_string(),
  };

  let remote =
    denokv_remote::Remote::new(client, DummyPermissions, metadata_endpoint);
  let remote2 = remote.clone();
  let local = LocalSet::new();
  let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
  local.spawn_local(async move {
    let mut s = remote2.watch(vec![vec![1]]);
    while let Some(w) = s.next().await {
      let w = w.expect("watch success");
      eprintln!("Watch output: {w:?}");
      for w in w {
        if let WatchKeyOutput::Changed { entry: Some(_) } = w {
          tx.send(w).expect("send success");
          return;
        }
      }
    }
  });
  local.spawn_local(async move {
    for i in 0..10 {
      _ = remote
        .atomic_write(AtomicWrite {
          checks: vec![],
          mutations: vec![denokv_proto::Mutation {
            key: vec![1],
            kind: denokv_proto::MutationKind::Set(denokv_proto::KvValue::U64(
              i,
            )),
            expire_at: None,
          }],
          enqueues: vec![],
        })
        .await
        .unwrap()
        .expect("commit success");
    }
  });
  tokio::time::timeout(Duration::from_secs(60), local)
    .await
    .expect("no timeout");
  let w = rx.try_recv().expect("recv success");
  let WatchKeyOutput::Changed { entry: Some(entry) } = w else {
    panic!("Unexpected watch result");
  };
  assert_eq!(entry.key, vec![1]);
}

async fn set_key<P: RemotePermissions, T: RemoteTransport>(
  remote: &denokv_remote::Remote<P, T>,
  key: Vec<u8>,
  value: u64,
) {
  remote
    .atomic_write(AtomicWrite {
      checks: vec![],
      mutations: vec![denokv_proto::Mutation {
        key,
        kind: denokv_proto::MutationKind::Set(KvValue::U64(value)),
        expire_at: None,
      }],
      enqueues: vec![],
    })
    .await
    .unwrap()
    .expect("commit success");
}

#[tokio::test]
async fn watch_channel() {
  let (_child, addr) = start_server().await;
  let client = WsReqwestClient(reqwest::Client::new());
  let url = format!("http://localhost:{}", addr.port()).parse().unwrap();

  let metadata_endpoint = denokv_remote::MetadataEndpoint {
    url,
    access_token: ACCESS_TOKEN.to_string(),
  };

  let remote =
    denokv_remote::Remote::new(client, DummyPermissions, metadata_endpoint);

  set_key(&remote, vec![1], 42).await;

  let mut watch = remote.watch(vec![vec![1], vec![2]]);

  // The first item is a full snapshot: key 1 has a value, key 2 does not.
  let first = tokio::time::timeout(Duration::from_secs(30), watch.next())
    .await
    .expect("no timeout")
    .unwrap()
    .expect("watch success");
  assert_eq!(first.len(), 2);
  let WatchKeyOutput::Changed { entry: Some(entry) } = &first[0] else {
    panic!("expected key 1 to have a value: {first:?}");
  };
  assert_eq!(entry.key, vec![1]);
  assert!(matches!(&first[1], WatchKeyOutput::Changed { entry: None }));

  // A write to one key produces an update marking only that key changed.
  set_key(&remote, vec![2], 1).await;
  let second = tokio::time::timeout(Duration::from_secs(30), watch.next())
    .await
    .expect("no timeout")
    .unwrap()
    .expect("watch success");
  assert!(matches!(&second[0], WatchKeyOutput::Unchanged));
  let WatchKeyOutput::Changed { entry: Some(entry) } = &second[1] else {
    panic!("expected key 2 to have a value: {second:?}");
  };
  assert_eq!(entry.key, vec![2]);

  // A second watch shares the same channel and gets its own initial
  // snapshot, while the first watch keeps working.
  let mut watch2 = remote.watch(vec![vec![1]]);
  let first2 = tokio::time::timeout(Duration::from_secs(30), watch2.next())
    .await
    .expect("no timeout")
    .unwrap()
    .expect("watch success");
  assert_eq!(first2.len(), 1);
  assert!(matches!(
    &first2[0],
    WatchKeyOutput::Changed { entry: Some(_) }
  ));

  set_key(&remote, vec![1], 43).await;
  for (name, watch) in [("watch1", &mut watch), ("watch2", &mut watch2)] {
    loop {
      let outputs = tokio::time::timeout(Duration::from_secs(30), watch.next())
        .await
        .unwrap_or_else(|_| panic!("{name} timed out"))
        .unwrap()
        .expect("watch success");
      // The re-add for watch2's subscription may produce a redundant
      // update for key 1; skip until the write is observed.
      if let Some(WatchKeyOutput::Changed { entry: Some(entry) }) =
        outputs.first()
      {
        if matches!(entry.value, KvValue::U64(43)) {
          break;
        }
      }
    }
  }
}

/// A transport that claims WebSocket support but whose connections always
/// fail — as seen through proxies that strip the Upgrade header.
#[derive(Clone)]
struct BrokenWsClient(reqwest::Client);

impl RemoteTransport for BrokenWsClient {
  type Response = ReqwestResponse;
  async fn post(
    &self,
    url: Url,
    headers: http::HeaderMap,
    body: Bytes,
  ) -> Result<(Url, http::StatusCode, Self::Response), JsErrorBox> {
    ReqwestClient(self.0.clone()).post(url, headers, body).await
  }

  fn supports_watch_channel(&self) -> bool {
    true
  }

  async fn websocket(
    &self,
    _url: Url,
    _headers: http::HeaderMap,
  ) -> Result<
    (
      denokv_remote::WebSocketMessageSink,
      denokv_remote::WebSocketMessageStream,
    ),
    JsErrorBox,
  > {
    Err(JsErrorBox::generic("upgrade blocked"))
  }
}

#[tokio::test]
async fn watch_channel_falls_back_when_ws_unavailable() {
  let (_child, addr) = start_server().await;
  let client = BrokenWsClient(reqwest::Client::new());
  let url = format!("http://localhost:{}", addr.port()).parse().unwrap();

  let metadata_endpoint = denokv_remote::MetadataEndpoint {
    url,
    access_token: ACCESS_TOKEN.to_string(),
  };

  let remote =
    denokv_remote::Remote::new(client, DummyPermissions, metadata_endpoint);

  set_key(&remote, vec![7], 7).await;

  // The server negotiates v4, every websocket attempt fails, and the watch
  // must still deliver via the per-watch streaming fallback.
  let mut watch = remote.watch(vec![vec![7]]);
  let first = tokio::time::timeout(Duration::from_secs(30), watch.next())
    .await
    .expect("no timeout")
    .unwrap()
    .expect("watch success");
  let WatchKeyOutput::Changed { entry: Some(entry) } = &first[0] else {
    panic!("expected key 7 to have a value: {first:?}");
  };
  assert_eq!(entry.key, vec![7]);
}

#[tokio::test]
async fn watch_channel_empty_keys_yields_initial_snapshot() {
  let (_child, addr) = start_server().await;
  let client = WsReqwestClient(reqwest::Client::new());
  let url = format!("http://localhost:{}", addr.port()).parse().unwrap();

  let metadata_endpoint = denokv_remote::MetadataEndpoint {
    url,
    access_token: ACCESS_TOKEN.to_string(),
  };

  let remote =
    denokv_remote::Remote::new(client, DummyPermissions, metadata_endpoint);

  let mut watch = remote.watch(vec![]);
  let first = tokio::time::timeout(Duration::from_secs(30), watch.next())
    .await
    .expect("no timeout")
    .unwrap()
    .expect("watch success");
  assert!(first.is_empty());
}

#[tokio::test]
async fn watch_channel_resumes_across_subscriptions() {
  let (_child, addr) = start_server().await;
  let client = WsReqwestClient(reqwest::Client::new());
  let url = format!("http://localhost:{}", addr.port()).parse().unwrap();

  let metadata_endpoint = denokv_remote::MetadataEndpoint {
    url,
    access_token: ACCESS_TOKEN.to_string(),
  };

  let remote =
    denokv_remote::Remote::new(client, DummyPermissions, metadata_endpoint);

  // Open and fully drain a first watch, then drop it.
  set_key(&remote, vec![9], 1).await;
  {
    let mut watch = remote.watch(vec![vec![9]]);
    let first = tokio::time::timeout(Duration::from_secs(30), watch.next())
      .await
      .expect("no timeout")
      .unwrap()
      .expect("watch success");
    assert!(matches!(
      &first[0],
      WatchKeyOutput::Changed { entry: Some(_) }
    ));
  }

  // A new watch for the same key must again receive the current state.
  let mut watch = remote.watch(vec![vec![9]]);
  let first = tokio::time::timeout(Duration::from_secs(30), watch.next())
    .await
    .expect("no timeout")
    .unwrap()
    .expect("watch success");
  let WatchKeyOutput::Changed { entry: Some(entry) } = &first[0] else {
    panic!("expected key 9 to have a value: {first:?}");
  };
  assert!(matches!(entry.value, KvValue::U64(1)));
}

#[tokio::test]
async fn no_auth() {
  let (_child, addr) = start_server().await;
  let client = ReqwestClient(reqwest::Client::new());
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

#[tokio::test]
async fn sum_type_mismatch() {
  let (_child, addr) = start_server().await;
  let client = ReqwestClient(reqwest::Client::new());
  let url = format!("http://localhost:{}", addr.port()).parse().unwrap();

  let metadata_endpoint = denokv_remote::MetadataEndpoint {
    url,
    access_token: ACCESS_TOKEN.to_string(),
  };

  let remote =
    denokv_remote::Remote::new(client, DummyPermissions, metadata_endpoint);
  let commit_result = remote
    .atomic_write(AtomicWrite {
      checks: vec![],
      mutations: vec![denokv_proto::Mutation {
        key: vec![1],
        kind: denokv_proto::MutationKind::Set(denokv_proto::KvValue::Bytes(
          vec![1, 2, 3],
        )),
        expire_at: None,
      }],
      enqueues: vec![],
    })
    .await
    .unwrap()
    .expect("commit success");
  assert_ne!(commit_result.versionstamp, [0; 10]);

  let res = remote
    .atomic_write(AtomicWrite {
      checks: vec![],
      mutations: vec![denokv_proto::Mutation {
        key: vec![1],
        kind: denokv_proto::MutationKind::Sum {
          value: denokv_proto::KvValue::U64(1),
          min_v8: vec![],
          max_v8: vec![],
          clamp: false,
        },
        expire_at: None,
      }],
      enqueues: vec![],
    })
    .await;

  let err = res.unwrap_err();
  assert!(err.to_string().contains(
    "Failed to perform 'sum' mutation on a non-U64 value in the database"
  ));

  let res = remote
    .atomic_write(AtomicWrite {
      checks: vec![],
      mutations: vec![denokv_proto::Mutation {
        key: vec![1],
        kind: denokv_proto::MutationKind::Sum {
          value: denokv_proto::KvValue::V8(
            ValueSerializer::default()
              .finish(&Heap::default(), &Value::BigInt(1.into()))
              .unwrap(),
          ),
          min_v8: vec![],
          max_v8: vec![],
          clamp: false,
        },
        expire_at: None,
      }],
      enqueues: vec![],
    })
    .await;

  let err = res.unwrap_err();
  assert!(err.to_string().contains("unsupported value type"));
}

#[tokio::test]
async fn sum_values() {
  let (_child, addr) = start_server().await;
  let client = ReqwestClient(reqwest::Client::new());
  let url = format!("http://localhost:{}", addr.port()).parse().unwrap();

  let metadata_endpoint = denokv_remote::MetadataEndpoint {
    url,
    access_token: ACCESS_TOKEN.to_string(),
  };

  let remote =
    denokv_remote::Remote::new(client, DummyPermissions, metadata_endpoint);

  // Sum(nothing, u64) -> u64
  let commit_result = remote
    .atomic_write(AtomicWrite {
      checks: vec![],
      mutations: vec![denokv_proto::Mutation {
        key: vec![1],
        kind: denokv_proto::MutationKind::Sum {
          value: denokv_proto::KvValue::U64(42),
          min_v8: vec![],
          max_v8: vec![],
          clamp: false,
        },
        expire_at: None,
      }],
      enqueues: vec![],
    })
    .await
    .unwrap()
    .expect("commit success");
  assert_ne!(commit_result.versionstamp, [0; 10]);

  // Old sum semantics: u64 + u64 -> u64
  remote
    .atomic_write(AtomicWrite {
      checks: vec![],
      mutations: vec![denokv_proto::Mutation {
        key: vec![1],
        kind: denokv_proto::MutationKind::Sum {
          value: denokv_proto::KvValue::U64(1),
          min_v8: vec![],
          max_v8: vec![],
          clamp: false,
        },
        expire_at: None,
      }],
      enqueues: vec![],
    })
    .await
    .unwrap()
    .expect("commit success");

  let entry = read_key_1(&remote).await;
  assert!(matches!(entry.value, denokv_proto::KvValue::U64(43)));

  // Backward compat: u64 + v8(bigint) -> u64
  remote
    .atomic_write(AtomicWrite {
      checks: vec![],
      mutations: vec![denokv_proto::Mutation {
        key: vec![1],
        kind: denokv_proto::MutationKind::Sum {
          value: denokv_proto::KvValue::V8(
            ValueSerializer::default()
              .finish(&Heap::default(), &Value::BigInt(1.into()))
              .unwrap(),
          ),
          min_v8: vec![],
          max_v8: vec![],
          clamp: false,
        },
        expire_at: None,
      }],
      enqueues: vec![],
    })
    .await
    .unwrap()
    .expect("commit success");

  let entry = read_key_1(&remote).await;
  assert!(matches!(entry.value, denokv_proto::KvValue::U64(44)));

  // Reset to v8(bigint)
  remote
    .atomic_write(AtomicWrite {
      checks: vec![],
      mutations: vec![denokv_proto::Mutation {
        key: vec![1],
        kind: denokv_proto::MutationKind::Set(denokv_proto::KvValue::V8(
          ValueSerializer::default()
            .finish(&Heap::default(), &Value::BigInt(42.into()))
            .unwrap(),
        )),
        expire_at: None,
      }],
      enqueues: vec![],
    })
    .await
    .unwrap()
    .expect("commit success");
  let entry = read_key_1(&remote).await;
  let KvValue::V8(value) = &entry.value else {
    panic!("expected v8 value");
  };
  let (value, heap) = ValueDeserializer::default().read(value).unwrap();
  assert!(value_eq(
    (&value, &heap),
    (&Value::BigInt(42.into()), &heap)
  ));

  // v8(bigint) + v8(bigint) -> v8(bigint)
  remote
    .atomic_write(AtomicWrite {
      checks: vec![],
      mutations: vec![denokv_proto::Mutation {
        key: vec![1],
        kind: denokv_proto::MutationKind::Sum {
          value: denokv_proto::KvValue::V8(
            ValueSerializer::default()
              .finish(&Heap::default(), &Value::BigInt(1.into()))
              .unwrap(),
          ),
          min_v8: vec![],
          max_v8: vec![],
          clamp: false,
        },
        expire_at: None,
      }],
      enqueues: vec![],
    })
    .await
    .unwrap()
    .expect("commit success");
  let entry = read_key_1(&remote).await;
  let KvValue::V8(value) = &entry.value else {
    panic!("expected v8 value");
  };
  let (value, heap) = ValueDeserializer::default().read(value).unwrap();
  assert!(value_eq(
    (&value, &heap),
    (&Value::BigInt(43.into()), &heap)
  ));

  // v8(bigint) + v8(number) -> error
  let res = remote
    .atomic_write(AtomicWrite {
      checks: vec![],
      mutations: vec![denokv_proto::Mutation {
        key: vec![1],
        kind: denokv_proto::MutationKind::Sum {
          value: denokv_proto::KvValue::V8(
            ValueSerializer::default()
              .finish(&Heap::default(), &Value::I32(1))
              .unwrap(),
          ),
          min_v8: vec![],
          max_v8: vec![],
          clamp: false,
        },
        expire_at: None,
      }],
      enqueues: vec![],
    })
    .await;

  let err = res.unwrap_err();
  assert!(err.to_string().contains("Cannot sum BigInt with Number"));

  // clamp=false error
  let res = remote
    .atomic_write(AtomicWrite {
      checks: vec![],
      mutations: vec![denokv_proto::Mutation {
        key: vec![1],
        kind: denokv_proto::MutationKind::Sum {
          value: denokv_proto::KvValue::V8(
            ValueSerializer::default()
              .finish(&Heap::default(), &Value::BigInt(2.into()))
              .unwrap(),
          ),
          min_v8: vec![],
          max_v8: ValueSerializer::default()
            .finish(&Heap::default(), &Value::BigInt(44.into()))
            .unwrap(),
          clamp: false,
        },
        expire_at: None,
      }],
      enqueues: vec![],
    })
    .await;

  let err = res.unwrap_err();
  assert!(
    err
      .to_string()
      .contains("The result of a Sum operation would exceed its range limit"),
    "{}",
    err
  );

  // clamp=false ok
  remote
    .atomic_write(AtomicWrite {
      checks: vec![],
      mutations: vec![denokv_proto::Mutation {
        key: vec![1],
        kind: denokv_proto::MutationKind::Sum {
          value: denokv_proto::KvValue::V8(
            ValueSerializer::default()
              .finish(&Heap::default(), &Value::BigInt(1.into()))
              .unwrap(),
          ),
          min_v8: vec![],
          max_v8: ValueSerializer::default()
            .finish(&Heap::default(), &Value::BigInt(44.into()))
            .unwrap(),
          clamp: false,
        },
        expire_at: None,
      }],
      enqueues: vec![],
    })
    .await
    .unwrap()
    .expect("commit success");
  let entry = read_key_1(&remote).await;
  let KvValue::V8(value) = &entry.value else {
    panic!("expected v8 value");
  };
  let (value, heap) = ValueDeserializer::default().read(value).unwrap();
  assert!(value_eq(
    (&value, &heap),
    (&Value::BigInt(44.into()), &heap)
  ));

  // clamp=true
  remote
    .atomic_write(AtomicWrite {
      checks: vec![],
      mutations: vec![denokv_proto::Mutation {
        key: vec![1],
        kind: denokv_proto::MutationKind::Sum {
          value: denokv_proto::KvValue::V8(
            ValueSerializer::default()
              .finish(&Heap::default(), &Value::BigInt(10.into()))
              .unwrap(),
          ),
          min_v8: vec![],
          max_v8: ValueSerializer::default()
            .finish(&Heap::default(), &Value::BigInt(50.into()))
            .unwrap(),
          clamp: true,
        },
        expire_at: None,
      }],
      enqueues: vec![],
    })
    .await
    .unwrap()
    .expect("commit success");
  let entry = read_key_1(&remote).await;
  let KvValue::V8(value) = &entry.value else {
    panic!("expected v8 value");
  };
  let (value, heap) = ValueDeserializer::default().read(value).unwrap();
  assert!(value_eq(
    (&value, &heap),
    (&Value::BigInt(50.into()), &heap)
  ));

  // Sum(nothing, v8(bigint)) -> v8(bigint)
  let commit_result = remote
    .atomic_write(AtomicWrite {
      checks: vec![],
      mutations: vec![denokv_proto::Mutation {
        key: vec![2],
        kind: denokv_proto::MutationKind::Sum {
          value: denokv_proto::KvValue::V8(
            ValueSerializer::default()
              .finish(&Heap::default(), &Value::BigInt(10.into()))
              .unwrap(),
          ),
          min_v8: vec![],
          max_v8: vec![],
          clamp: false,
        },
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
        start: vec![2],
        end: vec![3],
        limit: NonZeroU32::try_from(1).unwrap(),
        reverse: false,
      }],
      denokv_proto::SnapshotReadOptions {
        consistency: denokv_proto::Consistency::Strong,
      },
    )
    .await
    .unwrap();
  assert_eq!(ranges.len(), 1);
  let range = ranges.into_iter().next().unwrap();
  assert_eq!(range.entries.len(), 1);
  assert_eq!(range.entries[0].key, vec![2]);
  let KvValue::V8(value) = &range.entries[0].value else {
    panic!("expected v8 value");
  };
  let (value, heap) = ValueDeserializer::default().read(value).unwrap();
  assert!(value_eq(
    (&value, &heap),
    (&Value::BigInt(10.into()), &heap)
  ));
}

async fn read_key_1<P: RemotePermissions, T: RemoteTransport>(
  remote: &denokv_remote::Remote<P, T>,
) -> denokv_proto::KvEntry {
  let ranges = remote
    .snapshot_read(
      vec![ReadRange {
        start: vec![1],
        end: vec![2],
        limit: NonZeroU32::try_from(1).unwrap(),
        reverse: false,
      }],
      denokv_proto::SnapshotReadOptions {
        consistency: denokv_proto::Consistency::Strong,
      },
    )
    .await
    .unwrap();
  assert_eq!(ranges.len(), 1);
  let range = ranges.into_iter().next().unwrap();
  assert_eq!(range.entries.len(), 1);
  assert_eq!(range.entries[0].key, vec![1]);
  range.entries.into_iter().next().unwrap()
}

#[tokio::test]
async fn atomic_write_reports_failed_checks() {
  let (_child, addr) = start_server().await;
  let client = ReqwestClient(reqwest::Client::new());
  let url = format!("http://localhost:{}", addr.port()).parse().unwrap();

  let metadata_endpoint = denokv_remote::MetadataEndpoint {
    url,
    access_token: ACCESS_TOKEN.to_string(),
  };

  let remote =
    denokv_remote::Remote::new(client, DummyPermissions, metadata_endpoint);

  remote
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

  // The first and third checks expect the (existing) key [1] to be
  // absent and must fail; the second check expects the absent key [2]
  // to be absent and must pass.
  let write = denokv_proto::datapath::AtomicWrite {
    checks: vec![
      denokv_proto::datapath::Check {
        key: vec![1],
        versionstamp: vec![],
      },
      denokv_proto::datapath::Check {
        key: vec![2],
        versionstamp: vec![],
      },
      denokv_proto::datapath::Check {
        key: vec![1],
        versionstamp: vec![],
      },
    ],
    mutations: vec![],
    enqueues: vec![],
  };

  let response = reqwest::Client::new()
    .post(format!("http://localhost:{}/v2/atomic_write", addr.port()))
    .header("authorization", format!("Bearer {ACCESS_TOKEN}"))
    .header("x-denokv-version", "3")
    .header(
      "x-denokv-database-id",
      "00000000-0000-0000-0000-000000000000",
    )
    .body(prost::Message::encode_to_vec(&write))
    .send()
    .await
    .unwrap();
  assert_eq!(response.status(), 200);
  let output: denokv_proto::datapath::AtomicWriteOutput =
    prost::Message::decode(response.bytes().await.unwrap()).unwrap();

  assert_eq!(
    output.status(),
    denokv_proto::datapath::AtomicWriteStatus::AwCheckFailure
  );
  assert_eq!(output.failed_checks, vec![0, 2]);
}
