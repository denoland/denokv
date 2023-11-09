#[derive(Clone, Debug)]
pub struct KeyMetadata {
  pub versionstamp: [u8; 10],
  pub value_encoding: i32,
  pub expire_at_ms: i64,
}

impl KeyMetadata {
  pub fn decode(raw: &[u8]) -> Option<Self> {
    if raw.len() < 11 {
      return None;
    }

    let mut versionstamp = [0; 10];
    versionstamp.copy_from_slice(&raw[0..10]);
    let value_encoding = raw[10] as i32;

    let expire_at_ms = if raw.len() >= 19 {
      i64::from_le_bytes(raw[11..19].try_into().unwrap())
    } else {
      -1
    };

    Some(Self {
      versionstamp,
      value_encoding,
      expire_at_ms,
    })
  }

  pub fn encode(&self) -> Vec<u8> {
    let mut buf = Vec::with_capacity(10 + 1 + 8);
    buf.extend_from_slice(&self.versionstamp);
    buf.push(self.value_encoding as u8);
    buf.extend_from_slice(&self.expire_at_ms.to_le_bytes());
    buf
  }
}
