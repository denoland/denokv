// Copyright 2018-2023 the Deno authors. All rights reserved. MIT license.

use std::io;

#[cfg(feature = "build_protos")]
mod build_protos {
  use std::env;
  use std::io;
  use std::path::Path;
  use std::path::PathBuf;
  fn compiled_proto_path(proto: impl AsRef<Path>) -> PathBuf {
    let proto = proto.as_ref();
    let mut path = PathBuf::from(env::var("OUT_DIR").unwrap());
    path.push(format!(
      "com.deno.kv.{}.rs",
      proto.file_stem().unwrap().to_str().unwrap()
    ));
    path
  }

  fn protobuf_module_path() -> PathBuf {
    PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap()).join("protobuf")
  }

  fn protobuf_dest_path(compiled_proto_path: impl AsRef<Path>) -> PathBuf {
    let proto = compiled_proto_path.as_ref();
    let mut path = protobuf_module_path();
    path.push(proto.file_name().unwrap());
    path
  }

  fn copy_compiled_proto(proto: impl AsRef<Path>) -> io::Result<()> {
    let generated = compiled_proto_path(proto);
    let contents = std::fs::read_to_string(&generated)?;
    let header = [
      "// Copyright 2018-2024 the Deno authors. All rights reserved. MIT license.\n",
      "// Generated by build.rs, enable the `build_protos` feature to regeneraten\n",
    ].join("\n");

    let contents = format!("{header}{}", contents);
    std::fs::write(protobuf_dest_path(&generated), contents.as_bytes())?;
    Ok(())
  }

  pub fn build() -> io::Result<()> {
    let descriptor_path =
      PathBuf::from(env::var("OUT_DIR").unwrap()).join("proto_descriptor.bin");

    let protos = &["schema/datapath.proto", "schema/backup.proto"];

    prost_build::Config::new()
      .file_descriptor_set_path(&descriptor_path)
      .compile_well_known_types()
      .compile_protos(protos, &["schema/"])?;

    eprintln!("built");
    for proto in protos {
      copy_compiled_proto(proto)?;
    }

    Ok(())
  }
}

fn main() -> io::Result<()> {
  println!("cargo:rerun-if-changed=./schema/datapath.proto");
  println!("cargo:rerun-if-changed=./schema/backup.proto");

  #[cfg(feature = "build_protos")]
  {
    build_protos::build()
  }
  #[cfg(not(feature = "build_protos"))]
  {
    Ok(())
  }
}
