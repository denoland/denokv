FROM rust:1.73-bookworm as builder

RUN apt-get update && apt-get install -y protobuf-compiler

WORKDIR /usr/src/denokv
COPY . .

RUN cargo build --release

FROM gcr.io/distroless/cc-debian12:debug

COPY --from=builder /usr/src/denokv/target/release/denokv /usr/local/bin/

ENTRYPOINT ["/usr/local/bin/denokv"]