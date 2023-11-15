# denokv

A self-hosted backend for [Deno KV](https://deno.com/kv), the JavaScript first key-value database:

- Seamlessly integrated JavaScript APIs
- ACID transactions
- Multiple consistency levels for optimal performance for every usecase

![Diagram showing how a `denokv` setup looks](./.github/diagram-dark.png#gh-dark-mode-only)
![Diagram showing how a `denokv` setup looks](./.github/diagram-light.png#gh-light-mode-only)

Deno KV can be used with the built-in single instance database in the CLI,
useful for testing and development, with a hosted and [scalable backend](https://deno.com/blog/building-deno-kv) on
[Deno Deploy](https://deno.com/deploy), or with this self-hostable Deno KV
backend.

To run `denokv`, just run:

```sh
docker run -it --init -p 4512:4512 -v ./data:/data ghcr.io/denoland/denokv --sqlite-path /data/denokv.sqlite serve --access-token <random-token>
```

Then run your Deno program and specify the access token in the
`DENO_KV_ACCESS_TOKEN` environment variable:

```ts
const kv = await Deno.openKv("http://localhost:4512");
```

The self-hosted `denokv` backend is built on the same robust SQLite backend as
the built-in single instance database in the CLI. It is designed to be run on a
VPS or Kubernetes cluster statefully, with Deno processes connecting via the
network using [KV Connect](https://docs.deno.com/kv/manual/on_deploy#connect-to-managed-databases-from-outside-of-deno-deploy).

The standalone `denokv` binary is designed to handle thousands of concurrent
requests, from hundreds of different Deno processes. It is built on top of the
robust SQLite database, and uses non-blocking IO to ensure excellent performance
even in the face of hundreds of concurrent connections.

Just like the Deno CLI, `denokv` is MIT licensed, free and open source.

Read more in [the announcement of self-hosted Deno KV](https://deno.com/blog/kv-is-open-source-with-continuous-backup).

## When should I use this?

If you need more than a single Deno process to access the same KV database, and
you are ok with running a server, keeping `denokv` updated, handling backups,
and performing regular maintenance, then this is for you.

You can use a hosted KV database on Deno Deploy if you don't want to self-host
and manage a `denokv` server.

If you are just need a backend for local development or testing, you can use the
Deno KV backend built into the Deno CLI. You can open a temporary in memory KV
database with `Deno.openKv(":memory:")` or a persistent database by specifying a
path like `Deno.openKv("./my-database.sqlite")`.

## How to run

### Docker on a VPS

> Ensure that you are running on a service that supports persistent storage, and
> does not perform auto-scaling beyond a single instance. This means you can not
> run `denokv` on Google Cloud Run or AWS Lambda.

Install Docker on your VPS and create a directory for the database to store data
in.

```sh
$ mkdir -p /data
```

Then run the `denokv` Docker image, mounting the `/data` directory as a volume
and specifying a random access token.

```sh
docker run -it --init -p 4512:4512 -v ./data:/data ghcr.io/denoland/denokv --sqlite-path /data/denokv.sqlite serve --access-token <random-token>
```

You can now access the database from your Deno programs by specifying the access
token in the `DENO_KV_ACCESS_TOKEN` environment variable, and the host and port
of your VPS in the URL passed to `Deno.openKv`.

You should additionally add a HTTPS terminating proxy or loadbalancer in front
of `denokv` to ensure that all communication happens over TLS. Not using TLS can
pose a significant security risk. The HTTP protocol used by Deno KV is
compatible with any HTTP proxy, such as `caddy`, `nginx`, or a loadbalancer.

### Fly.io

You can easily host `denokv` on https://fly.io.

> Note: Fly.io is a paid service. You will need to add a credit card to your
> account to use it.

Sign up to Fly.io and
[install the `flyctl` CLI](https://fly.io/docs/hands-on/install-flyctl/).

Sign into the CLI with `flyctl auth login`.

Create a new app with `flyctl apps create`.

Create a `fly.toml` file with the following contents. Make sure to replace the
`<your-app-name>` and `<region>` placeholders with your app name and the region
you want to deploy to.

```toml
app = "<your-app-name>"
primary_region = "<region>"

[build]
  image = "ghcr.io/denoland/denokv:latest"

[http_service]
  internal_port = 4512
  force_https = true
  auto_stop_machines = true
  auto_start_machines = true
  min_machines_running = 0

[env]
  DENO_KV_SQLITE_PATH="/data/denokv.sqlite3"
  # access token is set via `flyctl secrets set`

[mounts]
  destination = "/data"
  source = "denokv_data"
```

Run `flyctl volumes create denokv_data` to create a volume to store the database
in.

Run `flyctl secrets set DENO_KV_ACCESS_TOKEN=<random-token>` to set the access
token. Make sure to replace `<random-token>` with a random string. Keep this
token secret, and don't share it with anyone. You will need this token to
connect to your database from Deno.

Run `flyctl deploy` to deploy your app.

You can now access the database from your Deno programs by specifying the access
token in the `DENO_KV_ACCESS_TOKEN` environment variable, and the URL provided
by `flyctl deploy` in the URL passed to `Deno.openKv`.

Be aware that with this configuration, your database can scale to 0 instances
when not in use. This means that the first request to your database after a
period of inactivity will be slow, as the database needs to be started. You can
avoid this by setting `min_machines_running` to `1`, and setting
`auto_stop_machines = false`.

### Install binary

You can download a prebuilt binary from the
[releases page](https://github.com/denoland/denokv/releases/tag/0.1.0) and place
it in your `PATH`.

You can also compile from source by running `cargo install denokv --locked`.

## How to connect

### Deno

To connect to a `denokv` server from Deno, use the `Deno.openKv` API:

```ts
const kv = await Deno.openKv("http://localhost:4512");
```

Make sure to specify your access token in the `DENO_KV_ACCESS_TOKEN` environment
variable.

<!-- TBD: ### Node.js -->

## Advanced setup

### Running as a replica of a hosted KV database

`denokv` has a mode for running as a replica of a KV database hosted on Deno
Deploy through the S3 backup feature.

To run as a replica:

```sh
docker run -it --init -p 4512:4512 -v ./data:/data \
  -e AWS_ACCESS_KEY_ID="<aws-access-key-id>" \
  -e AWS_SECRET_ACCESS_KEY="<aws-secret-access-key>" \
  -e AWS_REGION="<aws-region>" \
  ghcr.io/denoland/denokv --sqlite-path /data/denokv.sqlite serve \
  --access-token <random-token> --sync-from-s3 --s3-bucket your-bucket --s3-prefix some-prefix/6aea9765-2b1e-41c7-8904-0bdcd70b21d3/
```

To sync the local database from S3, without updating the snapshot:

```sh
denokv --sqlite-path /data/denokv.sqlite pitr sync --s3-bucket your-bucket --s3-prefix some-prefix/6aea9765-2b1e-41c7-8904-0bdcd70b21d3/
```

To list recoverable points:

```sh
denokv --sqlite-path /data/denokv.sqlite pitr list
```

To checkout the snapshot at a specific recoverable point:

```sh
denokv --sqlite-path /data/denokv.sqlite pitr checkout 0100000002c0f4c10000
```

### Continuous backup using LiteFS

TODO

## Other things in this repo

This repository contains two crates:

- `denokv_proto` (`/proto`): Shared interfaces backing KV, like definitions of
  `Key`, `Database`, and `Value`.
- `denokv_sqlite` (`/sqlite`): An implementation of `Database` backed by SQLite.
- `denokv_remote` (`/remote`): An implementation of `Database` backed by a
  remote KV database, acessible via the KV Connect protocol.

These crates are used by the `deno_kv` crate in the Deno repository to provide a
JavaScript API for interacting with Deno KV.

The Deno KV Connect protocol used for communication between Deno and a remote KV
database is defined in [`/proto/kv-connect.md`](./proto/kv-connect.md).
