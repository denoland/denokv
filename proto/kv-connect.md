# KV Connect protocol

The KV Connect protocol is the protocol used by the Deno CLI (and other third
party clients) to communicate with remote Deno KV backends. This protocol is not
in use when using the Deno CLI to access a local KV database that is backed by
SQLite. The KV Connect protocol is used when calling
`await Deno.openKv("http://<remote>")`.

To connect to a backend that implements the KV Connect protocol, the client just
needs to know a single URL and access token.

> The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD",
> "SHOULD NOT", "RECOMMENDED", "MAY", and "OPTIONAL" in this document are to be
> interpreted as described in
> [RFC 2119](https://datatracker.ietf.org/doc/html/rfc2119).

## Connection

The KV Connect protocol uses HTTP, JSON, and ProtoBuf. KV Connect compatible
backends MUST support HTTP/1.1 and HTTP/2. It is RECOMMENDED that KV Connect
compatible backends that are accessible over the public internet support HTTPS
only.

The KV Connect protocol is a request-response protocol. The client sends a
request to the server and the server responds with a response. The client and
server MAY send multiple requests and responses over the same underlying
HTTP/1.1 or HTTP/2 connection, as defined in those specifications.

The KV Connect protocol is stateless. The client and server MAY send requests
and responses in any order. The client and server MUST NOT assume that the
requests and responses are received in the same order as they were sent. While
no state is maintained on either client on server between requests, the client
SHOULD maintain a cache of metadata and authentication information received from
the server to avoid unnecessary network round trips.

## Protocol

The KV Connect protocol consists of two parts:

- A JSON based protocol for performing metadata exchange and authentication on
  startup. This is the _Metadata Exchange Protocol_.
- A ProtoBuf based protocol for performing CRUD operations on the KV database.
  This is the _Data Path Protcol_.

### Metadata Exchange

Before being able to perform a CRUD operation on the KV database, the client
MUST perform a metadata exchange with the server. The metadata exchange is used
to inform the client about:

- The version of the KV Connect protocol that the server implements.
- A UUID that uniquely identifies the KV database that the client is connecting
  to.
- The HTTP endpoints that the _Data Path Protocol_ is available on.
- Authentication information for the client to use when performing CRUD
  operations.

The metadata exchange is performed by sending a POST request to the
user-specified URL. This request MUST include a `Authorization` header with the
value `Bearer <token>`, where `<token>` is the access token that the user
specified. The request SHOULD include a `User-Agent` header with a value
identifying the client library and version that is being used. The request MAY
include a JSON body that adheres to the JSON schema defined in the
`schema/kv-metadata-exchange-request.json` file. The `supportedVersions`
property of the request body MUST be an array of numbers that represents the KV
Connect protocol versions that the client supports (e.g. `[1, 2, 3]`). If the
request body is not included, the client is assumed to support only version 1 of
the KV Connect protocol.

Upon receiving the POST request, the server MUST verify that the request body
adheres to the JSON schema defined in the
`schema/kv-metadata-exchange-request.json` file. If the request body is not
included the server MUST assume that the client only supports version 1 of the
KV Connect protocol. The server MUST use this information to choose a protocol
version that both the client and server support. The server MUST verify that the
access token provided in the `Authorization` header is valid for the KV database
that the client is attempting to connect to.

If the access token is invalid, no overlapping protocol versions were found, the
request body is invalid, or another error is encountered that prevent the server
from returning database metadata, the server MUST respond with a 4xx class HTTP
status response and a plain text body containing a human readable error message.
The server MAY return a 3xx class HTTP redirect response. In this case, the
client MUST follow the redirect and perform the metadata exchange with the new
URL as the base URL.

The server MAY return a 5xx class HTTP status response at any time if the server
is unable to process the request right now due to a temporary condition, for
example an internal server error or rate limiting. The server SHOULD include a
plain text body with a human readable error message in this case.

If the access token is valid and a database is found, the server MUST respond
with a 200 OK response. This response MUST include a `Content-Type` header with
the value `application/json`. The text encoding of the response body MUST be
UTF-8. The response body MUST be a JSON object that adheres to the JSON schema
defined in the `schema/kv-metadata-exchange-response.v<protocolversion>.json`
file.

If the client fails to receive a response from the server due to a network
error, or a 5xx class HTTP status, the client SHOULD retry the request using an
exponential backoff strategy with unlimited retries.

The client MUST verify that the response is a 200 OK, 3xx class redirect
response, or 4xx class HTTP status response. If the response is a 3xx class
redirect response, the client MUST follow the redirect and perform the metadata
exchange with the new URL as the base URL (the client MAY impose a maximum
redirect depth). If the response is a 4xx class HTTP status response, the client
MUST fatally abort the metadata exchange and display the error message to the
user. If the response is a 200 OK response, the client MUST verify that the
response body adheres to the JSON schema defined in the
`schema/kv-metadata-exchange-response.v<protocolversion>.json` file. If the
response body is invalid, the client MUST fatally abort the metadata exchange
and display the error message to the user.

If the response body is valid, the client MUST verify that the `version`
property is a protocol version that the client supports. If the `version` is not
a protocol version that the client supports, the client MUST fatally abort the
metadata exchange and display an error message to the user.

If client MUST verify that at least one of the entries in the `endpoints` array
has a `consistency` property that is equal to `strong`. If no such entry is
found, the client MUST fatally abort the metadata exchange and display an error
message to the user.

If the client is using protocol version 2 or higher, the client MUST parse each
URL in the endpoints array and resolve it against the base URL of the metadata
exchange request. If the client followed a redirect during the metadata exchange
the base URL MUST be the URL that the client was last redirected to. If the
client did not follow a redirect during the metadata exchange the base URL MUST
be the URL that the client originally sent the metadata exchange request to. The
client MUST verify that the resolved URL is a valid URL. If the resolved URL is
not a valid URL, the client MUST fatally abort the metadata exchange and display
an error message to the user.

The `token` property of the response body is an opaque string that the client
MUST use as the authorization token when performing CRUD operations using the
_Data Path Protocol_. The `expiresAt` property of the response body is an ISO
8601 formatted string that represents the date and time when the returned
database metadata expires. The client MUST NOT use the returned database
metadata after this date and time. The client SHOULD cache the returned database
metadata and use it until it expires. The client SHOULD perform a metadata
exchange with the server just before the metadata expires to ensure that the
client is using the latest database metadata.

Example metadata exchange:

```http
POST / HTTP/1.1
Authorization: Bearer a1b2c3d4e5f6g7h8i9
Content-Type: application/json
Content-Length: 33

{
  "supportedVersions": [1, 2]
}
```

```http
HTTP/1.1 200 OK
Content-Type: application/json
Content-Length: 315

{
  "version": 2,
  "uuid": "a1b2c3d4-e5f6-7g8h-9i1j-2k3l4m5n6o7p",
  "endpoints": [
    {
      "url": "/v2",
      "consistency": "strong"
    },
    {
      "url": "https://mirror.example.com/v2",
      "consistency": "eventual"
    }
  ],
  "token": "123abc456def789ghi",
  "expiresAt": "2023-10-01T00:00:00Z"
}
```

### Data Path Protocol

After performing the metadata exchange, the client can perform CRUD operations
on the KV database using the _Data Path Protocol_. The _Data Path Protocol_ is a
Protobuf over HTTP protocol.

All requests on this protocol MUST include an `Authorization` header with the
value `Bearer <token>`, where `<token>` is the token returned by the server
during the metadata exchange.

The client MUST include on all requests a `User-Agent` header with a value
identifying the client library and version that is being used.

The client MUST include on all requests a `Content-Type` header with the value
`application/x-protobuf`.

If the protocol version used is 1, the client MUST include on all requests a
`x-transaction-domain-id` header with a value of the UUID of the database as
returned by the server during the metadata exchange.

If the protocol version used is 2 or higher, the client MUST include on all
requests a `x-denokv-database-id` header with a value of the UUID of the
database as returned by the server during the metadata exchange.

If the protocol version used is 2 or higher, the client MUST include on all
requests a `x-denokv-version` header with a value of the protocol version that
the client is using.

The server MUST NOT respond with 3xx class redirect responses to data path
requests.

There are two types of requests that the client may send to the server:

- A _Snapshot Read Request_ which is used to read some values from the database.
- An _Atomic Write Request_ which is used to write some values to the database.

#### Snapshot Read Request

A _Snapshot Read Request_ is used to read some values from the database.

A _Snapshot Read Request_ can either be strongly consistent or eventually
consistent. A strongly consistent _Snapshot Read Request_ MUST be sent to an
endpoint that has a `consistency` property that is equal to `strong`. An
eventually consistent _Snapshot Read Request_ MUST be sent to an endpoint that
has a `consistency` property that is either equal to `strong` or `eventual`.

To determine which endpoint to send a _Snapshot Read Request_ to, the client
MUST first narrow down the list of endpoints to only include endpoints that have
a valid consistency property for the type of _Snapshot Read Request_ that the
client wants to perform. From this list, the client MAY make requests to any
endpoint, but the client SHOULD prefer endpoints that have lower latency.

The client sends a _Snapshot Read Request_ to the server by sending a POST
request to the `<endpointUrl>/snapshot_read` endpoint, where `<endpointUrl>` is
the resolved URL of the selected endpoint. The request body MUST be a Protobuf
message in the format `com.deno.kv.datapath.SnapshotRead` (defined in the
`schema/datapath.proto` file in this repository).

Upon receiving the POST request, the server MUST verify that the request body
adheres to the Protobuf schema for the `com.deno.kv.datapath.SnapshotRead`
message. If the request body is invalid, the server MUST respond with a 4xx
response and a plain text body containing a human readable error message.

The server MAY return a 5xx class HTTP status response at any time if the server
is unable to process the request right now due to a temporary condition, for
example an internal server error or rate limiting. The server SHOULD include a
plain text body with a human readable error message in this case.

The server MAY perform quota checks on the request. If the request is rejected
due to quota limits, the server MUST respond with a 4xx class response and a
plain text body containing a human readable error message.

The server MUST perform the requested snapshot read operation. The server MUST
respond with a 200 OK response. The response MUST include a `Content-Type`
header with the value `application/x-protobuf`. The response body MUST be a
Protobuf message in the format `com.deno.kv.datapath.SnapshotReadOutput`. If the
server is unable to perform the read because the database is not available from
this server, the `read_disabled` field MUST be set to `true`. The server MUST
include the `read_is_strongly_consistent` field, setting it to `true` if the
server has a strongly consistent view of the database, and `false` if the server
has an eventually consistent view. If the request succeeds, the server MUST
include the `ranges` field with a list of ranges that were read from the
database. The order MUST be the same as the order of the ranges in the request.

If the client fails to receive a response from the server due to a network
error, or a 5xx class HTTP status, the client SHOULD retry the request using an
exponential backoff strategy with unlimited retries.

The client MUST verify that the response is a 200 OK response. If the response
is a 4xx class error response, the client MUST fatally abort the request and
display the error message to the user. If the response is a 200 OK response, the
client MUST verify that the response body adheres to the Protobuf schema for the
`com.deno.kv.datapath.SnapshotReadOutput` message. If the response body is
invalid, the client MUST fatally abort the request and display the error message
to the user.

If the protocol version is 3 or higher, the client MUST read the `status` field
of the response. If the response has a `status` field set to `SR_UNSPECIFIED`,
the client MUST fatally abort the request and display an error message to the
user.

If the protocol version is 3 or higher and the `status` field is set to
`SR_READ_DISABLED`, or if the protocol version is 2 or lower and the
`read_disabled` field is set to `true`, or if the request was strongly
consistent but the `read_is_strongly_consistent` field is set to `false`, the
client SHOULD perform a metadata exchange with the server to get a new list of
endpoints, and then retry the request.

If the ranges in the response match the ranges in the request, the client SHOULD
return the values to the user.

Example snapshot read request:

```http
POST /v2/snapshot_read HTTP/1.1
Authorization: Bearer 123abc456def789ghi
Content-Type: application/x-protobuf
x-denokv-version: 3
x-denokv-database-id: a1b2c3d4-e5f6-7g8h-9i1j-2k3l4m5n6o7p
User-Agent: Deno/1.38.0
Content-Length: 45

<protobuf message>
```

```http
HTTP/1.1 200 OK
Content-Type: application/x-protobuf
Content-Length: 45

<protobuf message>
```

#### Atomic Write Request

An _Atomic Write Request_ is used to write some values to the database.

An _Atomic Write Request_ MUST be sent to an endpoint that has a `consistency`
property that is equal to `strong`.

To determine which endpoint to send an _Atomic Write Request_ to, the client
MUST first narrow down the list of endpoints to only include endpoints that have
a `consistency` property that is equal to `strong`. From this list, the client
MAY make requests to any endpoint, but the client SHOULD prefer endpoints that
have lower latency.

The client sends an _Atomic Write Request_ to the server by sending a POST
request to the `<endpointUrl>/atomic_write` endpoint, where `<endpointUrl>` is
the resolved URL of the selected endpoint. The request body MUST be a Protobuf
message in the format `com.deno.kv.datapath.AtomicWrite` (defined in the
`schema/datapath.proto` file in this repository).

Upon receiving the POST request, the server MUST verify that the request body
adheres to the Protobuf schema for the `com.deno.kv.datapath.AtomicWrite`
message. If the request body is invalid, the server MUST respond with a 4xx
class HTTP status response and a plain text body containing a human readable
error message.

The server MAY perform quota checks on the request. If the request is rejected
due to quota limits, the server MUST respond with a 4xx class HTTP status
response and a plain text body containing a human readable error message.

The server MUST perform the requested atomic write operation. The server MUST
respond with a 200 OK response. The response MUST include a `Content-Type`
header with the value `application/x-protobuf`. The response body MUST be a
Protobuf message in the format `com.deno.kv.datapath.AtomicWriteResponse`. If
the server is unable to perform the write because the database is not available
from this server, the `status` field MUST be set to `AW_WRITE_DISABLED`. If a
write operation fails due to a check conflict, the `status` field MUST be set to
`AW_CHECK_FAILED`. If the write operation succeeds, the `status` field MUST be
set to `AW_SUCCESS`. If the request succeeds, the server MUST include the
`versionstamp` field with the versionstamp of the write operation.

If the client fails to receive a response from the server due to a network
error, or a 5xx class HTTP status, the client SHOULD retry the request using an
exponential backoff strategy with unlimited retries.

The client MUST verify that the response is a 200 OK response. If the response
is a 4xx class error response, the client MUST fatally abort the request and
display the error message to the user. If the response is a 200 OK response, the
client MUST verify that the response body adheres to the Protobuf schema for the
`com.deno.kv.datapath.AtomicWriteResponse` message. If the response body is
invalid, the client MUST fatally abort the request and display the error message
to the user.

If the response has a `status` field set to `AW_WRITE_DISABLED`, the client
SHOULD perform a metadata exchange with the server to get a new list of
endpoints, and then retry the request.

If the response has a `status` field set to `AW_CHECK_FAILED`, the client MUST
return an error to the user indicating that the write operation failed due to a
check conflict. The client SHOULD report the checks that failed to the user by
interpereting the `check_failures` field of the response.

If the response has a `status` field set to `AW_UNDEFINED`, the client MUST
return an error to the user indicating that the write operation failed due to an
unknown error.

If the response has a `status` field set to `AW_SUCCESS`, the client SHOULD
return the versionstamp of the write operation to the user.

Example atomic write request:

```http
POST /v2/atomic_write HTTP/1.1
Authorization: Bearer 123abc456def789ghi
Content-Type: application/x-protobuf
x-denokv-version: 2
x-denokv-database-id: a1b2c3d4-e5f6-7g8h-9i1j-2k3l4m5n6o7p
User-Agent: Deno/1.38.0
Content-Length: 45

<protobuf message>
```

```http
HTTP/1.1 200 OK
Content-Type: application/x-protobuf
Content-Length: 12

<protobuf message>
```

#### Watch Request

A _Watch Request_ is used to watch for changes to keys in the database.

A _Watch Request_ is always eventually consistent, so it MUST be sent to an
endpoint that has a `consistency` property that is either equal to `strong` or
`eventual`.

To determine which endpoint to send a _Watch Request_ to, the client MUST first
narrow down the list of endpoints to only include endpoints that have a valid
consistency property for the type of _Watch Request_ that the client wants to
perform. From this list, the client MAY make requests to any endpoint, but the
client SHOULD prefer endpoints that have lower latency.

The client sends a _Watch Request_ to the server by sending a POST request to
the `<endpointUrl>/watch` endpoint, where `<endpointUrl>` is the resolved URL of
the selected endpoint. The request body MUST be a Protobuf message in the format
`com.deno.kv.datapath.Watch` (defined in the `schema/datapath.proto` file in
this repository).

Upon receiving the POST request, the server MUST verify that the request body
adheres to the Protobuf schema for the `com.deno.kv.datapath.Watch` message. If
the request body is invalid, the server MUST respond with a 4xx class HTTP
status response and a plain text body containing a human readable error message.

The server MAY return a 5xx class HTTP status response at any time if the server
is unable to process the request right now due to a temporary condition, for
example an internal server error or rate limiting. The server SHOULD include a
plain text body with a human readable error message in this case.

The server MAY perform quota checks on the request. If the request is rejected
due to quota limits, the server MUST respond with a 4xx class HTTP status
response and a plain text body containing a human readable error message.

The server MUST perform a snapshot read operation for the requested keys. The
server MUST respond with a 200 OK response. The response MUST include a
`Content-Type` header with the value `application/octet-stream`. The response
body MUST be a stream. The first message of this stream MUST be a Protobuf
encoded message in the format `com.deno.kv.datapath.WatchOutput`, prefixed with
a 4 byte little endian integer that represents the length of the message.

When any of the watched keys change, the server MUST send a Protobuf encoded
message in the format `com.deno.kv.datapath.WatchOutput`, prefixed with a 4 byte
little endian integer that represents the length of the message, to the client
over the same stream. The server MAY collapse multiple changes into a single
notification, as long as each message represents a consistent snapshot of the
keys that the client is watching.

The server MAY send a zero length message to the client over the stream at any
time to indicate that the server is still alive. These messages must be
structured as a 4 byte little endian integer encoding of 0, followed by no data.

If the client fails to receive a response from the server due to a network
error, or a 5xx class HTTP status, the client SHOULD retry the request using an
exponential backoff strategy with unlimited retries.

The client MUST verify that the response is a 200 OK response. If the response
is a 4xx class error response, the client MUST fatally abort the request and
display the error message to the user.

If the response is a 200 OK response, the client MUST parse each message in the
stream by first reading a 4 byte little endian integer that represents the
length of the message, and then reading that many bytes from the stream. If the
length of the message is 0, the client MUST ignore the message and wait for the
next message. If the length of the message is greater than 0, the client MUST
then verify that the message adheres to the Protobuf schema for the
`com.deno.kv.datapath.WatchOutput` message. If the message is invalid, the
client MUST fatally abort the request and display the error message to the user.

If the message is valid, the client MUST read the `status` field of the message
and verify that it is set to `SR_SUCCESS`. If the `status` field is set to
`SR_READ_DISABLED`, the client SHOULD perform a metadata exchange with the
server to get a new list of endpoints, and then retry the request. If the
`status` field is set to any other value, the client MUST fatally abort the
request and display an error message to the user.

If the message is valid, the client MUST yield the message to the user, and then
continue reading messages from the stream.

Example watch request:

```http
POST /v3/watch HTTP/1.1
x-denokv-version: 3
x-denokv-database-id: a1b2c3d4-e5f6-7g8h-9i1j-2k3l4m5n6o7p

<protobuf message>
```

```http
HTTP/1.1 200 OK
Content-Type: application/octet-stream

<u32 little endian><protobuf message><u32 little endian><protobuf message>...
```

## Protocol Versions

The KV Connect protocol is versioned. The version of the protocol is negotiated
during the metadata exchange. Below is a list of protocol versions and how they
differ from each other.

This specification defines server and client behavior for version 2, but is
entirely backwards compatible with version 1. This means that a client that
supports version 2 as specified here can connect to a server that only supports
version 1. A client that only supports version 1 can not connect to a server
that only supports version 2.

### Version 1

Version 1 is the initial version of the KV Connect protocol. It is the default
version that is used if the client does not specify a list of supported versions
during the metadata exchange.

### Version 2

Version 2 adds the following features:

- The `x-denokv-version` header is added to all requests on the _Data Path
  Protocol_. This header is used to indicate the protocol version that the
  client is using. This allows the server to support multiple protocol versions
  at the same time.

- The `x-denokv-database-id` header is added to all requests on the _Data Path
  Protocol_. This header is used to indicate the UUID of the database that the
  client is accessing. This allows the server to support multiple databases at
  the same time.

- The `read_is_strongly_consistent` field is added to the response body of
  _Snapshot Read Requests_. This field is used to indicate whether the server
  has a strongly consistent view of the database or not. This allows the client
  to retry the request if the server does not have a strongly consistent view of
  the database.

### Version 3

Version 3 adds the following features:

- The `status` field is added to the response body of _Snapshot Read Requests_
  and is used instead of the `read_disabled` boolean field by the client.
- The "Watch" data path operation is added.
