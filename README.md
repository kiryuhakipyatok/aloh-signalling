# aloh Signalling Server


**aloh Signalling Server** is a high-performance, lightweight QUIC-based signalling server written in Go. It facilitates real-time peer-to-peer communication by managing user connections, sessions, and message routing using HTTP/3 (QUIC) transport with both stream-based and datagram-based message passing.

---

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
  - [Layered Architecture](#layered-architecture)
- [Protocol](#protocol)
  - [Message Envelope](#message-envelope)
  - [Message Types](#message-types)
  - [Response Format](#response-format)
  - [Error Codes](#error-codes)
  - [Error Handling Strategy](#error-handling-strategy)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Configuration](#configuration)
  - [Configuration File](#configuration-file)
  - [Environment Variables](#environment-variables)
  - [App Environments](#app-environments)
- [Building](#building)
  - [From Source (Makefile)](#from-source-makefile)
  - [From Source (Manual)](#from-source-manual)
- [Running](#running)
  - [Prerequisites: TLS Certificates](#prerequisites-tls-certificates)
  - [Local Development](#local-development)
  - [Using Docker (Development)](#using-docker-development)
- [Docker](#docker)
  - [Production](#production)
  - [Development](#development-1)
  - [Multi-Stage Dockerfile](#multi-stage-dockerfile)
- [Development](#development)
  - [Available Makefile Targets](#available-makefile-targets)
  - [Testing](#testing)
- [Dependencies](#dependencies)
- [CI/CD](#cicd)
- [Logging](#logging)
- [Error Handling](#error-handling)
- [How It Works](#how-it-works)
  - [1. Server Startup](#1-server-startup)
  - [2. Connection Handling](#2-connection-handling)
  - [3. Command Processing](#3-command-processing)
  - [4. Credentials Generation](#4-credentials-generation)
  - [5. Session Model](#5-session-model)
- [License](#license)

---

## Features

- **⚡ QUIC/HTTP3 Transport** — Built on `quic-go`, leveraging multiplexed streams and UDP datagrams.
- **💾 In-Memory Data Stores** — Thread-safe `sync.Map`-backed repositories for connections and sessions with no external database dependency.
- **📡 Multi-Stream Signaling** — Supports both unidirectional stream messaging (reliable, ordered) and datagram proxying (unreliable, low-latency).
- **🔑 HMAC Credentials** — Time-limited, HMAC-SHA1 signed credentials for secure TURN/session authentication.
- **👥 Session Management** — Multi-user sessions with add/remove participant operations.
- **🟢 Online Presence** — Fetch all online users or query real-time online status of specific friend lists.
- **📝 Structured Logging** — Environment-aware logging (text/JSON, debug/info levels) via Go's native `log/slog`.
- **🛡️ Strict Validation** — All incoming payload formats validated using `go-playground/validator`.
- **🛑 Graceful Shutdown** — Handles `SIGINT` / `SIGTERM` signals with thorough connection and session cleanup.
- **🐳 Docker Ready** — Optimized multi-stage Dockerfile and docker-compose configurations for development and production.

---

## Architecture

```text
┌─────────────────────────────────────────────────────────────┐
│                          Clients                            │
│     (User A)                 (User B)             (User C)  │
│          │                       │                    │     │
│        QUIC UDP :1234            │                    │     │
│          │                       │                    │     │
└──────────┼───────────────────────┼────────────────────┘     │
           │                       │                          │
           ▼                       │                          │
┌──────────────────────────────────▼──────────────────────────┐
│                   QUIC Signalling Server                    │
│                                                             │
│  ┌──────────┐   ┌─────────────────────────────────────────┐ │
│  │ Listener │   │           AcceptConnections()           │ │
│  │ (UDP+TLS)│   │  ┌─────────────────────────────────────┐│ │
│  └────┬─────┘   │  │        ServeConnection()            ││ │
│       │         │  │  ┌────────────────────────┐         ││ │
│       │         │  │  │   Registration (REG)   │         ││ │
│       │         │  │  │   → genCreds()         │         ││ │
│       │         │  │  │   → commandLoop()      │         ││ │
│       │         │  │  └───────────┬────────────┘         ││ │
│       │         │  │              │                      ││ │
│       │         │  │    ┌─────────┴─────────┐            ││ │
│       │         │  │    │  Message Switch   │            ││ │
│       │         │  │    ├───────────────────┤            ││ │
│       │         │  │    │ STREAM  → sendMsg │            ││ │
│       │         │  │    │ DATAGRAM→ proxy   │            ││ │
│       │         │  │    │ DISCONN → close   │            ││ │
│       │         │  │    │ ONLINE  → fetch   │            ││ │
│       │         │  │    │ SESSION → repo    │            ││ │
│       │         │  │    └─────────┬─────────┘            ││ │
│       │         │  └──────────────┼──────────────────────┘│ │
│       │         │                 │                       │ │
│       │         │  ┌──────────────┴──────────────┐        │ │
│       │         │  │  Repositories (in-memory)   │        │ │
│       │         │  │  ┌──────────────────────┐   │        │ │
│       │         │  │  │ ConnectionsRepo      │   │        │ │
│       │         │  │  │  (sync.Map)          │   │        │ │
│       │         │  │  └──────────────────────┘   │        │ │
│       │         │  │  ┌──────────────────────┐   │        │ │
│       │         │  │  │ SessionsRepo         │   │        │ │
│       │         │  │  │  (sync.Map)          │   │        │ │
│       │         │  │  └──────────────────────┘   │        │ │
│       │         │  └─────────────────────────────┘        │ │
└───────┴─────────┴─────────────────────────────────────────┴─┘
```

### Layered Architecture

The project follows a clean, decoupled layout:

```text
cmd/app/main.go              ← Application entry point
internal/app/app.go          ← Bootstrap (dependency injection & lifecycle)
internal/config/             ← Configuration loading (Viper + environment variables)
internal/server/             ← QUIC transport layer (listener & accept loop)
internal/protocols/          ← Protocol definitions (messages, constants, errors)
internal/domain/
  ├── models/                ← Domain entities (Connection, Session)
  ├── repository/            ← In-memory stores (ConnectionsRepo, SessionsRepo)
  └── services/              ← Business logic (SignallingService)
pkg/
  ├── errs/                  ← Error types and constructors
  ├── logger/                ← Structured logging wrapper (slog)
  └── validator/             ← Struct validation wrapper
```

---

## Protocol

The server uses a **JSON-over-QUIC** protocol. Messages are exchanged over a control stream (the first bidirectional stream opened upon handshake). Message framing is handled via Go's streaming `json.Decoder`.

### Message Envelope

All client-to-server messages follow a unified envelope format:

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "type": 0,
  "data": { }
}
```

| Field | Type | Description |
| :--- | :--- | :--- |
| `id` | `uuid.UUID` | Unique message identifier for request/response correlation |
| `type` | `uint8` | Message type opcode (see table below) |
| `data` | `json.RawMessage` | Type-specific payload body |

### Message Types

| Constant | Value | Description |
| :--- | :---: | :--- |
| `REG_TYPE` | `0` | **Registration** — First message; client provides user ID and receives HMAC credentials |
| `STREAM_TYPE` | `1` | **Stream Message** — Forwards a payload to target receiver IDs via unidirectional streams |
| `DATAGRAM_TYPE` | `2` | **Datagram Proxy** — Sets up bidirectional UDP-style datagram proxying between peers |
| `DISCONN_TYPE` | `3` | **Disconnect** — Gracefully closes the connection |
| `GET_ONLINE_TYPE` | `4` | **Fetch Online** — Returns all currently connected user IDs |
| `ADD_IN_SESSION` | `5` | **Add to Session** — Adds a user to the sender's session group |
| `GET_SESSIONS_BY_ID`| `6` | **Fetch Session Members** — Returns all user IDs connected in a specific user's session |
| `DELETE_FROM_SESSION`| `7` | **Remove from Session** — Removes a participant from the sender's session group |
| `GET_ONLINE_FRIENDS` | `8` | **Fetch Friends Online** — Filters an input list of friend IDs by current online status |

### Response Format

All server responses follow a structured envelope:

```json
{
  "msgId": "550e8400-e29b-41d4-a716-446655440000",
  "code": 0,
  "payload": { }
}
```

| Field | Type | Description |
| :--- | :--- | :--- |
| `msgId` | `uuid.UUID` | ID corresponding to the client request |
| `code` | `uint` | Status/error code (see Error Codes) |
| `payload` | `json.RawMessage` | Response body or `null` |

### Error Codes

| Code | Constant | Description |
| :---: | :--- | :--- |
| `0` | `SUCCESS` | Operation completed successfully (no payload) |
| `1` | `NOT_FOUND` | Resource (user/session/connection) not found |
| `2` | `ALREADY_EXISTS` | Resource already exists |
| `3` | `REQUEST_TIMEOUT` | Request timed out or context was cancelled |
| `4` | `PAYLOAD_SUCCESS` | Operation completed successfully with payload |
| `5` | `INVALID_PROTOCOL` | Malformed JSON payload or struct validation failure |
| `6` | `STREAM_ERROR` | Stream read/write failure |
| `7` | `INVALID_TYPE` | Unknown message or invalid data type |
| `8` | `INTERNAL_ERROR` | Unhandled internal server error |

### Error Handling Strategy

The server maps internal domain errors to protocol response codes using a centralized `processError` function:

| Internal Go Error | Protocol Response Code |
| :--- | :--- |
| `ErrAlreadyExistsBase` | `ALREADY_EXISTS` (`2`) |
| `ErrNotFoundBase` | `NOT_FOUND` (`1`) |
| `ErrRequestTimeoutBase` | `REQUEST_TIMEOUT` (`3`) |
| `ErrValidationBase` | `INVALID_PROTOCOL` (`5`) |
| `ErrDecodeMsgBase` | `INVALID_PROTOCOL` (`5`) |
| `ErrInvalidJsonBase` | `INVALID_PROTOCOL` (`5`) |
| `ErrWrongMessageTypeBase`| `INVALID_TYPE` (`7`) |
| *(Any other error)* | `INTERNAL_ERROR` (`8`) |

---

## Project Structure

```text
aloh-signalling/
├── cmd/
│   └── app/
│       └── main.go             # Application entry point
├── configs/
│   └── app/
│       └── config.yaml         # Default configuration file
├── internal/
│   ├── app/
│   │   └── app.go              # Application bootstrap & lifecycle management
│   ├── config/
│   │   └── config.go           # Configuration struct & Viper loader
│   ├── domain/
│   │   ├── models/
│   │   │   ├── connection.go   # Connection entity (user ID + QUIC connection)
│   │   │   ├── session.go      # Session entity (user ID + connected users)
│   │   │   └── user-data.go    # User data model
│   │   ├── repository/
│   │   │   ├── connections-repo.go # Thread-safe connection store (sync.Map)
│   │   │   └── session-repo.go     # Thread-safe session store (sync.Map)
│   │   └── services/
│   │       ├── signalling-serv.go  # Core signalling service & command loop
│   │       ├── helpers.go          # Message routing, streaming, datagrams
│   │       └── utils.go            # Error checks, message utilities
│   ├── protocols/
│   │   ├── protocol.go         # Message structs & serializers
│   │   ├── types.go            # Message type constants (iota)
│   │   └── errs.go             # Response builder & error constructors
│   └── server/
│       └── server.go           # QUIC listener & connection accept loop
├── pkg/
│   ├── errs/
│   │   └── errs.go             # Shared error types & AppError wrapper
│   ├── logger/
│   │   └── logger.go           # Structured logger (slog wrapper)
│   └── validator/
│       └── validator.go        # Validator wrapper for struct validation
├── .env                        # Environment variable template
├── .gitignore
├── .gitlab-ci.yml              # GitLab CI pipeline
├── Dockerfile                  # Multi-stage production Docker build
├── docker-compose.yaml         # Production compose configuration
├── docker-compose.dev.yaml     # Development compose configuration
├── go.mod
├── go.sum
└── Makefile                    # Build, test, run, and container targets
```

---

## Prerequisites

- **Go 1.26+** (specified in `go.mod`)
- **TLS 1.3 certificates** (required for QUIC handshake)
- **Docker & Docker Compose** (for containerized deployments)

---

## Configuration

Configuration is loaded from `configs/app/config.yaml` with automatic environment variable expansion via Viper.

### Configuration File

```yaml
app:
  name: "aloh"
  version: "1.0.0"
  env: "dev"

server:
  host: "localhost"
  port: "${SERVER_PORT}"
  handshakeTimeout: 10s
  idleTimeout: 60s
  keepAlivePeriodTimeout: 10s
  closeTimeout: 10s
  startTimeout: 10s
  certsPath: "../etc/certs"
  nextProtos: ["aloh-proto"]
  maxIncomingStreams: 100

signaling:
  secret: "${SECRET}"
  credsTTL: 24h
```

### Environment Variables

Configured in your `.env` file:

| Variable | Default | Description |
| :--- | :--- | :--- |
| `CONFIG_PATH` | `../configs/app/` | Path to directory containing `config.yaml` |
| `SERVER_PORT` | `1234` | UDP/TCP port for the QUIC listener |
| `SECRET` | `ticindicin0978` | HMAC secret key used for session credential generation |

### App Environments

The `app.env` setting governs logging format and levels:

| Value | Logger Format | Log Level |
| :--- | :--- | :--- |
| `local` | Text (stdout) | `Debug` |
| `dev` | JSON (stdout) | `Info` |
| `prod` | Text (stdout) | `Info` |

---

## Building

### From Source (Makefile)

```bash
make build
```

### From Source (Manual)

```bash
go mod download
go build -o app cmd/app/main.go
```

---

## Running

### Prerequisites: TLS Certificates

QUIC requires TLS 1.3. Generate a local self-signed certificate/key pair for development:

```bash
openssl req -x509 -newkey rsa:4096 -keyout server.key -out server.crt \
  -days 365 -nodes -subj "/CN=localhost"
```

### Local Development

```bash
# 1. Setup environment file
cp .env.example .env

# 2. Place TLS certificates in configs/certs/ (or path specified in config)

# 3. Run application
make run

# Alternatively run with custom environment overrides
CONFIG_PATH="../configs/app/" SERVER_PORT=1234 SECRET="your-secret" ./app
```

### Using Docker (Development)

Builds the binary and mounts certificates from your local directory:

```bash
make docker-run-app
```

---

## Docker

### Production

Run with pre-built release image:

```bash
# Build production image
make docker-build

# Launch production compose stack
docker-compose up -d
```

### Development

Runs with local volume mounting and dynamic rebuild:

```bash
docker-compose -f docker-compose.dev.yaml up --build
```

### Multi-Stage Dockerfile

```dockerfile
FROM golang:1.26-alpine AS builder
WORKDIR /usr/local/src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o ./bin/app cmd/app/main.go

FROM alpine AS runner
WORKDIR /app
COPY --from=builder /usr/local/src/bin/app .
COPY configs /configs
EXPOSE 1234
CMD ["./app"]
```

---

## Development

### Available Makefile Targets

| Target | Description |
| :--- | :--- |
| `make all` | Build application and run test suite |
| `make build` | Compile binary to `main.exe` or `app` |
| `make run` | Run application locally via `go run` |
| `make test` | Run all tests with verbose output |
| `make clean` | Remove compiled binaries |
| `make docker-run-infra` | Start supporting infrastructure containers |
| `make docker-run-app` | Build and run development container |
| `make docker-run-all` | Run tests, migrations, and launch Docker service |
| `make docker-down` | Tear down running Docker containers |
| `make docker-build` | Rebuild Docker image without cache |

### Testing

```bash
go test ./... -v
```

---

## Dependencies

| Package | Purpose |
| :--- | :--- |
| `github.com/quic-go/quic-go` | Core QUIC and HTTP/3 transport layer |
| `github.com/spf13/viper` | Configuration loading with environment variable interpolation |
| `github.com/go-playground/validator/v10` | Struct and message validation |
| `github.com/google/uuid` | UUID generation and parsing |
| `golang.org/x/sync` | Concurrency synchronization and `errgroup` utilities |

---

## CI/CD

The project includes a GitLab CI pipeline (`.gitlab-ci.yml`) structured into three stages:

| Stage | Job | Description |
| :--- | :--- | :--- |
| `build` | `build-job` | Compiles the Docker image (`aloh-image`) |
| `test` | `test-job` | Runs automated test suite |
| `deploy` | `deploy-job` | Injects certificates/secrets, restarts containers, and verifies deployment |

---

## Logging

Powered by Go's standard `log/slog` via an internal package wrapper (`pkg/logger/logger.go`).

### Example Log Entry

```text
level=INFO app=aloh version=1.0.0 env=dev op=signalService.ServeConnection msg="user is registered" userID=550e8400-e29b-41d4-a716-446655440000 msgId=...
```

### Logger API

```go
logger.Info(msg, attrs...)
logger.Error(msg, attrs...)
logger.Debug(msg, attrs...)

// Operation context chaining
logger.NewOp("operation.name")
logger.Err(err)
logger.Attr("key", value)
```

---

## Error Handling

### Error Types

Defined in `pkg/errs/errs.go`:
- `AppError` — Carries operation context (`Op`) alongside wrapped errors (`Err`).
- **Sentinel Errors:** `ErrNotFoundBase`, `ErrAlreadyExistsBase`, `ErrRequestTimeoutBase`, `ErrValidationBase`, `ErrDecodeMsgBase`, `ErrInvalidJsonBase`, `ErrWrongMessageTypeBase`, `ErrInvalidTypeBase`, `ErrWriteMsgBase`.

### Error Propagation

```go
func processError(ctx context.Context, uc *Connection, err error, msgId uuid.UUID) error {
    switch {
    case errors.Is(err, errs.ErrAlreadyExistsBase):
        return uc.SendError(msgId, ALREADY_EXISTS)
    case errors.Is(err, errs.ErrNotFoundBase):
        return uc.SendError(msgId, NOT_FOUND)
    case errors.Is(err, errs.ErrRequestTimeoutBase):
        return uc.SendError(msgId, REQUEST_TIMEOUT)
    case errors.Is(err, errs.ErrValidationBase),
         errors.Is(err, errs.ErrDecodeMsgBase),
         errors.Is(err, errs.ErrInvalidJsonBase):
        return uc.SendError(msgId, INVALID_PROTOCOL)
    case errors.Is(err, errs.ErrWrongMessageType):
        return uc.SendError(msgId, INVALID_TYPE)
    default:
        return uc.SendError(msgId, INTERNAL_ERROR)
    }
}
```

---

## How It Works

### 1. Server Startup
- Loads configuration with Viper and applies environment variable expansions.
- Configures structured logger for target environment.
- Initializes thread-safe in-memory `ConnectionsRepo` and `SessionsRepo`.
- Starts QUIC UDP listener configured with TLS 1.3, stream limits, and datagram capabilities.

### 2. Connection Handling
- Accepts incoming QUIC connections in dedicated goroutines.
- Opens the primary bidirectional control stream.
- Awaits initial registration message (`REG_TYPE`).
- Verifies identity, registers connection, and evicts older connections for the same user ID (enforces single-session-per-user).
- Issues signed HMAC credentials and enters `commandLoop`.

### 3. Command Processing

| Message Type | Handler | Effect |
| :--- | :--- | :--- |
| `STREAM_TYPE` | `sendMsg()` | Opens unidirectional streams to recipient peers and writes payload |
| `DATAGRAM_TYPE` | `datagramProxing()` | Proxies low-latency UDP datagrams directly between peers |
| `DISCONN_TYPE` | `closeConnection()` | Tears down connection and removes peer from active sessions |
| `GET_ONLINE_TYPE` | `fetchOnline()` | Returns all currently active user IDs |
| `ADD_IN_SESSION` | `addInSession()` | Attaches specified user to sender's session pool |
| `GET_SESSIONS_BY_ID` | `fetchSessionsById()` | Returns all users associated with specified session |
| `DELETE_FROM_SESSION`| `deleteFromSession()` | Removes target user from sender's active session |
| `GET_ONLINE_FRIENDS` | `fetchOnlineFriends()` | Returns online status array for provided list of friend IDs |

### 4. Credentials Generation

HMAC credentials for TURN or session validation:

```text
username = "{expiry_timestamp}:{user_id}"
password = base64(HMAC-SHA1(secret, username))
```

Expiration is configured via `signaling.credsTTL` (default: 24 hours).

### 5. Session Model

Each connected user manages a `Session` record:
- `UserId`: Session owner.
- `ConnectedUsers`: Array of participant IDs currently connected to this group session.

---

## License

This project is licensed under the [MIT License](LICENSE).