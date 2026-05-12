# AeroShare API

This file is kept for compatibility with older links. The project documentation is now English-only.

AeroShare API is a Rust file-sharing application that uses a relay server. The project is split into three workspace crates:

- `server` - WebSocket/HTTP relay server that registers clients and streams file data between them.
- `client` - client application that indexes a local shared folder, connects to the server, and uploads/downloads files.
- `shared` - shared message types used by both the server and the client.

## Security

The current version requires an authorization token for:

- WebSocket connections: `/ws?token=...`
- stream uploads: `Authorization: Bearer <token>`
- stream downloads: `Authorization: Bearer <token>`

The server does not print the token in logs. `server_token.txt`, `client_token.txt`, `.env`, `shared_files/`, and `target/` are ignored by git.

The client also validates file paths before reading files:

- absolute paths are rejected,
- `..` path traversal is rejected,
- `canonicalize` is used to block reads outside the configured shared directory.

For use outside a local network, put the server behind a TLS reverse proxy and use `wss://`/`https://`. The WebSocket token is currently passed in the URL query string, so proxy URL logging should be restricted in public deployments.

The server can also expose files from `SERVER_SHARED_DIR` as the special download target `server`.

## Requirements

- Rust stable with Cargo
- Windows, Linux, or macOS

Install Rust from:

```text
https://rustup.rs
```

## Build And Verify

```bash
cargo check
cargo test
cargo clippy --all-targets --all-features -- -D warnings
```

## Server Configuration

The server needs an authorization token. You can provide it through an environment variable.

PowerShell:

```powershell
$env:SERVER_TOKEN="paste_a_long_random_token_here"
cargo run -p server
```

Linux/macOS:

```bash
SERVER_TOKEN="paste_a_long_random_token_here" cargo run -p server
```

If `SERVER_TOKEN` is not set, the server uses `server_token.txt`. If that file does not exist, the server generates a new token and writes it to `server_token.txt`.

By default, the server listens on:

```text
0.0.0.0:5000
```

Server-hosted files are read from:

```text
./server_files
```

The server logs the exact absolute path at startup. If `server-files` or `files server` is empty, put the files in that logged directory or set `SERVER_SHARED_DIR`.

Files downloaded by the server console are written to:

```text
./server_downloads
```

## Client Configuration

The client must use the same token as the server.

Option 1: environment variables.

PowerShell:

```powershell
$env:SERVER_TOKEN="same_token_as_the_server"
$env:SERVER_URL="127.0.0.1:5000"
$env:SHARED_DIR="C:\path\to\folder"
cargo run -p client
```

Linux/macOS:

```bash
SERVER_TOKEN="same_token_as_the_server" SERVER_URL="127.0.0.1:5000" SHARED_DIR="/home/user/files" cargo run -p client
```

Option 2: create `client_token.txt` in the project root:

```text
same_token_as_the_server
```

If `SHARED_DIR` is not set, the client uses:

```text
./shared_files
```

If `SERVER_URL` is not set, the client connects to:

```text
127.0.0.1:5000
```

## Local Usage

1. Start the server:

```bash
cargo run -p server
```

2. Copy the token from `server_token.txt` to `client_token.txt`, or set `SERVER_TOKEN`.

3. Create a folder with files to share:

```bash
mkdir shared_files
```

4. Start the client:

```bash
cargo run -p client
```

5. Start a second client on another machine or in another working directory with the same token and a `SERVER_URL` that points to the server.

6. To download a file from another connected client, use the client command:

```text
download <client_id> <file_path>
```

Example:

```text
download 8f3c2f6a-0f6d-4c57-9c6e-cf7f9d6f4b1a test.txt
```

The requesting client saves downloaded files in `./downloads`.

To list files shared by the server from the client console:

```text
server-files
```

You can also use:

```text
files server
```

To download from the server's own shared folder, use:

```text
download server test.txt
```

The server console can also request a file from a connected client:

```text
download <client_id> <file_path>
```

The server saves those files in `./server_downloads`.

To list connected clients from the server console:

```text
clients
```

The `clients` command also includes the special `server` target and its file count.

## Environment Variables

- `SERVER_TOKEN` - authorization token used by the server and client.
- `SERVER_BIND` - server bind address, defaults to `0.0.0.0:5000`.
- `SERVER_DOWNLOAD_DIR` - server-side download directory, defaults to `./server_downloads`.
- `SERVER_SHARED_DIR` - server-side shared directory, defaults to `./server_files`.
- `SERVER_URL` - server address used by the client, defaults to `127.0.0.1:5000`.
- `SHARED_DIR` - client directory to share, defaults to `./shared_files`.
- `RUST_LOG` - log level, for example `debug`.

Example:

```bash
RUST_LOG=debug cargo run -p server
```

## Operational Notes

- Do not commit tokens or private files.
- Do not expose the server publicly without TLS and controlled logging.
- Any client with the valid token can request files from other connected clients, so treat the token as an administrative secret.
