use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use shared::{ClientMessage, FileMetadata, ServerMessage, SharedFolder};
use std::{
    env, fs,
    path::{Component, Path, PathBuf},
    sync::Arc,
};
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, http::HeaderValue, protocol::Message},
};
use tracing::{error, info, warn};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let token = env::var("SERVER_TOKEN").unwrap_or_else(|_| {
        fs::read_to_string("client_token.txt")
            .expect("SERVER_TOKEN is missing and client_token.txt could not be read")
            .trim()
            .to_string()
    });

    if token.is_empty() {
        anyhow::bail!("Token cannot be empty");
    }

    let shared_dir = env::var("SHARED_DIR").unwrap_or_else(|_| "./shared_files".to_string());
    fs::create_dir_all(&shared_dir)?;
    info!("Client shared folder: {}", shared_dir);

    let mut files = Vec::new();
    for entry in walkdir::WalkDir::new(&shared_dir)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        if entry.file_type().is_file() {
            let metadata = entry.metadata()?;
            let path = entry
                .path()
                .strip_prefix(&shared_dir)?
                .to_string_lossy()
                .to_string();
            files.push(FileMetadata {
                path: path.replace('\\', "/"),
                size: metadata.len(),
            });
        }
    }
    info!("Indexed {} files", files.len());

    let server_url = env::var("SERVER_URL").unwrap_or_else(|_| "127.0.0.1:5000".to_string());
    let ws_url = format!("ws://{}/ws", server_url);
    info!("Connecting to WS server: {}", server_url);

    let mut ws_request = ws_url.into_client_request()?;
    ws_request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {}", token))?,
    );

    let (ws_stream, _) = connect_async(ws_request).await?;
    info!("Connected to WS server");

    let (mut write, mut read) = ws_stream.split();

    let reg_msg = ClientMessage::Register {
        folder: SharedFolder { files },
    };
    write
        .send(Message::Text(serde_json::to_string(&reg_msg)?))
        .await?;

    info!(
        "Commands: clients, server-files, files <client_id|server>, download <client_id|server> <file_path>, help"
    );

    tokio::spawn(async move {
        let stdin = BufReader::new(tokio::io::stdin());
        let mut lines = stdin.lines();

        while let Ok(Some(line)) = lines.next_line().await {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }

            if line.eq_ignore_ascii_case("help") {
                info!("Commands: clients, server-files, files <client_id|server>, download <client_id|server> <file_path>, help");
                continue;
            }

            let mut parts = line.splitn(3, ' ');
            let command = parts.next().unwrap_or_default();
            let target_client_id = parts.next().unwrap_or_default().trim();
            let file_path = parts.next().unwrap_or_default().trim();

            if command.eq_ignore_ascii_case("clients")
                && target_client_id.is_empty()
                && file_path.is_empty()
            {
                let msg = ClientMessage::ListClients;
                match serde_json::to_string(&msg) {
                    Ok(json) => {
                        if write.send(Message::Text(json)).await.is_err() {
                            error!("Could not send the client list command to the server");
                            break;
                        }
                    }
                    Err(e) => error!("Could not prepare the client list command: {}", e),
                }
                continue;
            }

            if command.eq_ignore_ascii_case("server-files")
                && target_client_id.is_empty()
                && file_path.is_empty()
            {
                let msg = ClientMessage::ListFiles {
                    target_client_id: "server".to_string(),
                };
                match serde_json::to_string(&msg) {
                    Ok(json) => {
                        if write.send(Message::Text(json)).await.is_err() {
                            error!("Could not send the server file list command");
                            break;
                        }
                    }
                    Err(e) => {
                        error!("Could not prepare the server file list command: {}", e)
                    }
                }
                continue;
            }

            if command.eq_ignore_ascii_case("files")
                && !target_client_id.is_empty()
                && file_path.is_empty()
            {
                let msg = ClientMessage::ListFiles {
                    target_client_id: target_client_id.to_string(),
                };
                match serde_json::to_string(&msg) {
                    Ok(json) => {
                        if write.send(Message::Text(json)).await.is_err() {
                            error!("Could not send the file list command to the server");
                            break;
                        }
                    }
                    Err(e) => error!("Could not prepare the file list command: {}", e),
                }
                continue;
            }

            if !command.eq_ignore_ascii_case("download")
                || target_client_id.is_empty()
                || file_path.is_empty()
            {
                warn!("Unknown command. Usage: clients, server-files, files <client_id|server>, download <client_id|server> <file_path>");
                continue;
            }

            let msg = ClientMessage::RequestDownload {
                target_client_id: target_client_id.to_string(),
                file_path: file_path.replace('\\', "/"),
            };

            match serde_json::to_string(&msg) {
                Ok(json) => {
                    if write.send(Message::Text(json)).await.is_err() {
                        error!("Could not send the download command to the server");
                        break;
                    }
                }
                Err(e) => error!("Could not prepare the download command: {}", e),
            }
        }
    });

    let http_client = Client::new();
    let shared_dir = Arc::new(shared_dir);
    let server_url = Arc::new(server_url);
    let auth_token = Arc::new(token);

    let mut server_closed_connection = false;
    while let Some(msg) = read.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                let Ok(server_msg) = serde_json::from_str::<ServerMessage>(&text) else {
                    warn!("Received an invalid message from the server");
                    continue;
                };

                match server_msg {
                    ServerMessage::Registered { client_id } => {
                        info!("Registered successfully. My ID: {}", client_id);
                    }
                    ServerMessage::ClientsList { clients } => {
                        if clients.is_empty() {
                            info!("No available targets");
                        } else {
                            info!("Available targets:");
                            for client in clients {
                                info!("- {} ({} files)", client.client_id, client.files_count);
                            }
                        }
                    }
                    ServerMessage::FileList {
                        target_client_id,
                        files,
                    } => {
                        if files.is_empty() {
                            info!("No files for: {}", target_client_id);
                        } else {
                            info!("Files for {}:", target_client_id);
                            for file in files {
                                info!("- {} ({} B)", file.path, file.size);
                            }
                        }
                    }
                    ServerMessage::UploadInstruction {
                        file_path,
                        stream_id,
                        stream_token,
                    } => {
                        info!("Server requested file upload: {}", file_path);

                        if !is_safe_relative_path(&file_path) {
                            warn!("Invalid file path structure: {}", file_path);
                            continue;
                        }

                        let mut full_path = PathBuf::from(shared_dir.as_str());
                        full_path.push(&file_path);

                        let shared_dir_canon = match std::fs::canonicalize(shared_dir.as_str()) {
                            Ok(path) => path,
                            Err(e) => {
                                error!("Could not verify the shared folder: {}", e);
                                continue;
                            }
                        };
                        let is_safe = match std::fs::canonicalize(&full_path) {
                            Ok(canon_path) => canon_path.starts_with(&shared_dir_canon),
                            Err(_) => false,
                        };

                        if !is_safe {
                            warn!("Blocked an attempt to read outside the shared folder");
                            continue;
                        }

                        let http_client_clone = http_client.clone();
                        let server_url_clone = server_url.clone();
                        let auth_token_clone = auth_token.clone();
                        tokio::spawn(async move {
                            match File::open(&full_path).await {
                                Ok(file) => {
                                    let reader_stream = tokio_util::io::ReaderStream::with_capacity(
                                        file,
                                        256 * 1024,
                                    );
                                    let upload_url =
                                        format!("http://{}/stream/{}", server_url_clone, stream_id);
                                    info!("Streaming file to relay server");

                                    let res = http_client_clone
                                        .post(&upload_url)
                                        .bearer_auth(auth_token_clone.as_str())
                                        .header("x-stream-token", stream_token.as_str())
                                        .body(reqwest::Body::wrap_stream(reader_stream))
                                        .send()
                                        .await;

                                    match res {
                                        Ok(r) if r.status().is_success() => {
                                            info!("File upload completed successfully")
                                        }
                                        Ok(r) => {
                                            error!("HTTP error during upload: {}", r.status())
                                        }
                                        Err(e) => error!("Network error: {}", e),
                                    }
                                }
                                Err(e) => {
                                    error!("Could not open file for upload: {}", e);
                                }
                            }
                        });
                    }
                    ServerMessage::DownloadReady {
                        stream_id,
                        file_name,
                        stream_token,
                    } => {
                        info!("Downloading file: {}", file_name);
                        let http_client_clone = http_client.clone();
                        let server_url_clone = server_url.clone();
                        let auth_token_clone = auth_token.clone();

                        tokio::spawn(async move {
                            let download_url =
                                format!("http://{}/stream/{}", server_url_clone, stream_id);
                            match http_client_clone
                                .get(&download_url)
                                .bearer_auth(auth_token_clone.as_str())
                                .header("x-stream-token", stream_token.as_str())
                                .send()
                                .await
                            {
                                Ok(mut response) if response.status().is_success() => {
                                    let downloads_dir = PathBuf::from("./downloads");
                                    let _ = std::fs::create_dir_all(&downloads_dir);

                                    let safe_file_name = PathBuf::from(&file_name)
                                        .file_name()
                                        .unwrap_or_default()
                                        .to_string_lossy()
                                        .into_owned();

                                    if safe_file_name.is_empty() {
                                        error!("Server returned an empty file name");
                                        return;
                                    }

                                    let file_path = downloads_dir.join(safe_file_name);

                                    if let Ok(file) = tokio::fs::File::create(&file_path).await {
                                        use tokio::io::AsyncWriteExt;

                                        let mut buf_writer =
                                            tokio::io::BufWriter::with_capacity(256 * 1024, file);

                                        while let Ok(Some(chunk)) = response.chunk().await {
                                            if buf_writer.write_all(&chunk).await.is_err() {
                                                error!("Error writing file to disk");
                                                break;
                                            }
                                        }
                                        let _ = buf_writer.flush().await;
                                        info!("Saved file successfully: {:?}", file_path);
                                    } else {
                                        error!("Could not create file: {:?}", file_path);
                                    }
                                }
                                Ok(response) => {
                                    error!("Server error during download: {}", response.status())
                                }
                                Err(e) => error!("Network error: {}", e),
                            }
                        });
                    }
                    ServerMessage::Error { message } => {
                        error!("Server error: {}", message);
                    }
                }
            }
            Ok(Message::Close(frame)) => {
                server_closed_connection = true;
                if let Some(frame) = frame {
                    warn!(
                        "Server closed the connection: {} ({})",
                        frame.reason, frame.code
                    );
                } else {
                    warn!("Server closed the connection");
                }
                break;
            }
            Ok(_) => {}
            Err(e) => {
                error!("Lost connection to the server: {}", e);
                break;
            }
        }
    }

    if !server_closed_connection {
        warn!("Disconnected from the server");
    }

    Ok(())
}

fn is_safe_relative_path(file_path: &str) -> bool {
    !file_path.is_empty()
        && Path::new(file_path)
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
}
