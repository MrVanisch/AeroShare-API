use axum::{
    body::{Body, Bytes},
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, State,
    },
    http::{header::AUTHORIZATION, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use futures_util::{sink::SinkExt, stream::StreamExt};
use http_body_util::BodyExt;
use rand::{distributions::Alphanumeric, Rng};
use std::{
    collections::{HashMap, HashSet},
    env, fs,
    path::{Component, Path as StdPath, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, RwLock};
use tracing::{error, info, warn};

use shared::{ClientInfo, ClientMessage, FileMetadata, ServerMessage, SharedFolder};

type StreamChunk = Result<Bytes, std::io::Error>;

const STREAM_TOKEN_HEADER: &str = "x-stream-token";
const DEFAULT_MAX_REGISTERED_FILES: usize = 10_000;
const DEFAULT_MAX_FILE_PATH_BYTES: usize = 1024;
const DEFAULT_MAX_STREAM_BYTES: u64 = 1024 * 1024 * 1024;
const DEFAULT_STREAM_TTL_SECS: u64 = 600;
const TOKEN_LEN: usize = 48;

struct StreamEntry {
    sender: Option<mpsc::Sender<StreamChunk>>,
    receiver: Option<mpsc::Receiver<StreamChunk>>,
    upload_token: String,
    download_token: String,
    expected_size: Option<u64>,
}

struct AppState {
    auth_token: String,
    clients: RwLock<HashMap<String, mpsc::Sender<ServerMessage>>>,
    folders: RwLock<HashMap<String, SharedFolder>>,
    streams: RwLock<HashMap<String, StreamEntry>>,
    server_shared_dir: PathBuf,
    server_download_dir: PathBuf,
    max_stream_bytes: u64,
    stream_ttl: Duration,
}

fn generate_secret(len: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

fn write_secret_file(path: &str, secret: &str) -> anyhow::Result<()> {
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);

    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }

    use std::io::Write;
    let mut file = options.open(path)?;
    file.write_all(secret.as_bytes())?;
    file.flush()?;
    Ok(())
}

fn read_u64_env(name: &str, default_value: u64) -> anyhow::Result<u64> {
    match env::var(name) {
        Ok(value) => value
            .parse::<u64>()
            .map_err(|e| anyhow::anyhow!("Invalid {} value: {}", name, e)),
        Err(_) => Ok(default_value),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let token_path = "server_token.txt";
    let token = if let Ok(env_token) = env::var("SERVER_TOKEN") {
        if env_token.trim().is_empty() {
            anyhow::bail!("SERVER_TOKEN cannot be empty");
        }
        info!("Using token from SERVER_TOKEN");
        env_token.trim().to_string()
    } else if let Ok(existing_token) = fs::read_to_string(token_path) {
        existing_token.trim().to_string()
    } else {
        let new_token = generate_secret(TOKEN_LEN);
        write_secret_file(token_path, &new_token)?;
        info!(
            "Generated a new authentication token and wrote it to {}",
            token_path
        );
        new_token
    };

    info!("Server started with token authentication enabled");
    let max_stream_bytes = read_u64_env("MAX_STREAM_BYTES", DEFAULT_MAX_STREAM_BYTES)?;
    let stream_ttl = Duration::from_secs(read_u64_env("STREAM_TTL_SECS", DEFAULT_STREAM_TTL_SECS)?);
    info!("Maximum stream size: {} bytes", max_stream_bytes);
    info!("Stream lifetime: {} seconds", stream_ttl.as_secs());

    let server_shared_dir =
        PathBuf::from(env::var("SERVER_SHARED_DIR").unwrap_or_else(|_| "./server_files".into()));
    fs::create_dir_all(&server_shared_dir)?;
    let server_shared_dir = fs::canonicalize(&server_shared_dir)?;
    info!("Server shared folder: {:?}", server_shared_dir);
    let server_download_dir = PathBuf::from(
        env::var("SERVER_DOWNLOAD_DIR").unwrap_or_else(|_| "./server_downloads".into()),
    );
    fs::create_dir_all(&server_download_dir)?;
    let server_download_dir = fs::canonicalize(&server_download_dir)?;
    info!("Server download folder: {:?}", server_download_dir);

    let state = Arc::new(AppState {
        auth_token: token,
        clients: RwLock::new(HashMap::new()),
        folders: RwLock::new(HashMap::new()),
        streams: RwLock::new(HashMap::new()),
        server_shared_dir,
        server_download_dir,
        max_stream_bytes,
        stream_ttl,
    });

    tokio::spawn(read_server_commands(state.clone()));

    let app = Router::new()
        .route("/ws", get(ws_handler))
        .route(
            "/stream/:id",
            axum::routing::post(handle_upload).get(handle_download),
        )
        .with_state(state);

    let bind_addr = env::var("SERVER_BIND").unwrap_or_else(|_| "0.0.0.0:5000".to_string());
    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
    info!("Server listening on {}", listener.local_addr()?);
    axum::serve(listener, app).await?;

    Ok(())
}

async fn read_server_commands(state: Arc<AppState>) {
    println!("Server commands: clients, server-files, files <client_id|server>, download <client_id|server> <file_path>, help");

    let stdin = BufReader::new(tokio::io::stdin());
    let mut lines = stdin.lines();

    while let Ok(Some(line)) = lines.next_line().await {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        if line.eq_ignore_ascii_case("help") {
            println!("Commands: clients, server-files, files <client_id|server>, download <client_id|server> <file_path>, help");
            continue;
        }

        if line.eq_ignore_ascii_case("clients") {
            list_connected_clients(&state).await;
            continue;
        }

        if line.eq_ignore_ascii_case("server-files") {
            list_files_for_target(&state, "server").await;
            continue;
        }

        let mut parts = line.splitn(3, ' ');
        let command = parts.next().unwrap_or_default();
        let target_client_id = parts.next().unwrap_or_default().trim();
        let file_path = parts.next().unwrap_or_default().trim();

        if command.eq_ignore_ascii_case("files")
            && !target_client_id.is_empty()
            && file_path.is_empty()
        {
            list_files_for_target(&state, target_client_id).await;
            continue;
        }

        if !command.eq_ignore_ascii_case("download")
            || target_client_id.is_empty()
            || file_path.is_empty()
        {
            println!("Unknown command. Usage: clients, server-files, files <client_id|server>, download <client_id|server> <file_path>");
            continue;
        }

        request_client_file_for_server(
            &state,
            target_client_id.to_string(),
            file_path.replace('\\', "/"),
        )
        .await;
    }
}

async fn list_connected_clients(state: &Arc<AppState>) {
    let clients = state.clients.read().await;
    let folders = state.folders.read().await;
    let server_files = match collect_server_files(state) {
        Ok(files) => files,
        Err(e) => {
            println!("Error listing server files: {}", e);
            Vec::new()
        }
    };

    println!("Available targets:");
    println!("- server ({} files)", server_files.len());
    if server_files.is_empty() {
        println!("  no files");
    } else {
        for file in &server_files {
            println!("  - {} ({} B)", file.path, file.size);
        }
    }

    if clients.is_empty() {
        println!("No connected clients");
        return;
    }

    println!("Connected clients:");
    for client_id in clients.keys() {
        if let Some(folder) = folders.get(client_id) {
            println!("- {} ({} files)", client_id, folder.files.len());
            if folder.files.is_empty() {
                println!("  no files");
            } else {
                for file in &folder.files {
                    println!("  - {} ({} B)", file.path, file.size);
                }
            }
        } else {
            println!("- {} (no registered file list)", client_id);
        }
    }
}

async fn list_files_for_target(state: &Arc<AppState>, target_client_id: &str) {
    if target_client_id.eq_ignore_ascii_case("server") {
        match collect_server_files(state) {
            Ok(files) => print_files("server", &files),
            Err(e) => println!("Error listing server files: {}", e),
        }
        return;
    }

    let clients = state.clients.read().await;
    if !clients.contains_key(target_client_id) {
        println!("Client not found: {}", target_client_id);
        return;
    }
    drop(clients);

    let folders = state.folders.read().await;
    let Some(folder) = folders.get(target_client_id) else {
        println!(
            "Client {} has not registered a file list yet",
            target_client_id
        );
        return;
    };

    print_files(target_client_id, &folder.files);
}

fn print_files(target_client_id: &str, files: &[FileMetadata]) {
    if files.is_empty() {
        println!("No files for: {}", target_client_id);
        return;
    }

    println!("Files for {}:", target_client_id);
    for file in files {
        println!("- {} ({} B)", file.path, file.size);
    }
}

fn collect_server_files(state: &AppState) -> anyhow::Result<Vec<FileMetadata>> {
    let mut files = Vec::new();

    for entry in walkdir::WalkDir::new(&state.server_shared_dir)
        .into_iter()
        .filter_map(|entry| entry.ok())
    {
        if entry.file_type().is_file() {
            let metadata = entry.metadata()?;
            let path = entry
                .path()
                .strip_prefix(&state.server_shared_dir)?
                .to_string_lossy()
                .replace('\\', "/");
            files.push(FileMetadata {
                path,
                size: metadata.len(),
            });
        }
    }

    Ok(files)
}

fn validate_shared_folder(
    folder: SharedFolder,
    max_stream_bytes: u64,
) -> Result<SharedFolder, String> {
    if folder.files.len() > DEFAULT_MAX_REGISTERED_FILES {
        return Err(format!(
            "Too many registered files; limit is {}",
            DEFAULT_MAX_REGISTERED_FILES
        ));
    }

    let mut seen_paths = HashSet::new();
    let mut files = Vec::with_capacity(folder.files.len());

    for mut file in folder.files {
        file.path = file.path.replace('\\', "/");

        if file.path.len() > DEFAULT_MAX_FILE_PATH_BYTES {
            return Err(format!(
                "File path is too long; limit is {} bytes",
                DEFAULT_MAX_FILE_PATH_BYTES
            ));
        }

        if !is_safe_relative_path(&file.path) {
            return Err(format!("Invalid registered file path: {}", file.path));
        }

        if file.size > max_stream_bytes {
            return Err(format!(
                "Registered file exceeds the stream limit: {}",
                file.path
            ));
        }

        if !seen_paths.insert(file.path.clone()) {
            return Err(format!("Duplicate registered file path: {}", file.path));
        }

        files.push(file);
    }

    Ok(SharedFolder { files })
}

fn find_registered_file(folder: &SharedFolder, file_path: &str) -> Option<FileMetadata> {
    folder
        .files
        .iter()
        .find(|file| file.path == file_path)
        .cloned()
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
) -> Response {
    if !is_authorized(&headers, &state) {
        warn!("Rejected WS connection because of an invalid token");
        return StatusCode::UNAUTHORIZED.into_response();
    }

    ws.on_upgrade(move |socket| handle_socket(socket, state))
        .into_response()
}

async fn handle_socket(socket: WebSocket, state: Arc<AppState>) {
    let client_id = uuid::Uuid::new_v4().to_string();
    info!("Client connected. Registered ID: {}", client_id);

    let (tx, mut rx) = mpsc::channel(100);
    state
        .clients
        .write()
        .await
        .insert(client_id.clone(), tx.clone());

    let (mut sender, mut receiver) = socket.split();

    let msg = ServerMessage::Registered {
        client_id: client_id.clone(),
    };
    if let Ok(json) = serde_json::to_string(&msg) {
        if sender.send(Message::Text(json)).await.is_err() {
            cleanup_client(&state, &client_id).await;
            return;
        }
    }

    let mut send_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            match serde_json::to_string(&msg) {
                Ok(json) => {
                    if sender.send(Message::Text(json)).await.is_err() {
                        break;
                    }
                }
                Err(err) => {
                    error!("Could not serialize server message: {}", err);
                    break;
                }
            }
        }
    });

    let state_clone = state.clone();
    let cid = client_id.clone();
    let mut recv_task = tokio::spawn(async move {
        while let Some(msg) = receiver.next().await {
            let text = match msg {
                Ok(Message::Text(text)) => text,
                Ok(Message::Close(frame)) => {
                    if let Some(frame) = frame {
                        info!(
                            "Client {} closed the connection: {} ({})",
                            cid, frame.reason, frame.code
                        );
                    } else {
                        info!("Client {} closed the connection", cid);
                    }
                    break;
                }
                Ok(_) => continue,
                Err(err) => {
                    warn!("Connection error from client {}: {}", cid, err);
                    break;
                }
            };

            let client_msg = match serde_json::from_str::<ClientMessage>(&text) {
                Ok(msg) => msg,
                Err(err) => {
                    warn!("Invalid message from client {}: {}", cid, err);
                    continue;
                }
            };

            match client_msg {
                ClientMessage::Register { folder } => {
                    let folder = match validate_shared_folder(folder, state_clone.max_stream_bytes)
                    {
                        Ok(folder) => folder,
                        Err(message) => {
                            warn!(
                                "Rejected invalid file registration from {}: {}",
                                cid, message
                            );
                            let _ = tx.send(ServerMessage::Error { message }).await;
                            continue;
                        }
                    };

                    info!(
                        "Client {} shared a folder with {} files",
                        cid,
                        folder.files.len()
                    );
                    state_clone
                        .folders
                        .write()
                        .await
                        .insert(cid.clone(), folder);
                }
                ClientMessage::ListClients => {
                    send_clients_list(&state_clone, &tx).await;
                }
                ClientMessage::ListFiles { target_client_id } => {
                    send_files_list(&state_clone, &tx, target_client_id).await;
                }
                ClientMessage::RequestDownload {
                    target_client_id,
                    file_path,
                } => {
                    info!("Client {} requested a file from {}", cid, target_client_id);
                    request_download(&state_clone, &tx, target_client_id, file_path).await;
                }
            }
        }
    });

    tokio::select! {
        _ = (&mut send_task) => recv_task.abort(),
        _ = (&mut recv_task) => send_task.abort(),
    }

    info!("Client {} disconnected", client_id);
    cleanup_client(&state, &client_id).await;
}

async fn request_download(
    state: &Arc<AppState>,
    requester_tx: &mpsc::Sender<ServerMessage>,
    target_client_id: String,
    file_path: String,
) {
    if target_client_id.eq_ignore_ascii_case("server") {
        request_server_file_download(state, requester_tx, file_path).await;
        return;
    }

    let file_path = file_path.replace('\\', "/");
    if !is_safe_relative_path(&file_path) {
        let _ = requester_tx
            .send(ServerMessage::Error {
                message: "Invalid target file path".into(),
            })
            .await;
        return;
    }

    let (target_tx, file_metadata) = {
        let clients_read = state.clients.read().await;
        let Some(target_tx) = clients_read.get(&target_client_id).cloned() else {
            let _ = requester_tx
                .send(ServerMessage::Error {
                    message: "Target is unavailable".into(),
                })
                .await;
            return;
        };

        let folders = state.folders.read().await;
        let Some(folder) = folders.get(&target_client_id) else {
            let _ = requester_tx
                .send(ServerMessage::Error {
                    message: "Target has not registered a file list yet".into(),
                })
                .await;
            return;
        };
        let Some(file_metadata) = find_registered_file(folder, &file_path) else {
            let _ = requester_tx
                .send(ServerMessage::Error {
                    message: "File is not registered by target".into(),
                })
                .await;
            return;
        };

        (target_tx, file_metadata)
    };

    let stream_id = uuid::Uuid::new_v4().to_string();
    let upload_token = generate_secret(TOKEN_LEN);
    let download_token = generate_secret(TOKEN_LEN);
    let _stream_tx = create_stream(
        state,
        stream_id.clone(),
        Some(file_metadata.size),
        upload_token.clone(),
        download_token.clone(),
    )
    .await;

    let file_name = std::path::PathBuf::from(&file_path)
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();

    let _ = requester_tx
        .send(ServerMessage::DownloadReady {
            stream_id: stream_id.clone(),
            file_name,
            stream_token: download_token,
        })
        .await;

    let upload_req = ServerMessage::UploadInstruction {
        file_path,
        stream_id: stream_id.clone(),
        stream_token: upload_token,
    };

    if target_tx.send(upload_req).await.is_err() {
        state.streams.write().await.remove(&stream_id);
        let _ = requester_tx
            .send(ServerMessage::Error {
                message: "Could not request file upload".into(),
            })
            .await;
    }
}

async fn create_stream(
    state: &Arc<AppState>,
    stream_id: String,
    expected_size: Option<u64>,
    upload_token: String,
    download_token: String,
) -> mpsc::Sender<StreamChunk> {
    let (stream_tx, stream_rx) = mpsc::channel(1024);
    state.streams.write().await.insert(
        stream_id.clone(),
        StreamEntry {
            sender: Some(stream_tx.clone()),
            receiver: Some(stream_rx),
            upload_token,
            download_token,
            expected_size,
        },
    );

    let state_clone = state.clone();
    tokio::spawn(async move {
        tokio::time::sleep(state_clone.stream_ttl).await;
        if state_clone
            .streams
            .write()
            .await
            .remove(&stream_id)
            .is_some()
        {
            warn!("Expired stream removed: {}", stream_id);
        }
    });

    stream_tx
}

async fn send_clients_list(state: &Arc<AppState>, requester_tx: &mpsc::Sender<ServerMessage>) {
    let connected_clients = state.clients.read().await;
    let folders = state.folders.read().await;
    let server_files_count = match collect_server_files(state) {
        Ok(files) => files.len(),
        Err(e) => {
            error!("Could not list server files for clients command: {}", e);
            0
        }
    };
    let mut clients = vec![ClientInfo {
        client_id: "server".into(),
        files_count: server_files_count,
    }];
    clients.extend(connected_clients.keys().map(|client_id| {
        ClientInfo {
            client_id: client_id.clone(),
            files_count: folders
                .get(client_id)
                .map(|folder| folder.files.len())
                .unwrap_or_default(),
        }
    }));

    let _ = requester_tx
        .send(ServerMessage::ClientsList { clients })
        .await;
}

async fn send_files_list(
    state: &Arc<AppState>,
    requester_tx: &mpsc::Sender<ServerMessage>,
    target_client_id: String,
) {
    if target_client_id.eq_ignore_ascii_case("server") {
        match collect_server_files(state) {
            Ok(files) => {
                let _ = requester_tx
                    .send(ServerMessage::FileList {
                        target_client_id: "server".into(),
                        files,
                    })
                    .await;
            }
            Err(e) => {
                error!("Could not list server files: {}", e);
                let _ = requester_tx
                    .send(ServerMessage::Error {
                        message: "Could not list server files".into(),
                    })
                    .await;
            }
        }
        return;
    }

    let clients = state.clients.read().await;
    if !clients.contains_key(&target_client_id) {
        let _ = requester_tx
            .send(ServerMessage::Error {
                message: format!("Client not found: {}", target_client_id),
            })
            .await;
        return;
    }
    drop(clients);

    let folders = state.folders.read().await;
    let Some(folder) = folders.get(&target_client_id) else {
        let _ = requester_tx
            .send(ServerMessage::Error {
                message: format!(
                    "Client {} has not registered a file list yet",
                    target_client_id
                ),
            })
            .await;
        return;
    };

    let _ = requester_tx
        .send(ServerMessage::FileList {
            target_client_id,
            files: folder.files.clone(),
        })
        .await;
}

async fn request_server_file_download(
    state: &Arc<AppState>,
    requester_tx: &mpsc::Sender<ServerMessage>,
    file_path: String,
) {
    if !is_safe_relative_path(&file_path) {
        let _ = requester_tx
            .send(ServerMessage::Error {
                message: "Invalid server file path".into(),
            })
            .await;
        return;
    }

    let mut full_path = state.server_shared_dir.clone();
    full_path.push(&file_path);

    let shared_dir_canon = match fs::canonicalize(&state.server_shared_dir) {
        Ok(path) => path,
        Err(e) => {
            error!("Could not verify the server file folder: {}", e);
            let _ = requester_tx
                .send(ServerMessage::Error {
                    message: "Server file folder is unavailable".into(),
                })
                .await;
            return;
        }
    };

    let is_safe = match fs::canonicalize(&full_path) {
        Ok(canon_path) => canon_path.starts_with(&shared_dir_canon),
        Err(_) => false,
    };

    if !is_safe {
        let _ = requester_tx
            .send(ServerMessage::Error {
                message: "Server file does not exist or is outside the folder".into(),
            })
            .await;
        return;
    }

    let file_size = match fs::metadata(&full_path) {
        Ok(metadata) if metadata.is_file() => metadata.len(),
        _ => {
            let _ = requester_tx
                .send(ServerMessage::Error {
                    message: "Server file does not exist".into(),
                })
                .await;
            return;
        }
    };

    if file_size > state.max_stream_bytes {
        let _ = requester_tx
            .send(ServerMessage::Error {
                message: format!(
                    "Server file is larger than the configured stream limit ({} bytes)",
                    state.max_stream_bytes
                ),
            })
            .await;
        return;
    }

    let stream_id = uuid::Uuid::new_v4().to_string();
    let upload_token = generate_secret(TOKEN_LEN);
    let download_token = generate_secret(TOKEN_LEN);
    let stream_tx = create_stream(
        state,
        stream_id.clone(),
        Some(file_size),
        upload_token,
        download_token.clone(),
    )
    .await;

    let file_name = PathBuf::from(&file_path)
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();

    let _ = requester_tx
        .send(ServerMessage::DownloadReady {
            stream_id: stream_id.clone(),
            file_name,
            stream_token: download_token,
        })
        .await;

    let state_clone = state.clone();
    tokio::spawn(async move {
        if let Err(e) = stream_server_file(full_path, stream_tx).await {
            error!("Error streaming server file: {}", e);
        }

        let mut streams = state_clone.streams.write().await;
        if let Some(entry) = streams.get_mut(&stream_id) {
            entry.sender = None;
            if entry.receiver.is_none() {
                streams.remove(&stream_id);
            }
        }
    });
}

async fn request_client_file_for_server(
    state: &Arc<AppState>,
    target_client_id: String,
    file_path: String,
) {
    if target_client_id.eq_ignore_ascii_case("server") {
        download_server_file_for_server(state, file_path).await;
        return;
    }

    let file_path = file_path.replace('\\', "/");
    if !is_safe_relative_path(&file_path) {
        warn!("Invalid target file path");
        return;
    };

    let (target_tx, file_metadata) = {
        let clients_read = state.clients.read().await;
        let Some(target_tx) = clients_read.get(&target_client_id).cloned() else {
            warn!("Client not found: {}", target_client_id);
            return;
        };

        let folders = state.folders.read().await;
        let Some(folder) = folders.get(&target_client_id) else {
            warn!(
                "Client {} has not registered a file list yet",
                target_client_id
            );
            return;
        };
        let Some(file_metadata) = find_registered_file(folder, &file_path) else {
            warn!(
                "Client {} has not registered requested file: {}",
                target_client_id, file_path
            );
            return;
        };

        (target_tx, file_metadata)
    };

    let file_name = PathBuf::from(&file_path)
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();

    if file_name.is_empty() {
        warn!("Cannot download a file without a name");
        return;
    }

    let stream_id = uuid::Uuid::new_v4().to_string();
    let upload_token = generate_secret(TOKEN_LEN);
    let download_token = generate_secret(TOKEN_LEN);
    let _stream_tx = create_stream(
        state,
        stream_id.clone(),
        Some(file_metadata.size),
        upload_token.clone(),
        download_token,
    )
    .await;

    let upload_req = ServerMessage::UploadInstruction {
        file_path,
        stream_id: stream_id.clone(),
        stream_token: upload_token,
    };

    if target_tx.send(upload_req).await.is_err() {
        state.streams.write().await.remove(&stream_id);
        warn!("Could not request file upload from the client");
        return;
    }

    let mut receiver = {
        let mut streams = state.streams.write().await;
        let Some(entry) = streams.get_mut(&stream_id) else {
            warn!("Download stream does not exist");
            return;
        };
        entry.receiver.take()
    };

    let Some(mut receiver) = receiver.take() else {
        warn!("Download stream has already been used");
        return;
    };

    let output_path = state.server_download_dir.join(file_name);
    info!(
        "Server is downloading file from client {} to {:?}",
        target_client_id, output_path
    );

    let result = write_stream_to_file(&mut receiver, &output_path).await;
    state.streams.write().await.remove(&stream_id);

    match result {
        Ok(()) => info!("Server saved downloaded file: {:?}", output_path),
        Err(e) => error!("Error writing downloaded file on the server: {}", e),
    }
}

async fn download_server_file_for_server(state: &Arc<AppState>, file_path: String) {
    if !is_safe_relative_path(&file_path) {
        warn!("Invalid server file path");
        return;
    }

    let mut full_path = state.server_shared_dir.clone();
    full_path.push(&file_path);

    let shared_dir_canon = match fs::canonicalize(&state.server_shared_dir) {
        Ok(path) => path,
        Err(e) => {
            error!("Could not verify the server file folder: {}", e);
            return;
        }
    };

    let full_path = match fs::canonicalize(&full_path) {
        Ok(path) if path.starts_with(&shared_dir_canon) => path,
        _ => {
            warn!("Server file does not exist or is outside the folder");
            return;
        }
    };

    let file_name = PathBuf::from(&file_path)
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();

    if file_name.is_empty() {
        warn!("Cannot download a file without a name");
        return;
    }

    let output_path = state.server_download_dir.join(file_name);
    match tokio::fs::copy(&full_path, &output_path).await {
        Ok(_) => info!("Server copied file to {:?}", output_path),
        Err(e) => error!("Error copying server file: {}", e),
    }
}

async fn write_stream_to_file(
    receiver: &mut mpsc::Receiver<StreamChunk>,
    output_path: &StdPath,
) -> anyhow::Result<()> {
    let file = tokio::fs::File::create(output_path).await?;
    let mut writer = tokio::io::BufWriter::with_capacity(256 * 1024, file);

    while let Some(chunk) = receiver.recv().await {
        writer.write_all(&chunk?).await?;
    }

    writer.flush().await?;
    Ok(())
}

async fn stream_server_file(
    full_path: PathBuf,
    sender: mpsc::Sender<StreamChunk>,
) -> anyhow::Result<()> {
    let mut file = tokio::fs::File::open(full_path).await?;
    let mut buffer = vec![0_u8; 256 * 1024];

    loop {
        let bytes_read = file.read(&mut buffer).await?;
        if bytes_read == 0 {
            break;
        }

        if sender
            .send(Ok(Bytes::copy_from_slice(&buffer[..bytes_read])))
            .await
            .is_err()
        {
            break;
        }
    }

    Ok(())
}

async fn cleanup_client(state: &Arc<AppState>, client_id: &str) {
    state.clients.write().await.remove(client_id);
    state.folders.write().await.remove(client_id);
}

fn is_authorized(headers: &HeaderMap, state: &AppState) -> bool {
    headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .is_some_and(|token| secure_eq(token, &state.auth_token))
}

fn is_stream_token_authorized(headers: &HeaderMap, expected_token: &str) -> bool {
    headers
        .get(STREAM_TOKEN_HEADER)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|token| secure_eq(token, expected_token))
}

fn secure_eq(a: &str, b: &str) -> bool {
    let a = a.as_bytes();
    let b = b.as_bytes();
    let mut diff = a.len() ^ b.len();
    let max_len = a.len().max(b.len());

    for index in 0..max_len {
        let left = a.get(index).copied().unwrap_or_default();
        let right = b.get(index).copied().unwrap_or_default();
        diff |= usize::from(left ^ right);
    }

    diff == 0
}

fn is_safe_relative_path(file_path: &str) -> bool {
    !file_path.is_empty()
        && StdPath::new(file_path)
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
}

async fn handle_upload(
    Path(stream_id): Path<String>,
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    mut body: Body,
) -> Response {
    if !is_authorized(&headers, &state) {
        warn!("Rejected stream upload without valid authorization");
        return StatusCode::UNAUTHORIZED.into_response();
    }

    let (sender, expected_size) = {
        let streams = state.streams.read().await;
        let Some(entry) = streams.get(&stream_id) else {
            warn!("Invalid stream_id during upload: {}", stream_id);
            return StatusCode::NOT_FOUND.into_response();
        };

        if !is_stream_token_authorized(&headers, &entry.upload_token) {
            warn!(
                "Rejected stream upload with invalid stream token: {}",
                stream_id
            );
            return StatusCode::FORBIDDEN.into_response();
        }

        (entry.sender.clone(), entry.expected_size)
    };

    let Some(sender) = sender else {
        warn!("Stream upload sender is no longer available: {}", stream_id);
        return StatusCode::CONFLICT.into_response();
    };

    info!("Started receiving stream: {}", stream_id);
    let max_bytes = expected_size
        .unwrap_or(state.max_stream_bytes)
        .min(state.max_stream_bytes);
    let mut bytes_received = 0_u64;

    while let Some(chunk_res) = body.frame().await {
        match chunk_res {
            Ok(frame) => {
                if let Ok(bytes) = frame.into_data() {
                    let chunk_len = bytes.len() as u64;
                    if bytes_received.saturating_add(chunk_len) > max_bytes {
                        warn!(
                            "Rejected oversized upload for stream {} after {} bytes",
                            stream_id,
                            bytes_received.saturating_add(chunk_len)
                        );
                        state.streams.write().await.remove(&stream_id);
                        return StatusCode::PAYLOAD_TOO_LARGE.into_response();
                    }
                    bytes_received += chunk_len;

                    if sender.send(Ok(bytes)).await.is_err() {
                        warn!("Downloading client disconnected");
                        break;
                    }
                }
            }
            Err(err) => {
                error!("Error reading upload body: {}", err);
                return StatusCode::BAD_REQUEST.into_response();
            }
        }
    }

    drop(sender);
    let mut streams = state.streams.write().await;
    if let Some(entry) = streams.get_mut(&stream_id) {
        entry.sender = None;
        if entry.receiver.is_none() {
            streams.remove(&stream_id);
        }
    }

    info!("Finished receiving stream: {}", stream_id);
    StatusCode::OK.into_response()
}

async fn handle_download(
    Path(stream_id): Path<String>,
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    if !is_authorized(&headers, &state) {
        warn!("Rejected stream download without valid authorization");
        return StatusCode::UNAUTHORIZED.into_response();
    }

    let receiver = {
        let mut streams = state.streams.write().await;
        let Some(entry) = streams.get_mut(&stream_id) else {
            return (StatusCode::NOT_FOUND, "Stream not found").into_response();
        };

        if !is_stream_token_authorized(&headers, &entry.download_token) {
            warn!(
                "Rejected stream download with invalid stream token: {}",
                stream_id
            );
            return StatusCode::FORBIDDEN.into_response();
        }

        let receiver = entry.receiver.take();
        if entry.sender.is_none() {
            streams.remove(&stream_id);
        }
        receiver
    };

    let Some(rx) = receiver else {
        return (StatusCode::CONFLICT, "Stream already consumed").into_response();
    };

    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    (StatusCode::OK, Body::from_stream(stream)).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn safe_relative_path_rejects_traversal_and_absolute_paths() {
        assert!(is_safe_relative_path("folder/file.txt"));
        assert!(is_safe_relative_path("file.txt"));
        assert!(!is_safe_relative_path(""));
        assert!(!is_safe_relative_path("../file.txt"));
        assert!(!is_safe_relative_path("folder/../file.txt"));
        assert!(!is_safe_relative_path("/tmp/file.txt"));
        #[cfg(windows)]
        assert!(!is_safe_relative_path("C:\\tmp\\file.txt"));
    }

    #[test]
    fn secure_eq_matches_only_equal_strings() {
        assert!(secure_eq("same-token", "same-token"));
        assert!(!secure_eq("same-token", "other-token"));
        assert!(!secure_eq("same-token", "same-token-extra"));
    }

    #[test]
    fn registration_validation_rejects_unsafe_or_duplicate_paths() {
        let invalid = SharedFolder {
            files: vec![FileMetadata {
                path: "../secret.txt".into(),
                size: 1,
            }],
        };
        assert!(validate_shared_folder(invalid, 1024).is_err());

        let duplicate = SharedFolder {
            files: vec![
                FileMetadata {
                    path: "file.txt".into(),
                    size: 1,
                },
                FileMetadata {
                    path: "file.txt".into(),
                    size: 1,
                },
            ],
        };
        assert!(validate_shared_folder(duplicate, 1024).is_err());
    }

    #[test]
    fn registration_validation_rejects_files_over_stream_limit() {
        let folder = SharedFolder {
            files: vec![FileMetadata {
                path: "large.bin".into(),
                size: 2048,
            }],
        };
        assert!(validate_shared_folder(folder, 1024).is_err());
    }
}
