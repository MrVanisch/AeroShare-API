use axum::{
    body::{Body, Bytes},
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, Query, State,
    },
    http::{header::AUTHORIZATION, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use futures_util::{sink::SinkExt, stream::StreamExt};
use http_body_util::BodyExt;
use rand::{distributions::Alphanumeric, Rng};
use serde::Deserialize;
use std::{
    collections::HashMap,
    env, fs,
    path::{Component, Path as StdPath, PathBuf},
    sync::Arc,
};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, RwLock};
use tracing::{error, info, warn};

use shared::{ClientInfo, ClientMessage, FileMetadata, ServerMessage, SharedFolder};

type StreamChunk = Result<Bytes, std::io::Error>;

struct StreamEntry {
    sender: Option<mpsc::Sender<StreamChunk>>,
    receiver: Option<mpsc::Receiver<StreamChunk>>,
}

struct AppState {
    auth_token: String,
    clients: RwLock<HashMap<String, mpsc::Sender<ServerMessage>>>,
    folders: RwLock<HashMap<String, SharedFolder>>,
    streams: RwLock<HashMap<String, StreamEntry>>,
    server_shared_dir: PathBuf,
    server_download_dir: PathBuf,
}

#[derive(Deserialize)]
struct AuthQuery {
    token: String,
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
        let new_token: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(48)
            .map(char::from)
            .collect();
        fs::write(token_path, &new_token)?;
        info!(
            "Generated a new authentication token and wrote it to {}",
            token_path
        );
        new_token
    };

    info!("Server started with token authentication enabled");
    let server_shared_dir =
        PathBuf::from(env::var("SERVER_SHARED_DIR").unwrap_or_else(|_| "./server_files".into()));
    fs::create_dir_all(&server_shared_dir)?;
    info!("Server shared folder: {:?}", server_shared_dir);
    let server_download_dir = PathBuf::from(
        env::var("SERVER_DOWNLOAD_DIR").unwrap_or_else(|_| "./server_downloads".into()),
    );
    fs::create_dir_all(&server_download_dir)?;
    info!("Server download folder: {:?}", server_download_dir);

    let state = Arc::new(AppState {
        auth_token: token,
        clients: RwLock::new(HashMap::new()),
        folders: RwLock::new(HashMap::new()),
        streams: RwLock::new(HashMap::new()),
        server_shared_dir,
        server_download_dir,
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

async fn ws_handler(
    ws: WebSocketUpgrade,
    Query(query): Query<AuthQuery>,
    State(state): State<Arc<AppState>>,
) -> Response {
    if query.token != state.auth_token {
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
        while let Some(Ok(Message::Text(text))) = receiver.next().await {
            let client_msg = match serde_json::from_str::<ClientMessage>(&text) {
                Ok(msg) => msg,
                Err(err) => {
                    warn!("Invalid message from client {}: {}", cid, err);
                    continue;
                }
            };

            match client_msg {
                ClientMessage::Register { folder } => {
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

    let clients_read = state.clients.read().await;
    let Some(target_tx) = clients_read.get(&target_client_id).cloned() else {
        let _ = requester_tx
            .send(ServerMessage::Error {
                message: "Target is unavailable".into(),
            })
            .await;
        return;
    };
    drop(clients_read);

    let stream_id = uuid::Uuid::new_v4().to_string();
    let (stream_tx, stream_rx) = mpsc::channel(1024);
    state.streams.write().await.insert(
        stream_id.clone(),
        StreamEntry {
            sender: Some(stream_tx),
            receiver: Some(stream_rx),
        },
    );

    let file_name = std::path::PathBuf::from(&file_path)
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();

    let _ = requester_tx
        .send(ServerMessage::DownloadReady {
            stream_id: stream_id.clone(),
            file_name,
        })
        .await;

    let upload_req = ServerMessage::UploadInstruction {
        file_path,
        stream_id: stream_id.clone(),
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

async fn send_clients_list(state: &Arc<AppState>, requester_tx: &mpsc::Sender<ServerMessage>) {
    let clients = state.clients.read().await;
    let folders = state.folders.read().await;
    let clients = clients
        .keys()
        .map(|client_id| ClientInfo {
            client_id: client_id.clone(),
            files_count: folders
                .get(client_id)
                .map(|folder| folder.files.len())
                .unwrap_or_default(),
        })
        .collect();

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

    let stream_id = uuid::Uuid::new_v4().to_string();
    let (stream_tx, stream_rx) = mpsc::channel(1024);
    state.streams.write().await.insert(
        stream_id.clone(),
        StreamEntry {
            sender: Some(stream_tx.clone()),
            receiver: Some(stream_rx),
        },
    );

    let file_name = PathBuf::from(&file_path)
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();

    let _ = requester_tx
        .send(ServerMessage::DownloadReady {
            stream_id: stream_id.clone(),
            file_name,
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

    let clients_read = state.clients.read().await;
    let Some(target_tx) = clients_read.get(&target_client_id).cloned() else {
        warn!("Client not found: {}", target_client_id);
        return;
    };
    drop(clients_read);

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
    let (stream_tx, stream_rx) = mpsc::channel(1024);
    state.streams.write().await.insert(
        stream_id.clone(),
        StreamEntry {
            sender: Some(stream_tx),
            receiver: Some(stream_rx),
        },
    );

    let upload_req = ServerMessage::UploadInstruction {
        file_path,
        stream_id: stream_id.clone(),
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
    let expected = format!("Bearer {}", state.auth_token);
    headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value == expected)
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

    let sender = {
        let streams = state.streams.read().await;
        streams
            .get(&stream_id)
            .and_then(|entry| entry.sender.clone())
    };

    let Some(sender) = sender else {
        warn!("Invalid stream_id during upload: {}", stream_id);
        return StatusCode::NOT_FOUND.into_response();
    };

    info!("Started receiving stream: {}", stream_id);
    while let Some(chunk_res) = body.frame().await {
        match chunk_res {
            Ok(frame) => {
                if let Ok(bytes) = frame.into_data() {
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
