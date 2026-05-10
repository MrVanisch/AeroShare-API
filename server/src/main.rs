use axum::{
    extract::{ws::{Message, WebSocket, WebSocketUpgrade}, Query, State},
    response::IntoResponse,
    routing::get,
    Router,
};
use futures_util::{sink::SinkExt, stream::StreamExt};
use rand::{distributions::Alphanumeric, Rng};
use std::{collections::HashMap, fs, sync::Arc};
use tokio::sync::{mpsc, RwLock};
use tracing::{info, warn, error};
use serde::Deserialize;

use shared::{ClientMessage, ServerMessage, SharedFolder};
use axum::body::Body;
use axum::extract::Path;

// State aplikacji serwera
struct AppState {
    auth_token: String,
    // ID klienta -> kanał do wysyłania mu wiadomości
    clients: RwLock<HashMap<String, mpsc::Sender<ServerMessage>>>,
    // ID klienta -> udostępniony folder
    folders: RwLock<HashMap<String, SharedFolder>>,
    // Strumienie: strumień_id -> nadajnik kawałków bajtów
    streams: RwLock<HashMap<String, mpsc::Sender<Result<axum::body::Bytes, std::io::Error>>>>,
}

#[derive(Deserialize)]
struct AuthQuery {
    token: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    // Inicjalizacja bezpiecznego tokenu
    let token_path = "server_token.txt";
    let token = if let Ok(existing_token) = fs::read_to_string(token_path) {
        existing_token.trim().to_string()
    } else {
        let new_token: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(32)
            .map(char::from)
            .collect();
        fs::write(token_path, &new_token)?;
        info!("Wygenerowano nowy bezpieczny token uwierzytelniający i zapisano do {}", token_path);
        new_token
    };

    info!("Serwer wymaga tokenu: {}", token);

    let state = Arc::new(AppState {
        auth_token: token,
        clients: RwLock::new(HashMap::new()),
        folders: RwLock::new(HashMap::new()),
        streams: RwLock::new(HashMap::new()),
    });

    let app = Router::new()
        .route("/ws", get(ws_handler))
        .route("/stream/:id", axum::routing::post(handle_upload).get(handle_download))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
    info!("Serwer nasłuchuje na {}", listener.local_addr()?);
    axum::serve(listener, app).await?;

    Ok(())
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    Query(query): Query<AuthQuery>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    if query.token != state.auth_token {
        warn!("Odrzucono połączenie z powodu błędnego tokenu.");
        // Zwracamy HTTP 401 Unauthorized (Axum obsłuży to jeśli po prostu nie ulepszymy)
        // Ze względu na specyfikę axum::ws, bez upgreadu zwracamy błąd.
        // Najprościej wywołać upgread i od razu zamknąć.
    }

    ws.on_upgrade(move |socket| handle_socket(socket, state, query.token))
}

async fn handle_socket(mut socket: WebSocket, state: Arc<AppState>, token: String) {
    if token != state.auth_token {
        let _ = socket.close().await;
        return;
    }

    let client_id = uuid::Uuid::new_v4().to_string();
    info!("Klient połączony. Zarejestrowano ID: {}", client_id);

    let (tx, mut rx) = mpsc::channel(100);
    state.clients.write().await.insert(client_id.clone(), tx);

    // Wysyłamy klientowi jego ID
    let msg = ServerMessage::Registered { client_id: client_id.clone() };
    if let Ok(json) = serde_json::to_string(&msg) {
        if socket.send(Message::Text(json)).await.is_err() {
            return;
        }
    }

    let (mut sender, mut receiver) = socket.split();

    // Wątek wysyłający wiadomości do klienta
    let mut send_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if let Ok(json) = serde_json::to_string(&msg) {
                if sender.send(Message::Text(json)).await.is_err() {
                    break;
                }
            }
        }
    });

    // Pętla odbierająca wiadomości od klienta
    let state_clone = state.clone();
    let cid = client_id.clone();
    let mut recv_task = tokio::spawn(async move {
        while let Some(Ok(Message::Text(text))) = receiver.next().await {
            if let Ok(client_msg) = serde_json::from_str::<ClientMessage>(&text) {
                match client_msg {
                    ClientMessage::Register { folder } => {
                        info!("Klient {} udostępnił folder z {} plikami.", cid, folder.files.len());
                        state_clone.folders.write().await.insert(cid.clone(), folder);
                    }
                    ClientMessage::RequestDownload { target_client_id, file_path } => {
                        info!("Klient {} prosi o plik {} od {}", cid, file_path, target_client_id);
                        
                        let stream_id = uuid::Uuid::new_v4().to_string();
                        let clients_read = state_clone.clients.read().await;
                        
                        if let Some(target_tx) = clients_read.get(&target_client_id) {
                            // Rejestrujemy kanał dla strumieniowania pliku (buffer: 64 kawałki)
                            let (stream_tx, stream_rx) = mpsc::channel(64);
                            state_clone.streams.write().await.insert(stream_id.clone(), stream_tx);

                            // Informujemy żądającego klienta skąd ma pobrać plik
                            let ready_msg = ServerMessage::DownloadReady { 
                                stream_id: stream_id.clone(),
                                file_name: std::path::PathBuf::from(&file_path)
                                    .file_name()
                                    .unwrap_or_default()
                                    .to_string_lossy()
                                    .to_string(),
                            };
                            if let Ok(json) = serde_json::to_string(&ready_msg) {
                                let _ = sender.send(Message::Text(json)).await;
                            }

                            // Prosimy klienta źródłowego o wysłanie pliku
                            let upload_req = ServerMessage::UploadInstruction {
                                file_path,
                                stream_id,
                            };
                            let _ = target_tx.send(upload_req).await;
                        } else {
                            let err_msg = ServerMessage::Error { message: "Cel niedostępny".into() };
                            if let Ok(json) = serde_json::to_string(&err_msg) {
                                let _ = sender.send(Message::Text(json)).await;
                            }
                        }
                    }
                }
            }
        }
    });

    // Czekamy na zakończenie jednego z zadań (rozłączenie)
    tokio::select! {
        _ = (&mut send_task) => recv_task.abort(),
        _ = (&mut recv_task) => send_task.abort(),
    }

    info!("Klient {} odłączony.", client_id);
    state.clients.write().await.remove(&client_id);
    state.folders.write().await.remove(&client_id);
}

// Endpoint do którego Klient Źródłowy wysyła dane pliku
async fn handle_upload(
    Path(stream_id): Path<String>,
    State(state): State<Arc<AppState>>,
    mut body: Body,
) -> impl IntoResponse {
    let sender = {
        let streams = state.streams.read().await;
        streams.get(&stream_id).cloned()
    };

    if let Some(sender) = sender {
        info!("Rozpoczęto odbieranie streamu: {}", stream_id);
        use axum::body::HttpBody;
        while let Some(chunk_res) = body.frame().await {
            if let Ok(frame) = chunk_res {
                if let Ok(bytes) = frame.into_data() {
                    if sender.send(Ok(bytes)).await.is_err() {
                        warn!("Klient pobierający rozłączył się.");
                        break;
                    }
                }
            }
        }
        info!("Zakończono odbieranie streamu: {}", stream_id);
        state.streams.write().await.remove(&stream_id);
    } else {
        warn!("Błędny stream_id podczas uploadu: {}", stream_id);
    }
}

// Endpoint z którego Klient Żądający pobiera plik
async fn handle_download(
    Path(stream_id): Path<String>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    // Sprawdzamy czy stream istnieje. Uwaga: Musimy poczekać aż upload założy kanał.
    // Kanał jest zakładany ZANIM upload się zacznie, podczas RequestDownload.
    let (tx, mut rx) = mpsc::channel::<Result<axum::body::Bytes, std::io::Error>>(64);
    
    // Podmieniamy nadajnik w mapie na taki, który będzie wysyłał tutaj
    let mut streams = state.streams.write().await;
    if streams.contains_key(&stream_id) {
        streams.insert(stream_id.clone(), tx);
    } else {
        return (axum::http::StatusCode::NOT_FOUND, "Stream not found").into_response();
    }
    drop(streams);
    
    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    let body = Body::from_stream(stream);
    
    (axum::http::StatusCode::OK, body).into_response()
}
