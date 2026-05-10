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
use std::{collections::HashMap, env, fs, sync::Arc};
use tokio::sync::{mpsc, RwLock};
use tracing::{error, info, warn};

use shared::{ClientMessage, ServerMessage, SharedFolder};

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
            anyhow::bail!("SERVER_TOKEN nie moze byc pusty");
        }
        info!("Uzywam tokenu z SERVER_TOKEN");
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
            "Wygenerowano nowy token uwierzytelniajacy i zapisano do {}",
            token_path
        );
        new_token
    };

    info!("Serwer uruchomiony z wlaczona autoryzacja tokenem");

    let state = Arc::new(AppState {
        auth_token: token,
        clients: RwLock::new(HashMap::new()),
        folders: RwLock::new(HashMap::new()),
        streams: RwLock::new(HashMap::new()),
    });

    let app = Router::new()
        .route("/ws", get(ws_handler))
        .route(
            "/stream/:id",
            axum::routing::post(handle_upload).get(handle_download),
        )
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
    info!("Serwer nasluchuje na {}", listener.local_addr()?);
    axum::serve(listener, app).await?;

    Ok(())
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    Query(query): Query<AuthQuery>,
    State(state): State<Arc<AppState>>,
) -> Response {
    if query.token != state.auth_token {
        warn!("Odrzucono polaczenie WS z powodu blednego tokenu");
        return StatusCode::UNAUTHORIZED.into_response();
    }

    ws.on_upgrade(move |socket| handle_socket(socket, state))
        .into_response()
}

async fn handle_socket(socket: WebSocket, state: Arc<AppState>) {
    let client_id = uuid::Uuid::new_v4().to_string();
    info!("Klient polaczony. Zarejestrowano ID: {}", client_id);

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
                    error!("Nie mozna zserializowac wiadomosci serwera: {}", err);
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
                    warn!("Niepoprawna wiadomosc od klienta {}: {}", cid, err);
                    continue;
                }
            };

            match client_msg {
                ClientMessage::Register { folder } => {
                    info!(
                        "Klient {} udostepnil folder z {} plikami",
                        cid,
                        folder.files.len()
                    );
                    state_clone
                        .folders
                        .write()
                        .await
                        .insert(cid.clone(), folder);
                }
                ClientMessage::RequestDownload {
                    target_client_id,
                    file_path,
                } => {
                    info!("Klient {} prosi o plik od {}", cid, target_client_id);
                    request_download(&state_clone, &tx, target_client_id, file_path).await;
                }
            }
        }
    });

    tokio::select! {
        _ = (&mut send_task) => recv_task.abort(),
        _ = (&mut recv_task) => send_task.abort(),
    }

    info!("Klient {} odlaczony", client_id);
    cleanup_client(&state, &client_id).await;
}

async fn request_download(
    state: &Arc<AppState>,
    requester_tx: &mpsc::Sender<ServerMessage>,
    target_client_id: String,
    file_path: String,
) {
    let clients_read = state.clients.read().await;
    let Some(target_tx) = clients_read.get(&target_client_id).cloned() else {
        let _ = requester_tx
            .send(ServerMessage::Error {
                message: "Cel niedostepny".into(),
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
                message: "Nie mozna zlecic wysylki pliku".into(),
            })
            .await;
    }
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

async fn handle_upload(
    Path(stream_id): Path<String>,
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    mut body: Body,
) -> Response {
    if !is_authorized(&headers, &state) {
        warn!("Odrzucono upload streamu bez poprawnej autoryzacji");
        return StatusCode::UNAUTHORIZED.into_response();
    }

    let sender = {
        let streams = state.streams.read().await;
        streams
            .get(&stream_id)
            .and_then(|entry| entry.sender.clone())
    };

    let Some(sender) = sender else {
        warn!("Bledny stream_id podczas uploadu: {}", stream_id);
        return StatusCode::NOT_FOUND.into_response();
    };

    info!("Rozpoczeto odbieranie streamu: {}", stream_id);
    while let Some(chunk_res) = body.frame().await {
        match chunk_res {
            Ok(frame) => {
                if let Ok(bytes) = frame.into_data() {
                    if sender.send(Ok(bytes)).await.is_err() {
                        warn!("Klient pobierajacy rozlaczyl sie");
                        break;
                    }
                }
            }
            Err(err) => {
                error!("Blad odczytu body uploadu: {}", err);
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

    info!("Zakonczono odbieranie streamu: {}", stream_id);
    StatusCode::OK.into_response()
}

async fn handle_download(
    Path(stream_id): Path<String>,
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    if !is_authorized(&headers, &state) {
        warn!("Odrzucono download streamu bez poprawnej autoryzacji");
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
