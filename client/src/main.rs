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
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{error, info, warn};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let token = env::var("SERVER_TOKEN").unwrap_or_else(|_| {
        fs::read_to_string("client_token.txt")
            .expect("Brak SERVER_TOKEN i nie mozna odczytac client_token.txt")
            .trim()
            .to_string()
    });

    if token.is_empty() {
        anyhow::bail!("Token nie moze byc pusty");
    }

    let shared_dir = env::var("SHARED_DIR").unwrap_or_else(|_| "./shared_files".to_string());
    fs::create_dir_all(&shared_dir)?;
    info!("Folder udostepniony klienta: {}", shared_dir);

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
    info!("Zaindeksowano {} plikow", files.len());

    let server_url = env::var("SERVER_URL").unwrap_or_else(|_| "127.0.0.1:5000".to_string());
    let ws_url = format!("ws://{}/ws?token={}", server_url, token);
    info!("Laczenie do serwera WS: {}", server_url);

    let (ws_stream, _) = connect_async(&ws_url).await?;
    info!("Polaczono z serwerem WS");

    let (mut write, mut read) = ws_stream.split();

    let reg_msg = ClientMessage::Register {
        folder: SharedFolder { files },
    };
    write
        .send(Message::Text(serde_json::to_string(&reg_msg)?))
        .await?;

    info!("Komendy: download <client_id> <file_path>, help");

    tokio::spawn(async move {
        let stdin = BufReader::new(tokio::io::stdin());
        let mut lines = stdin.lines();

        while let Ok(Some(line)) = lines.next_line().await {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }

            if line.eq_ignore_ascii_case("help") {
                info!("Uzycie: download <client_id> <file_path>");
                continue;
            }

            let mut parts = line.splitn(3, ' ');
            let command = parts.next().unwrap_or_default();
            let target_client_id = parts.next().unwrap_or_default().trim();
            let file_path = parts.next().unwrap_or_default().trim();

            if command != "download" || target_client_id.is_empty() || file_path.is_empty() {
                warn!("Nieznana komenda. Uzycie: download <client_id> <file_path>");
                continue;
            }

            let msg = ClientMessage::RequestDownload {
                target_client_id: target_client_id.to_string(),
                file_path: file_path.replace('\\', "/"),
            };

            match serde_json::to_string(&msg) {
                Ok(json) => {
                    if write.send(Message::Text(json)).await.is_err() {
                        error!("Nie mozna wyslac komendy pobierania do serwera");
                        break;
                    }
                }
                Err(e) => error!("Nie mozna przygotowac komendy pobierania: {}", e),
            }
        }
    });

    let http_client = Client::new();
    let shared_dir = Arc::new(shared_dir);
    let server_url = Arc::new(server_url);
    let auth_token = Arc::new(token);

    while let Some(msg) = read.next().await {
        if let Ok(Message::Text(text)) = msg {
            if let Ok(server_msg) = serde_json::from_str::<ServerMessage>(&text) {
                match server_msg {
                    ServerMessage::Registered { client_id } => {
                        info!("Zarejestrowano poprawnie. Moje ID: {}", client_id);
                    }
                    ServerMessage::UploadInstruction {
                        file_path,
                        stream_id,
                    } => {
                        info!("Serwer zada wyslania pliku: {}", file_path);

                        if !is_safe_relative_path(&file_path) {
                            warn!("Niedozwolona struktura sciezki pliku: {}", file_path);
                            continue;
                        }

                        let mut full_path = PathBuf::from(shared_dir.as_str());
                        full_path.push(&file_path);

                        let shared_dir_canon = match std::fs::canonicalize(shared_dir.as_str()) {
                            Ok(path) => path,
                            Err(e) => {
                                error!("Nie mozna zweryfikowac folderu udostepniania: {}", e);
                                continue;
                            }
                        };
                        let is_safe = match std::fs::canonicalize(&full_path) {
                            Ok(canon_path) => canon_path.starts_with(&shared_dir_canon),
                            Err(_) => false,
                        };

                        if !is_safe {
                            warn!("Zablokowano probe odczytu spoza folderu udostepniania");
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
                                    info!("Strumieniowanie pliku do serwera relay");

                                    let res = http_client_clone
                                        .post(&upload_url)
                                        .bearer_auth(auth_token_clone.as_str())
                                        .body(reqwest::Body::wrap_stream(reader_stream))
                                        .send()
                                        .await;

                                    match res {
                                        Ok(r) if r.status().is_success() => {
                                            info!("Wysylanie pliku zakonczone sukcesem")
                                        }
                                        Ok(r) => {
                                            error!("Blad HTTP podczas wysylania: {}", r.status())
                                        }
                                        Err(e) => error!("Blad sieci: {}", e),
                                    }
                                }
                                Err(e) => {
                                    error!("Nie mozna otworzyc pliku do wyslania: {}", e);
                                }
                            }
                        });
                    }
                    ServerMessage::DownloadReady {
                        stream_id,
                        file_name,
                    } => {
                        info!("Pobieranie pliku: {}", file_name);
                        let http_client_clone = http_client.clone();
                        let server_url_clone = server_url.clone();
                        let auth_token_clone = auth_token.clone();

                        tokio::spawn(async move {
                            let download_url =
                                format!("http://{}/stream/{}", server_url_clone, stream_id);
                            match http_client_clone
                                .get(&download_url)
                                .bearer_auth(auth_token_clone.as_str())
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
                                        error!("Serwer zwrocil pusta nazwe pliku");
                                        return;
                                    }

                                    let file_path = downloads_dir.join(safe_file_name);

                                    if let Ok(file) = tokio::fs::File::create(&file_path).await {
                                        use tokio::io::AsyncWriteExt;

                                        let mut buf_writer =
                                            tokio::io::BufWriter::with_capacity(256 * 1024, file);

                                        while let Ok(Some(chunk)) = response.chunk().await {
                                            if buf_writer.write_all(&chunk).await.is_err() {
                                                error!("Blad zapisu pliku na dysk");
                                                break;
                                            }
                                        }
                                        let _ = buf_writer.flush().await;
                                        info!("Pomyslnie zapisano plik: {:?}", file_path);
                                    } else {
                                        error!("Nie mozna utworzyc pliku: {:?}", file_path);
                                    }
                                }
                                Ok(response) => {
                                    error!("Blad serwera podczas pobierania: {}", response.status())
                                }
                                Err(e) => error!("Blad sieci: {}", e),
                            }
                        });
                    }
                    ServerMessage::Error { message } => {
                        error!("Blad serwera: {}", message);
                    }
                }
            }
        }
    }

    Ok(())
}

fn is_safe_relative_path(file_path: &str) -> bool {
    !file_path.is_empty()
        && Path::new(file_path)
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
}
