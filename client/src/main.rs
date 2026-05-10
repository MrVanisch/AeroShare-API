use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use std::{env, fs, path::PathBuf, sync::Arc};
use tokio::fs::File;
use tokio_util::io::ReaderStream;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{info, warn, error};

use shared::{ClientMessage, FileMetadata, ServerMessage, SharedFolder};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    // Klient może pobrać token z pliku lub zmiennej środowiskowej
    let token = env::var("SERVER_TOKEN").unwrap_or_else(|_| {
        fs::read_to_string("client_token.txt")
            .unwrap_or_else(|_| "test_token".to_string())
            .trim()
            .to_string()
    });

    let shared_dir = env::var("SHARED_DIR").unwrap_or_else(|_| "./shared_files".to_string());
    fs::create_dir_all(&shared_dir)?;
    info!("Folder udostępniony klienta: {}", shared_dir);

    // Indeksowanie plików
    let mut files = Vec::new();
    for entry in walkdir::WalkDir::new(&shared_dir).into_iter().filter_map(|e| e.ok()) {
        if entry.file_type().is_file() {
            let metadata = entry.metadata()?;
            let path = entry.path().strip_prefix(&shared_dir)?.to_string_lossy().to_string();
            // Zabezpieczenie przed path traversal: path jest względne do folderu udostępniania!
            files.push(FileMetadata {
                path: path.replace("\\", "/"),
                size: metadata.len(),
            });
        }
    }
    info!("Zaindeksowano {} plików", files.len());

    let ws_url = format!("ws://127.0.0.1:3000/ws?token={}", token);
    info!("Łączenie do serwera: {}", ws_url);

    let (ws_stream, _) = connect_async(&ws_url).await?;
    info!("Połączono z serwerem WS");

    let (mut write, mut read) = ws_stream.split();

    // Rejestracja
    let reg_msg = ClientMessage::Register {
        folder: SharedFolder { files },
    };
    write.send(Message::Text(serde_json::to_string(&reg_msg)?)).await?;

    let http_client = Client::new();
    let shared_dir = Arc::new(shared_dir);

    while let Some(msg) = read.next().await {
        if let Ok(Message::Text(text)) = msg {
            if let Ok(server_msg) = serde_json::from_str::<ServerMessage>(&text) {
                match server_msg {
                    ServerMessage::Registered { client_id } => {
                        info!("Zarejestrowano poprawnie. Moje ID: {}", client_id);
                    }
                    ServerMessage::UploadInstruction { file_path, stream_id } => {
                        info!("Serwer żąda wysłania pliku: {}", file_path);
                        
                        // ZABEZPIECZENIE: Path Traversal
                        if file_path.contains("..") || file_path.starts_with('/') {
                            warn!("Niedozwolona ścieżka pliku: {}", file_path);
                            continue;
                        }

                        let mut full_path = PathBuf::from(shared_dir.as_str());
                        full_path.push(&file_path);

                        // EKSTREMALNE ZABEZPIECZENIE (Canonicalize):
                        // Rozwiązuje symlinki, usuwa "..", normalizuje ukośniki i wielkość liter (szczególnie na Windowsie)
                        let shared_dir_canon = std::fs::canonicalize(shared_dir.as_str()).unwrap_or_default();
                        let is_safe = match std::fs::canonicalize(&full_path) {
                            Ok(canon_path) => canon_path.starts_with(&shared_dir_canon),
                            Err(_) => false, // Plik nie istnieje lub ścieżka uszkodzona
                        };

                        if !is_safe {
                            warn!("Wykryto i ZABLOKOWANO zaawansowaną próbę ucieczki z katalogu (Path Traversal)!");
                            continue;
                        }

                        let http_client_clone = http_client.clone();
                        tokio::spawn(async move {
                            match File::open(&full_path).await {
                                Ok(file) => {
                                    let stream = ReaderStream::new(file);
                                    let upload_url = format!("http://127.0.0.1:3000/stream/{}", stream_id);
                                    info!("Strumieniowanie pliku do: {}", upload_url);
                                    
                                    let res = http_client_clone.post(&upload_url)
                                        .body(reqwest::Body::wrap_stream(stream))
                                        .send()
                                        .await;
                                        
                                    match res {
                                        Ok(r) if r.status().is_success() => info!("Wysyłanie pliku zakończone sukcesem."),
                                        Ok(r) => error!("Błąd HTTP podczas wysyłania: {}", r.status()),
                                        Err(e) => error!("Błąd sieci: {}", e),
                                    }
                                }
                                Err(e) => {
                                    error!("Nie można otworzyć pliku do wysłania: {}", e);
                                }
                            }
                        });
                    }
                    ServerMessage::DownloadReady { stream_id } => {
                        info!("Plik gotowy do pobrania ze streamu: {}", stream_id);
                    }
                    ServerMessage::Error { message } => {
                        error!("Błąd serwera: {}", message);
                    }
                }
            }
        }
    }

    Ok(())
}
