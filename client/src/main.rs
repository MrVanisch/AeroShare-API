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

    let server_url = env::var("SERVER_URL").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
    let ws_url = format!("ws://{}/ws?token={}", server_url, token);
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
    let server_url = Arc::new(server_url);

    // Rejestracja
    let reg_msg = ClientMessage::Register {
        folder: SharedFolder { files },
    };
    write.send(Message::Text(serde_json::to_string(&reg_msg)?)).await?;

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

                        // 1. ZABEZPIECZENIE STRUKTURALNE (Defense in Depth):
                        // Blokujemy z góry ścieżki, które zawierają ".." (cofanie katalogów), 
                        // zaczynają się od ukośnika (ścieżki absolutne z roota na Linux/Mac) 
                        // lub zawierają odniesienia do dysków Windows (np. C:, D:)
                        if file_path.contains("..") || file_path.starts_with('/') || file_path.starts_with('\\') || file_path.contains(':') {
                            warn!("Niedozwolona struktura ścieżki pliku (próba ataku): {}", file_path);
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
                        let server_url_clone = server_url.clone();
                        tokio::spawn(async move {
                            match File::open(&full_path).await {
                                Ok(file) => {
                                    // OPTYMALIZACJA WYDAJNOŚCIOWA:
                                    // Zamiast standardowych paczek dyskowych ~8KB, wymuszamy wczytywanie dużych porcji danych 256KB z dysku.
                                    // Minimalizuje to narzut systemowy podczas strumieniowania bardzo wielkich plików.
                                    let mut reader_stream = tokio_util::io::ReaderStream::with_capacity(file, 256 * 1024);
                                    let upload_url = format!("http://{}/stream/{}", server_url_clone, stream_id);
                                    info!("Strumieniowanie pliku do: {}", upload_url);
                                    
                                    let res = http_client_clone.post(&upload_url)
                                        .body(reqwest::Body::wrap_stream(reader_stream))
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
                    ServerMessage::DownloadReady { stream_id, file_name } => {
                        info!("Pobieranie pliku: {}", file_name);
                        let http_client_clone = http_client.clone();
                        let server_url_clone = server_url.clone();
                        
                        tokio::spawn(async move {
                            let download_url = format!("http://{}/stream/{}", server_url_clone, stream_id);
                            match http_client_clone.get(&download_url).send().await {
                                Ok(mut response) if response.status().is_success() => {
                                    let downloads_dir = PathBuf::from("./downloads");
                                    let _ = std::fs::create_dir_all(&downloads_dir);
                                    
                                    // Czyszczenie nazwy pliku ze ścieżek dla bezpieczeństwa po stronie zapisującej
                                    let safe_file_name = PathBuf::from(&file_name)
                                        .file_name()
                                        .unwrap_or_default()
                                        .to_string_lossy()
                                        .into_owned();
                                        
                                    let file_path = downloads_dir.join(safe_file_name);
                                    
                                    if let Ok(file) = tokio::fs::File::create(&file_path).await {
                                        use tokio::io::AsyncWriteExt;
                                        
                                        // OPTYMALIZACJA WYDAJNOŚCIOWA: 
                                        // BufWriter łączy małe paczki odbierane z sieci w jeden ogromny blok 256KB, 
                                        // zanim fizycznie każe dyskowi go zapisać. Piekielnie przyśpiesza to dyski HDD i słabsze SSD.
                                        let mut buf_writer = tokio::io::BufWriter::with_capacity(256 * 1024, file);
                                        
                                        while let Ok(Some(chunk)) = response.chunk().await {
                                            if buf_writer.write_all(&chunk).await.is_err() {
                                                error!("Błąd zapisu pliku na dysk!");
                                                break;
                                            }
                                        }
                                        let _ = buf_writer.flush().await; // Pamiętaj o opróżnieniu bufora na końcu!
                                        info!("Pomyślnie zapisano plik: {:?}", file_path);
                                    } else {
                                        error!("Nie można utworzyć pliku: {:?}", file_path);
                                    }
                                }
                                Ok(response) => error!("Błąd serwera podczas pobierania: {}", response.status()),
                                Err(e) => error!("Błąd sieci: {}", e),
                            }
                        });
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
