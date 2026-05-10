use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FileMetadata {
    pub path: String, // Względna ścieżka od root folderu udostępniania
    pub size: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SharedFolder {
    pub files: Vec<FileMetadata>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type")]
pub enum ClientMessage {
    /// Klient rejestruje folder z plikami po starcie
    Register { folder: SharedFolder },
    /// Klient prosi o pobranie pliku od innego klienta
    RequestDownload { target_client_id: String, file_path: String },
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type")]
pub enum ServerMessage {
    /// Potwierdzenie rejestracji z nadanym ID klienta
    Registered { client_id: String },
    /// Polecenie dla klienta (źródła) aby wysłał plik strumieniem na wskazany endpoint
    UploadInstruction { file_path: String, stream_id: String },
    /// Informacja dla klienta pobierającego skąd ma ściągnąć plik
    DownloadReady { stream_id: String, file_name: String },
    /// Błąd
    Error { message: String },
}
