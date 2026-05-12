use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FileMetadata {
    pub path: String, // Relative path from the shared folder root
    pub size: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SharedFolder {
    pub files: Vec<FileMetadata>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ClientInfo {
    pub client_id: String,
    pub files_count: usize,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type")]
pub enum ClientMessage {
    /// The client registers its shared folder after startup
    Register { folder: SharedFolder },
    /// The client requests the list of connected clients
    ListClients,
    /// The client requests the file list for another client or the server
    ListFiles { target_client_id: String },
    /// The client requests a file download from another client
    RequestDownload {
        target_client_id: String,
        file_path: String,
    },
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type")]
pub enum ServerMessage {
    /// Registration confirmation with the assigned client ID
    Registered { client_id: String },
    /// List of connected clients
    ClientsList { clients: Vec<ClientInfo> },
    /// File list for a client or the server
    FileList {
        target_client_id: String,
        files: Vec<FileMetadata>,
    },
    /// Instruction for the source client to stream a file to the given endpoint
    UploadInstruction {
        file_path: String,
        stream_id: String,
        stream_token: String,
    },
    /// Information for the downloading client about where to fetch the file
    DownloadReady {
        stream_id: String,
        file_name: String,
        stream_token: String,
    },
    /// Error
    Error { message: String },
}
