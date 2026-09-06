use super::error::{Result, TransferError};
use super::metadata::{PartialTransferState, TransferMetadata};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub struct FileReceiver {
    temp_path: PathBuf,
    meta_path: PathBuf,
    final_path: PathBuf,
    file: Option<File>,
    metadata: TransferMetadata,
    bytes_received: u64,
    chunks_received: u32,
    is_resumed: bool,
}

impl FileReceiver {
    /// Create a new receiver for a fresh transfer.
    pub async fn new(metadata: TransferMetadata, output_dir: &Path) -> Result<Self> {
        let safe_name = sanitize_filename(&metadata.file_name)?;

        fs::create_dir_all(output_dir).await?;

        let final_path = output_dir.join(&safe_name);
        let temp_path = output_dir.join(format!("{}.part", safe_name));
        let meta_path = output_dir.join(format!("{}.part.meta", safe_name));

        // Clean up any stale partial state
        if temp_path.exists() {
            fs::remove_file(&temp_path).await?;
        }
        if meta_path.exists() {
            fs::remove_file(&meta_path).await?;
        }

        let file = File::create(&temp_path).await?;

        Ok(Self {
            temp_path,
            meta_path,
            final_path,
            file: Some(file),
            metadata,
            bytes_received: 0,
            chunks_received: 0,
            is_resumed: false,
        })
    }

    /// Attempt to create a receiver that resumes from existing partial state.
    /// Returns `None` if no valid partial state exists for this transfer.
    pub async fn try_resume(metadata: TransferMetadata, output_dir: &Path) -> Result<Option<Self>> {
        let safe_name = match sanitize_filename(&metadata.file_name) {
            Ok(n) => n,
            Err(_) => return Ok(None),
        };

        let temp_path = output_dir.join(format!("{}.part", safe_name));
        let meta_path = output_dir.join(format!("{}.part.meta", safe_name));

        // Both files must exist
        if !temp_path.exists() || !meta_path.exists() {
            return Ok(None);
        }

        // Load and validate partial state
        let state_bytes = match fs::read(&meta_path).await {
            Ok(b) => b,
            Err(_) => return Ok(None),
        };

        let state: PartialTransferState = match bincode::deserialize(&state_bytes) {
            Ok(s) => s,
            Err(_) => {
                // Corrupt meta — clean up and fall back to fresh
                let _ = fs::remove_file(&temp_path).await;
                let _ = fs::remove_file(&meta_path).await;
                return Ok(None);
            }
        };

        // Verify the partial state matches the incoming transfer
        if !state.matches(&metadata) {
            let _ = fs::remove_file(&temp_path).await;
            let _ = fs::remove_file(&meta_path).await;
            return Ok(None);
        }

        // Verify the .part file size matches what the meta claims
        let actual_size = match fs::metadata(&temp_path).await {
            Ok(m) => m.len(),
            Err(_) => return Ok(None),
        };

        if actual_size != state.bytes_received {
            let _ = fs::remove_file(&temp_path).await;
            let _ = fs::remove_file(&meta_path).await;
            return Ok(None);
        }

        // Nothing to resume if already complete
        if state.chunks_received >= metadata.total_chunks {
            let _ = fs::remove_file(&temp_path).await;
            let _ = fs::remove_file(&meta_path).await;
            return Ok(None);
        }

        // Open the .part file for appending
        let file = OpenOptions::new().append(true).open(&temp_path).await?;

        Ok(Some(Self {
            temp_path,
            meta_path,
            final_path: output_dir.join(&safe_name),
            file: Some(file),
            metadata,
            bytes_received: state.bytes_received,
            chunks_received: state.chunks_received,
            is_resumed: true,
        }))
    }

    /// The chunk index the sender should start from.
    pub fn resume_from_chunk(&self) -> u32 {
        self.chunks_received
    }

    /// Whether this receiver is resuming a previous transfer.
    pub fn is_resumed(&self) -> bool {
        self.is_resumed
    }

    pub async fn write_chunk(&mut self, index: u32, data: &[u8]) -> Result<()> {
        if index != self.chunks_received {
            return Err(TransferError::InvalidChunkIndex {
                expected: self.chunks_received,
                actual: index,
            });
        }

        let file = self.file.as_mut().ok_or_else(|| {
            TransferError::UnexpectedMessage("File already finalized".to_string())
        })?;

        // Write and flush the chunk data FIRST
        file.write_all(data).await?;
        file.flush().await?;

        self.bytes_received += data.len() as u64;
        self.chunks_received += 1;

        // Persist resume state AFTER successful write
        self.save_partial_state().await?;

        Ok(())
    }

    /// Persist the current partial transfer state to disk atomically.
    async fn save_partial_state(&self) -> Result<()> {
        let state = PartialTransferState {
            file_name: self.metadata.file_name.clone(),
            file_size: self.metadata.file_size,
            chunk_size: self.metadata.chunk_size,
            total_chunks: self.metadata.total_chunks,
            sha256: self.metadata.sha256,
            chunks_received: self.chunks_received,
            bytes_received: self.bytes_received,
        };

        let encoded = bincode::serialize(&state).map_err(|e| {
            TransferError::UnexpectedMessage(format!("Failed to serialize state: {}", e))
        })?;

        // Write to temp file first, then rename for atomicity
        let tmp_meta = self.meta_path.with_extension("part.meta.tmp");
        fs::write(&tmp_meta, &encoded).await?;
        fs::rename(&tmp_meta, &self.meta_path).await?;

        Ok(())
    }

    pub async fn finalize(mut self) -> Result<PathBuf> {
        if let Some(mut file) = self.file.take() {
            file.flush().await?;
            file.sync_all().await?;
        }

        // Verify size
        if self.bytes_received != self.metadata.file_size {
            let _ = fs::remove_file(&self.temp_path).await;
            let _ = fs::remove_file(&self.meta_path).await;
            return Err(TransferError::SizeMismatch {
                expected: self.metadata.file_size,
                actual: self.bytes_received,
            });
        }

        // Verify hash by reading the complete file from disk.
        // This works correctly for both fresh and resumed transfers.
        let computed = hash_file(&self.temp_path).await?;
        if computed != self.metadata.sha256 {
            let _ = fs::remove_file(&self.temp_path).await;
            let _ = fs::remove_file(&self.meta_path).await;
            return Err(TransferError::IntegrityMismatch {
                expected: hex_encode(&self.metadata.sha256),
                actual: hex_encode(&computed),
            });
        }

        // Rename .part -> final
        fs::rename(&self.temp_path, &self.final_path).await?;

        // Clean up meta file
        let _ = fs::remove_file(&self.meta_path).await;

        Ok(self.final_path)
    }

    pub fn progress(&self) -> (u32, u32) {
        (self.chunks_received, self.metadata.total_chunks)
    }
}

/// Hash a file from disk using SHA-256.
async fn hash_file(path: &Path) -> Result<[u8; 32]> {
    let mut file = File::open(path).await?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hasher.finalize().into())
}

fn sanitize_filename(name: &str) -> Result<String> {
    if name.contains("..") || name.contains('/') || name.contains('\\') {
        return Err(TransferError::InvalidFilename(name.to_string()));
    }
    if name.starts_with('/') || name.starts_with('\\') {
        return Err(TransferError::InvalidFilename(name.to_string()));
    }
    if name.len() >= 2 && name.as_bytes()[1] == b':' {
        return Err(TransferError::InvalidFilename(name.to_string()));
    }

    let trimmed = name.trim();
    if trimmed.is_empty() {
        return Err(TransferError::InvalidFilename("empty filename".to_string()));
    }

    let safe: String = trimmed
        .chars()
        .filter(|c| c.is_alphanumeric() || *c == '.' || *c == '-' || *c == '_')
        .collect();

    if safe.is_empty() {
        return Err(TransferError::InvalidFilename(name.to_string()));
    }

    Ok(safe)
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}
