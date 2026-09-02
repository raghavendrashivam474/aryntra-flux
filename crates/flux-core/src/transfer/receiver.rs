use super::error::{Result, TransferError};
use super::metadata::TransferMetadata;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use tokio::fs::{self, File};
use tokio::io::AsyncWriteExt;

pub struct FileReceiver {
    temp_path: PathBuf,
    final_path: PathBuf,
    file: Option<File>,
    metadata: TransferMetadata,
    bytes_received: u64,
    chunks_received: u32,
    hasher: Sha256,
}

impl FileReceiver {
    pub async fn new(metadata: TransferMetadata, output_dir: &Path) -> Result<Self> {
        let safe_name = sanitize_filename(&metadata.file_name)?;

        fs::create_dir_all(output_dir).await?;

        let final_path = output_dir.join(&safe_name);
        let temp_path = output_dir.join(format!("{}.part", safe_name));

        if temp_path.exists() {
            fs::remove_file(&temp_path).await?;
        }

        let file = File::create(&temp_path).await?;

        Ok(Self {
            temp_path,
            final_path,
            file: Some(file),
            metadata,
            bytes_received: 0,
            chunks_received: 0,
            hasher: Sha256::new(),
        })
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

        file.write_all(data).await?;
        self.hasher.update(data);
        self.bytes_received += data.len() as u64;
        self.chunks_received += 1;

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
            return Err(TransferError::SizeMismatch {
                expected: self.metadata.file_size,
                actual: self.bytes_received,
            });
        }

        // Verify hash
        let computed: [u8; 32] = self.hasher.finalize().into();
        if computed != self.metadata.sha256 {
            let _ = fs::remove_file(&self.temp_path).await;
            return Err(TransferError::IntegrityMismatch {
                expected: hex_encode(&self.metadata.sha256),
                actual: hex_encode(&computed),
            });
        }

        // Rename .part -> final
        fs::rename(&self.temp_path, &self.final_path).await?;

        Ok(self.final_path)
    }

    pub fn progress(&self) -> (u32, u32) {
        (self.chunks_received, self.metadata.total_chunks)
    }
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
