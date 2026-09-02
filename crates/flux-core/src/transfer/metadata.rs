use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub type TransferId = Uuid;

pub const DEFAULT_CHUNK_SIZE: u32 = 64 * 1024; // 64 KiB

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TransferMetadata {
    pub transfer_id: TransferId,
    pub file_name: String,
    pub file_size: u64,
    pub chunk_size: u32,
    pub total_chunks: u32,
    pub sha256: [u8; 32],
}

impl TransferMetadata {
    pub fn new(file_name: String, file_size: u64, sha256: [u8; 32]) -> Self {
        let chunk_size = DEFAULT_CHUNK_SIZE;
        let total_chunks = file_size.div_ceil(chunk_size as u64) as u32;

        Self {
            transfer_id: Uuid::new_v4(),
            file_name,
            file_size,
            chunk_size,
            total_chunks,
            sha256,
        }
    }
}
