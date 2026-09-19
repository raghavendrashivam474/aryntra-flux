use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TransferStatus {
    Created,
    Running,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayTransferInfo {
    pub transfer_id: String,
    pub peer_id: String,
    pub status: TransferStatus,
    pub bytes_transferred: u64,
    pub total_bytes: u64,
    pub files_transferred: usize,
    pub total_files: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
}

pub struct ActiveTransfer {
    pub info: GatewayTransferInfo,
    pub task_handle: Option<JoinHandle<()>>,
}

#[derive(Clone, Default)]
pub struct GatewayTransferTracker {
    transfers: Arc<RwLock<HashMap<String, ActiveTransfer>>>,
}

impl GatewayTransferTracker {
    pub fn new() -> Self {
        Self {
            transfers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn register(
        &self,
        transfer_id: String,
        peer_id: String,
        total_bytes: u64,
        total_files: usize,
        task_handle: Option<JoinHandle<()>>,
    ) {
        let mut guard = self.transfers.write().await;
        guard.insert(
            transfer_id.clone(),
            ActiveTransfer {
                info: GatewayTransferInfo {
                    transfer_id,
                    peer_id,
                    status: TransferStatus::Running,
                    bytes_transferred: 0,
                    total_bytes,
                    files_transferred: 0,
                    total_files,
                    error_message: None,
                },
                task_handle,
            },
        );
    }

    pub async fn get(&self, transfer_id: &str) -> Option<GatewayTransferInfo> {
        let guard = self.transfers.read().await;
        guard.get(transfer_id).map(|t| t.info.clone())
    }

    pub async fn update_progress(&self, transfer_id: &str, bytes: u64, files: usize) {
        let mut guard = self.transfers.write().await;
        if let Some(transfer) = guard.get_mut(transfer_id) {
            transfer.info.bytes_transferred = bytes;
            transfer.info.files_transferred = files;
        }
    }

    pub async fn mark_completed(&self, transfer_id: &str) {
        let mut guard = self.transfers.write().await;
        if let Some(transfer) = guard.get_mut(transfer_id) {
            transfer.info.status = TransferStatus::Completed;
            transfer.info.bytes_transferred = transfer.info.total_bytes;
            transfer.info.files_transferred = transfer.info.total_files;
            transfer.task_handle = None;
        }
    }

    pub async fn mark_failed(&self, transfer_id: &str, error: String) {
        let mut guard = self.transfers.write().await;
        if let Some(transfer) = guard.get_mut(transfer_id) {
            transfer.info.status = TransferStatus::Failed;
            transfer.info.error_message = Some(error);
            transfer.task_handle = None;
        }
    }

    pub async fn cancel(&self, transfer_id: &str) -> bool {
        let mut guard = self.transfers.write().await;
        if let Some(transfer) = guard.get_mut(transfer_id) {
            if transfer.info.status == TransferStatus::Running
                || transfer.info.status == TransferStatus::Created
            {
                if let Some(handle) = transfer.task_handle.take() {
                    handle.abort();
                }
                transfer.info.status = TransferStatus::Cancelled;
                return true;
            }
        }
        false
    }
}
