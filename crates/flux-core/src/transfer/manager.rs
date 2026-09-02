use super::chunker::Chunker;
use super::error::{Result, TransferError};
use super::metadata::TransferMetadata;
use super::receiver::FileReceiver;
use crate::protocol::FluxMessage;
use crate::session::Session;
use sha2::{Digest, Sha256};
use std::path::Path;
use tokio::io::AsyncReadExt;

pub struct TransferManager;

impl TransferManager {
    async fn hash_file(path: &Path) -> Result<[u8; 32]> {
        let mut file = tokio::fs::File::open(path).await?;
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

    /// Send a file through an established session.
    pub async fn send_file(session: &mut Session, file_path: &Path) -> Result<()> {
        let file_name = file_path
            .file_name()
            .ok_or_else(|| TransferError::InvalidFilename("no filename".to_string()))?
            .to_string_lossy()
            .to_string();

        let file_size = tokio::fs::metadata(file_path).await?.len();
        let sha256 = Self::hash_file(file_path).await?;
        let metadata = TransferMetadata::new(file_name, file_size, sha256);

        println!("Preparing transfer...");
        println!("  File:   {}", metadata.file_name);
        println!("  Size:   {} bytes", metadata.file_size);
        println!("  Chunks: {}", metadata.total_chunks);
        println!("  SHA256: {}", hex_encode(&metadata.sha256));

        // 1. Send request
        session
            .send_message(&FluxMessage::TransferRequest {
                metadata: metadata.clone(),
            })
            .await?;

        // 2. Wait for accept/reject
        let response = session.recv_message().await?;
        match response {
            FluxMessage::TransferAccept { transfer_id } => {
                if transfer_id != metadata.transfer_id {
                    return Err(TransferError::UnexpectedMessage(
                        "Transfer ID mismatch in accept".to_string(),
                    ));
                }
            }
            FluxMessage::TransferReject { reason, .. } => {
                return Err(TransferError::Rejected(reason));
            }
            other => {
                return Err(TransferError::UnexpectedMessage(format!(
                    "Expected TransferAccept, got {:?}",
                    other
                )));
            }
        }

        // 3. Send chunks
        let mut chunker = Chunker::new(file_path, metadata.chunk_size).await?;
        while let Some((index, data)) = chunker.next_chunk().await? {
            session
                .send_message(&FluxMessage::TransferChunk {
                    transfer_id: metadata.transfer_id,
                    index,
                    data,
                })
                .await?;

            let (cur, tot) = chunker.progress();
            print!(
                "\r  Sending: chunk {}/{}  ({} bytes)",
                cur,
                tot,
                chunker.bytes_read()
            );
        }
        println!();

        // 4. Signal complete
        session
            .send_message(&FluxMessage::TransferComplete {
                transfer_id: metadata.transfer_id,
            })
            .await?;

        // 5. Wait for result
        let result = session.recv_message().await?;
        match result {
            FluxMessage::TransferResult {
                success, message, ..
            } => {
                if success {
                    println!("  Transfer completed: {}", message);
                    Ok(())
                } else {
                    Err(TransferError::UnexpectedMessage(format!(
                        "Transfer failed: {}",
                        message
                    )))
                }
            }
            other => Err(TransferError::UnexpectedMessage(format!(
                "Expected TransferResult, got {:?}",
                other
            ))),
        }
    }

    /// Handle an incoming transfer. The caller has already received the
    /// TransferRequest and passes the extracted metadata here.
    pub async fn receive_transfer(
        session: &mut Session,
        metadata: TransferMetadata,
        output_dir: &Path,
    ) -> Result<()> {
        println!("\n  Incoming transfer:");
        println!("    File:   {}", metadata.file_name);
        println!("    Size:   {} bytes", metadata.file_size);
        println!("    Chunks: {}", metadata.total_chunks);

        // 1. Accept
        session
            .send_message(&FluxMessage::TransferAccept {
                transfer_id: metadata.transfer_id,
            })
            .await?;

        // 2. Receive chunks
        let mut receiver = FileReceiver::new(metadata.clone(), output_dir).await?;

        for _ in 0..metadata.total_chunks {
            let msg = session.recv_message().await?;
            match msg {
                FluxMessage::TransferChunk {
                    transfer_id,
                    index,
                    data,
                } => {
                    if transfer_id != metadata.transfer_id {
                        return Err(TransferError::UnexpectedMessage(
                            "Transfer ID mismatch in chunk".to_string(),
                        ));
                    }
                    receiver.write_chunk(index, &data).await?;
                    let (cur, tot) = receiver.progress();
                    print!("\r    Receiving: chunk {}/{}", cur, tot);
                }
                other => {
                    return Err(TransferError::UnexpectedMessage(format!(
                        "Expected TransferChunk, got {:?}",
                        other
                    )));
                }
            }
        }
        println!();

        // 3. Wait for TransferComplete
        let msg = session.recv_message().await?;
        match msg {
            FluxMessage::TransferComplete { transfer_id } => {
                if transfer_id != metadata.transfer_id {
                    return Err(TransferError::UnexpectedMessage(
                        "Transfer ID mismatch in complete".to_string(),
                    ));
                }
            }
            other => {
                return Err(TransferError::UnexpectedMessage(format!(
                    "Expected TransferComplete, got {:?}",
                    other
                )));
            }
        }

        // 4. Finalize and verify
        print!("    Verifying integrity... ");
        match receiver.finalize().await {
            Ok(path) => {
                println!("OK");
                println!("    Saved: {}", path.display());
                session
                    .send_message(&FluxMessage::TransferResult {
                        transfer_id: metadata.transfer_id,
                        success: true,
                        message: "File received and verified".to_string(),
                    })
                    .await?;
                Ok(())
            }
            Err(e) => {
                println!("FAILED: {}", e);
                let _ = session
                    .send_message(&FluxMessage::TransferResult {
                        transfer_id: metadata.transfer_id,
                        success: false,
                        message: e.to_string(),
                    })
                    .await;
                Err(e)
            }
        }
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}
