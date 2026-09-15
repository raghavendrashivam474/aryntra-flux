use super::chunker::Chunker;
use super::collection::TransferPlan;
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

    /// Send a single file over an established session.
    pub async fn send_file(session: &mut Session, file_path: &Path) -> Result<()> {
        Self::send_file_internal(session, file_path, None).await
    }

    /// Internal implementation of single-file sending supporting optional relative paths.
    async fn send_file_internal(
        session: &mut Session,
        file_path: &Path,
        relative_path: Option<String>,
    ) -> Result<()> {
        let file_name = file_path
            .file_name()
            .ok_or_else(|| TransferError::InvalidFilename("no filename".to_string()))?
            .to_string_lossy()
            .to_string();

        let file_size = tokio::fs::metadata(file_path).await?.len();
        let sha256 = Self::hash_file(file_path).await?;
        let mut metadata = TransferMetadata::new(file_name, file_size, sha256);

        if let Some(rel) = relative_path {
            metadata = metadata.with_relative_path(rel);
        }

        println!("Preparing transfer...");
        if let Some(ref rel) = metadata.relative_path {
            println!("  Path:   {}", rel);
        } else {
            println!("  File:   {}", metadata.file_name);
        }
        println!("  Size:   {} bytes", metadata.file_size);
        println!("  Chunks: {}", metadata.total_chunks);
        println!("  SHA256: {}", hex_encode(&metadata.sha256));

        // 1. Send request
        session
            .send_message(&FluxMessage::TransferRequest {
                metadata: metadata.clone(),
            })
            .await?;

        // 2. Wait for accept/reject/resume
        let response = session.recv_message().await?;
        let start_chunk = match response {
            FluxMessage::TransferAccept { transfer_id } => {
                if transfer_id != metadata.transfer_id {
                    return Err(TransferError::UnexpectedMessage(
                        "Transfer ID mismatch in accept".to_string(),
                    ));
                }
                0
            }
            FluxMessage::TransferResume {
                transfer_id,
                resume_from_chunk,
            } => {
                if transfer_id != metadata.transfer_id {
                    return Err(TransferError::UnexpectedMessage(
                        "Transfer ID mismatch in resume".to_string(),
                    ));
                }
                println!(
                    "  Resuming from chunk {}/{}",
                    resume_from_chunk, metadata.total_chunks
                );
                resume_from_chunk
            }
            FluxMessage::TransferReject { reason, .. } => {
                return Err(TransferError::Rejected(reason));
            }
            other => {
                return Err(TransferError::UnexpectedMessage(format!(
                    "Expected TransferAccept/TransferResume, got {:?}",
                    other
                )));
            }
        };

        // 3. Send chunks (skip already-received if resuming)
        let mut chunker = Chunker::new(file_path, metadata.chunk_size).await?;
        if start_chunk > 0 {
            chunker.seek_to_chunk(start_chunk).await?;
        }

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

    /// Orchestrate sending a complete collection sequentially over a single session.
    pub async fn send_collection(session: &mut Session, plan: &TransferPlan) -> Result<()> {
        let total = plan.items.len();
        println!("Starting transfer of collection ({} items)...", total);

        for (idx, item) in plan.items.iter().enumerate() {
            println!("\n[{}/{}] Sending file...", idx + 1, total);
            let relative_str = item.relative_path.to_string_lossy().to_string();

            if let Err(e) =
                Self::send_file_internal(session, &item.source_path, Some(relative_str)).await
            {
                eprintln!("\nError sending item {}: {}", item.source_path.display(), e);
                return Err(e);
            }
        }

        // Send collection completion signal over the session
        println!("\nCollection transfer complete. Sending termination handshake...");
        session.send_message(&FluxMessage::Goodbye).await?;
        Ok(())
    }

    /// Handle an incoming transfer. The caller has already received the
    /// TransferRequest and passes the extracted metadata here.
    pub async fn receive_transfer(
        session: &mut Session,
        metadata: TransferMetadata,
        output_dir: &Path,
    ) -> Result<()> {
        println!("\n  Incoming transfer:");
        if let Some(ref rel) = metadata.relative_path {
            println!("    Path:   {}", rel);
        } else {
            println!("    File:   {}", metadata.file_name);
        }
        println!("    Size:   {} bytes", metadata.file_size);
        println!("    Chunks: {}", metadata.total_chunks);

        // 1. Check for resumable partial state, then accept or resume
        let (mut receiver, start_chunk) =
            match FileReceiver::try_resume(metadata.clone(), output_dir).await? {
                Some(r) => {
                    let from = r.resume_from_chunk();
                    println!("    Resuming from chunk {}/{}", from, metadata.total_chunks);
                    session
                        .send_message(&FluxMessage::TransferResume {
                            transfer_id: metadata.transfer_id,
                            resume_from_chunk: from,
                        })
                        .await?;
                    (r, from)
                }
                None => {
                    session
                        .send_message(&FluxMessage::TransferAccept {
                            transfer_id: metadata.transfer_id,
                        })
                        .await?;
                    let r = FileReceiver::new(metadata.clone(), output_dir).await?;
                    (r, 0)
                }
            };

        // 2. Receive remaining chunks
        let remaining = metadata.total_chunks - start_chunk;
        for _ in 0..remaining {
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

    /// Run the receiver-side loop over an established session, accepting
    /// consecutive file transfers until a Goodbye message is received or the session closes.
    pub async fn receive_collection(session: &mut Session, output_dir: &Path) -> Result<()> {
        println!("Ready to receive collection...");
        loop {
            let msg = match session.recv_message().await {
                Ok(m) => m,
                Err(e) => {
                    // Graceful exit on EOF / connection drops
                    println!("Connection closed or ended: {}", e);
                    break;
                }
            };

            match msg {
                FluxMessage::TransferRequest { metadata } => {
                    Self::receive_transfer(session, metadata, output_dir).await?;
                }
                FluxMessage::Goodbye => {
                    println!("Goodbye received. Collection transfer completed successfully.");
                    break;
                }
                other => {
                    return Err(TransferError::UnexpectedMessage(format!(
                        "Expected TransferRequest or Goodbye during collection, got {:?}",
                        other
                    )));
                }
            }
        }
        Ok(())
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}
