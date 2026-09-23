use super::chunker::Chunker;
use super::collection::TransferPlan;
use super::continuity::TransferContinuation;
use super::control::TransferCancellation;
use super::error::{Result, TransferError};
use super::metadata::TransferMetadata;
use super::progress::TransferProgress;
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
        let cancel = TransferCancellation::new();
        Self::send_file_with_cancel(session, file_path, &cancel).await
    }

    /// Send a single file over an established session with a cooperative cancellation token.
    pub async fn send_file_with_cancel(
        session: &mut Session,
        file_path: &Path,
        cancel: &TransferCancellation,
    ) -> Result<()> {
        let progress = TransferProgress::new();
        Self::send_file_internal(session, file_path, None, cancel, &progress).await
    }

    /// Send a single file over an established session with cancellation and progress reporting.
    pub async fn send_file_with_cancel_and_progress(
        session: &mut Session,
        file_path: &Path,
        cancel: &TransferCancellation,
        progress: &TransferProgress,
    ) -> Result<()> {
        Self::send_file_internal(session, file_path, None, cancel, progress).await
    }

    /// Internal implementation of single-file sending supporting optional relative paths, cancellation, and progress.
    async fn send_file_internal(
        session: &mut Session,
        file_path: &Path,
        relative_path: Option<String>,
        cancel: &TransferCancellation,
        progress: &TransferProgress,
    ) -> Result<()> {
        if cancel.is_cancelled() {
            return Err(TransferError::Cancelled);
        }

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

        // 2. Wait for accept/reject/resume/cancel
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
                // Credit already verified bytes to progress on resume
                let resumed_bytes = (resume_from_chunk as u64) * (metadata.chunk_size as u64);
                let actual_resumed_bytes = resumed_bytes.min(metadata.file_size);
                progress.add_bytes(actual_resumed_bytes);
                resume_from_chunk
            }
            FluxMessage::TransferReject { reason, .. } => {
                return Err(TransferError::Rejected(reason));
            }
            FluxMessage::TransferCancel { transfer_id } => {
                if transfer_id == metadata.transfer_id {
                    println!("  Transfer cancelled by remote peer");
                    return Err(TransferError::Cancelled);
                }
                return Err(TransferError::UnexpectedMessage(
                    "Transfer ID mismatch in cancel".to_string(),
                ));
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
            // Cooperative cancellation check before transmitting chunk
            if cancel.is_cancelled() {
                println!("\n  Transfer cancelled locally. Sending TransferCancel...");
                let _ = session
                    .send_message(&FluxMessage::TransferCancel {
                        transfer_id: metadata.transfer_id,
                    })
                    .await;
                return Err(TransferError::Cancelled);
            }

            let chunk_len = data.len() as u64;

            session
                .send_message(&FluxMessage::TransferChunk {
                    transfer_id: metadata.transfer_id,
                    index,
                    data,
                })
                .await?;

            // Record progress immediately upon successful transmission
            progress.add_bytes(chunk_len);

            let (cur, tot) = chunker.progress();
            print!(
                "\r  Sending: chunk {}/{}  ({} bytes)",
                cur,
                tot,
                chunker.bytes_read()
            );
        }
        println!();

        // Check cancellation before sending completion
        if cancel.is_cancelled() {
            println!("  Transfer cancelled locally before completion.");
            let _ = session
                .send_message(&FluxMessage::TransferCancel {
                    transfer_id: metadata.transfer_id,
                })
                .await;
            return Err(TransferError::Cancelled);
        }

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
            FluxMessage::TransferCancel { transfer_id } => {
                if transfer_id == metadata.transfer_id {
                    println!("  Transfer cancelled by remote peer");
                    Err(TransferError::Cancelled)
                } else {
                    Err(TransferError::UnexpectedMessage(
                        "Transfer ID mismatch in cancel".to_string(),
                    ))
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
        let cancel = TransferCancellation::new();
        Self::send_collection_with_cancel(session, plan, &cancel).await
    }

    /// Orchestrate sending a complete collection sequentially with a cancellation token.
    pub async fn send_collection_with_cancel(
        session: &mut Session,
        plan: &TransferPlan,
        cancel: &TransferCancellation,
    ) -> Result<()> {
        let progress = TransferProgress::new();
        Self::send_collection_with_cancel_and_progress(session, plan, cancel, &progress).await
    }

    /// Orchestrate sending a complete collection sequentially with cancellation and progress reporting.
    pub async fn send_collection_with_cancel_and_progress(
        session: &mut Session,
        plan: &TransferPlan,
        cancel: &TransferCancellation,
        progress: &TransferProgress,
    ) -> Result<()> {
        let total = plan.items.len();
        println!("Starting transfer of collection ({} items)...", total);

        for (idx, item) in plan.items.iter().enumerate() {
            if cancel.is_cancelled() {
                println!(
                    "\nCollection transfer cancelled before sending item {}",
                    idx + 1
                );
                return Err(TransferError::Cancelled);
            }

            println!("\n[{}/{}] Sending file...", idx + 1, total);
            let relative_str = item.relative_path.to_string_lossy().to_string();

            if let Err(e) = Self::send_file_internal(
                session,
                &item.source_path,
                Some(relative_str),
                cancel,
                progress,
            )
            .await
            {
                eprintln!("\nError sending item {}: {}", item.source_path.display(), e);
                return Err(e);
            }

            // File completed successfully
            progress.add_file();
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
        let cancel = TransferCancellation::new();
        let progress = TransferProgress::new();
        Self::receive_transfer_with_cancel_and_progress(
            session, metadata, output_dir, &cancel, &progress,
        )
        .await
    }

    /// Handle an incoming transfer with a cancellation token.
    pub async fn receive_transfer_with_cancel(
        session: &mut Session,
        metadata: TransferMetadata,
        output_dir: &Path,
        cancel: &TransferCancellation,
    ) -> Result<()> {
        let progress = TransferProgress::new();
        Self::receive_transfer_with_cancel_and_progress(
            session, metadata, output_dir, cancel, &progress,
        )
        .await
    }

    /// Handle an incoming transfer with cancellation and progress reporting.
    pub async fn receive_transfer_with_cancel_and_progress(
        session: &mut Session,
        metadata: TransferMetadata,
        output_dir: &Path,
        cancel: &TransferCancellation,
        progress: &TransferProgress,
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
                    let bytes_already = (from as u64) * (metadata.chunk_size as u64);
                    let actual_bytes_already = bytes_already.min(metadata.file_size);
                    progress.add_bytes(actual_bytes_already);

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
            if cancel.is_cancelled() {
                println!("\n    Receiver cancelled locally. Preserving partial state.");
                let _ = session
                    .send_message(&FluxMessage::TransferCancel {
                        transfer_id: metadata.transfer_id,
                    })
                    .await;
                return Err(TransferError::Cancelled);
            }

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
                    let chunk_len = data.len() as u64;
                    receiver.write_chunk(index, &data).await?;
                    progress.add_bytes(chunk_len);

                    let (cur, tot) = receiver.progress();
                    print!("\r    Receiving: chunk {}/{}", cur, tot);
                }
                FluxMessage::TransferCancel { transfer_id } => {
                    if transfer_id == metadata.transfer_id {
                        println!(
                            "\n    Received TransferCancel from sender. Preserving partial state."
                        );
                        return Err(TransferError::Cancelled);
                    }
                    return Err(TransferError::UnexpectedMessage(
                        "Transfer ID mismatch in cancel".to_string(),
                    ));
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

        if cancel.is_cancelled() {
            println!(
                "    Receiver cancelled locally before finalization. Preserving partial state."
            );
            let _ = session
                .send_message(&FluxMessage::TransferCancel {
                    transfer_id: metadata.transfer_id,
                })
                .await;
            return Err(TransferError::Cancelled);
        }

        // 3. Wait for TransferComplete or TransferCancel
        let msg = session.recv_message().await?;
        match msg {
            FluxMessage::TransferComplete { transfer_id } => {
                if transfer_id != metadata.transfer_id {
                    return Err(TransferError::UnexpectedMessage(
                        "Transfer ID mismatch in complete".to_string(),
                    ));
                }
            }
            FluxMessage::TransferCancel { transfer_id } => {
                if transfer_id == metadata.transfer_id {
                    println!("    Received TransferCancel from sender before finalization.");
                    return Err(TransferError::Cancelled);
                }
                return Err(TransferError::UnexpectedMessage(
                    "Transfer ID mismatch in cancel".to_string(),
                ));
            }
            other => {
                return Err(TransferError::UnexpectedMessage(format!(
                    "Expected TransferComplete, got {:?}",
                    other
                )));
            }
        }

        if cancel.is_cancelled() {
            println!("    Receiver cancelled locally after receiving TransferComplete.");
            let _ = session
                .send_message(&FluxMessage::TransferCancel {
                    transfer_id: metadata.transfer_id,
                })
                .await;
            return Err(TransferError::Cancelled);
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
        let cancel = TransferCancellation::new();
        Self::receive_collection_with_cancel(session, output_dir, &cancel).await
    }

    /// Run the receiver-side loop over an established session with a cancellation token.
    pub async fn receive_collection_with_cancel(
        session: &mut Session,
        output_dir: &Path,
        cancel: &TransferCancellation,
    ) -> Result<()> {
        let progress = TransferProgress::new();
        Self::receive_collection_with_cancel_and_progress(session, output_dir, cancel, &progress)
            .await
    }

    /// Run the receiver-side loop over an established session with cancellation and progress reporting.
    pub async fn receive_collection_with_cancel_and_progress(
        session: &mut Session,
        output_dir: &Path,
        cancel: &TransferCancellation,
        progress: &TransferProgress,
    ) -> Result<()> {
        println!("Ready to receive collection...");
        loop {
            if cancel.is_cancelled() {
                println!("Receive collection cancelled.");
                return Err(TransferError::Cancelled);
            }

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
                    Self::receive_transfer_with_cancel_and_progress(
                        session, metadata, output_dir, cancel, progress,
                    )
                    .await?;
                    progress.add_file();
                }
                FluxMessage::Goodbye => {
                    println!("Goodbye received. Collection transfer completed successfully.");
                    break;
                }
                FluxMessage::TransferCancel { .. } => {
                    println!("TransferCancel received at collection level.");
                    return Err(TransferError::Cancelled);
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
    /// Continue an interrupted collection transfer on a new session.
    ///
    /// Skips files that were already fully completed and resumes the first
    /// incomplete file using the existing checkpoint/resume mechanism.
    /// The shared `TransferProgress` and `TransferCancellation` are preserved
    /// from the original transfer, ensuring logical continuity.
    pub async fn continue_collection(
        session: &mut Session,
        continuation: &TransferContinuation,
    ) -> Result<()> {
        if continuation.is_complete() {
            println!("Collection already complete, nothing to continue.");
            return Ok(());
        }

        let total = continuation.plan.items.len();
        let start = continuation.completed_files;
        println!(
            "Continuing collection: {}/{} files done, resuming from file {}",
            start,
            total,
            start + 1
        );

        // Calibrate progress tracker to the verified baseline of completed files
        let mut completed_bytes = 0u64;
        for item in &continuation.plan.items[..start] {
            if let Ok(meta) = tokio::fs::metadata(&item.source_path).await {
                completed_bytes += meta.len();
            }
        }
        continuation.progress.set_bytes(completed_bytes);
        continuation.progress.set_files(start);

        for (idx, item) in continuation.plan.items.iter().enumerate().skip(start) {
            if continuation.cancel.is_cancelled() {
                println!(
                    "\nCollection continuation cancelled before item {}",
                    idx + 1
                );
                return Err(TransferError::Cancelled);
            }

            println!("\n[{}/{}] Continuing file...", idx + 1, total);
            let relative_str = item.relative_path.to_string_lossy().to_string();

            if let Err(e) = Self::send_file_internal(
                session,
                &item.source_path,
                Some(relative_str),
                &continuation.cancel,
                &continuation.progress,
            )
            .await
            {
                eprintln!(
                    "\nError continuing item {}: {}",
                    item.source_path.display(),
                    e
                );
                return Err(e);
            }

            continuation.progress.add_file();
        }

        println!("\nCollection continuation complete. Sending termination handshake...");
        session.send_message(&FluxMessage::Goodbye).await?;
        Ok(())
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}
