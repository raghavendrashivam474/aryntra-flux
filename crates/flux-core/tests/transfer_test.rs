use flux_core::identity::PeerId;
use flux_core::protocol::FluxMessage;
use flux_core::session::{Session, SessionBuilder};
use flux_core::transfer::chunker::Chunker;
use flux_core::transfer::error::TransferError;
use flux_core::transfer::metadata::{PartialTransferState, TransferMetadata, DEFAULT_CHUNK_SIZE};
use flux_core::transfer::receiver::FileReceiver;
use flux_core::transfer::TransferManager;
use flux_core::transport::tcp::TcpTransport;
use flux_core::transport::Transport;
use sha2::{Digest, Sha256};
use std::net::SocketAddr;
use std::path::Path;
use tempfile::tempdir;
use tokio::fs;

// --- Helper Functions ---

async fn create_test_file(path: &Path, size: usize) -> Vec<u8> {
    let data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
    fs::write(path, &data).await.unwrap();
    data
}

fn compute_hash(data: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().into()
}

// --- Chunking & Boundary Tests ---

#[tokio::test]
async fn test_chunker_empty_file() {
    let tmp = tempdir().unwrap();
    let file_path = tmp.path().join("empty.bin");
    create_test_file(&file_path, 0).await;

    let mut chunker = Chunker::new(&file_path, 1024).await.unwrap();
    let (cur, tot) = chunker.progress();
    assert_eq!(tot, 0);
    assert_eq!(cur, 0);

    let next = chunker.next_chunk().await.unwrap();
    assert!(next.is_none());
}

#[tokio::test]
async fn test_chunker_exact_chunk_size() {
    let tmp = tempdir().unwrap();
    let file_path = tmp.path().join("exact.bin");
    let chunk_size = 1024;
    create_test_file(&file_path, chunk_size).await;

    let mut chunker = Chunker::new(&file_path, chunk_size as u32).await.unwrap();
    let (_, tot) = chunker.progress();
    assert_eq!(tot, 1);

    let next = chunker.next_chunk().await.unwrap();
    assert!(next.is_some());
    let (idx, data) = next.unwrap();
    assert_eq!(idx, 0);
    assert_eq!(data.len(), chunk_size);

    assert!(chunker.next_chunk().await.unwrap().is_none());
}

#[tokio::test]
async fn test_chunker_exact_plus_one() {
    let tmp = tempdir().unwrap();
    let file_path = tmp.path().join("plus_one.bin");
    let chunk_size = 1024;
    create_test_file(&file_path, chunk_size + 1).await;

    let mut chunker = Chunker::new(&file_path, chunk_size as u32).await.unwrap();
    let (_, tot) = chunker.progress();
    assert_eq!(tot, 2);

    let first = chunker.next_chunk().await.unwrap().unwrap();
    assert_eq!(first.1.len(), chunk_size);

    let second = chunker.next_chunk().await.unwrap().unwrap();
    assert_eq!(second.1.len(), 1);

    assert!(chunker.next_chunk().await.unwrap().is_none());
}

// --- Integrity & Reassembly Tests ---

#[tokio::test]
async fn test_receiver_happy_path() {
    let tmp = tempdir().unwrap();
    let output_dir = tmp.path().join("received");

    let file_data = b"Hello World, Flux File Transfer Rocks!";
    let hash = compute_hash(file_data);
    let meta = TransferMetadata::new("hello.txt".to_string(), file_data.len() as u64, hash);

    let mut receiver = FileReceiver::new(meta.clone(), &output_dir).await.unwrap();
    receiver.write_chunk(0, file_data).await.unwrap();

    let finalized_path = receiver.finalize().await.unwrap();
    assert!(finalized_path.exists());

    let received_data = fs::read(finalized_path).await.unwrap();
    assert_eq!(received_data, file_data);
}

#[tokio::test]
async fn test_receiver_size_mismatch() {
    let tmp = tempdir().unwrap();
    let output_dir = tmp.path().join("received");

    let file_data = b"Some data";
    let hash = compute_hash(file_data);
    let meta = TransferMetadata::new(
        "bad_size.txt".to_string(),
        file_data.len() as u64 + 10,
        hash,
    );

    let mut receiver = FileReceiver::new(meta, &output_dir).await.unwrap();
    receiver.write_chunk(0, file_data).await.unwrap();

    let result = receiver.finalize().await;
    assert!(result.is_err());
    match result {
        Err(TransferError::SizeMismatch { .. }) => {}
        other => panic!("Expected SizeMismatch, got {:?}", other),
    }
}

#[tokio::test]
async fn test_receiver_integrity_mismatch() {
    let tmp = tempdir().unwrap();
    let output_dir = tmp.path().join("received");

    let file_data = b"Clean data";
    let corrupted_data = b"Dirty data";
    let hash = compute_hash(file_data);
    let meta = TransferMetadata::new(
        "corrupted.txt".to_string(),
        corrupted_data.len() as u64,
        hash,
    );

    let mut receiver = FileReceiver::new(meta, &output_dir).await.unwrap();
    receiver.write_chunk(0, corrupted_data).await.unwrap();

    let result = receiver.finalize().await;
    assert!(result.is_err());
    match result {
        Err(TransferError::IntegrityMismatch { .. }) => {}
        other => panic!("Expected IntegrityMismatch, got {:?}", other),
    }
}

#[tokio::test]
async fn test_receiver_invalid_sequence_index() {
    let tmp = tempdir().unwrap();
    let output_dir = tmp.path().join("received");

    let file_data = b"Testing sequence";
    let hash = compute_hash(file_data);
    let meta = TransferMetadata::new("out_of_order.txt".to_string(), file_data.len() as u64, hash);

    let mut receiver = FileReceiver::new(meta, &output_dir).await.unwrap();
    let result = receiver.write_chunk(1, file_data).await; // sending index 1 instead of 0
    assert!(result.is_err());
    match result {
        Err(TransferError::InvalidChunkIndex {
            expected: 0,
            actual: 1,
        }) => {}
        other => panic!("Expected InvalidChunkIndex, got {:?}", other),
    }
}

// --- End-To-End (E2E) Live Transport Transfer Test ---

#[tokio::test]
async fn test_e2e_tcp_file_transfer() {
    let tmp = tempdir().unwrap();
    let sender_dir = tmp.path().join("sender");
    let receiver_dir = tmp.path().join("receiver");
    fs::create_dir_all(&sender_dir).await.unwrap();
    fs::create_dir_all(&receiver_dir).await.unwrap();

    // 1. Create unique, robust payload (150 KB to trigger multiple 64KB chunks)
    let file_path = sender_dir.join("large_payload.bin");
    let original_payload = create_test_file(&file_path, 150 * 1024).await;

    // 2. Set up identities
    let local_peer_id = PeerId::new();
    let remote_peer_id = PeerId::new();

    // 3. Bind Listener
    let transport_receiver = TcpTransport::new();
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap(); // Bind to ephemeral port
    let mut listener = transport_receiver.listen(addr).await.unwrap();
    let local_addr = listener.local_addr();

    // 4. Run Receiver Loop in Background
    let remote_id_for_spawn = remote_peer_id.clone();
    let receiver_dir_for_spawn = receiver_dir.clone();
    let receiver_handle = tokio::spawn(async move {
        let (mut conn, _) = listener.accept().await.unwrap();

        // Perform server side handshake
        if let Some(tcp_conn) = conn
            .as_any_mut()
            .downcast_mut::<flux_core::transport::TcpConnection>()
        {
            tcp_conn
                .server_handshake(&remote_id_for_spawn)
                .await
                .unwrap();
        }

        let mut session = Session::from_connection(conn, remote_id_for_spawn);

        // Expect TransferRequest
        let first_msg = session.recv_message().await.unwrap();
        if let FluxMessage::TransferRequest { metadata } = first_msg {
            TransferManager::receive_transfer(&mut session, metadata, &receiver_dir_for_spawn)
                .await
                .unwrap();
        } else {
            panic!("Expected TransferRequest message!");
        }

        session.close().await.unwrap();
    });

    // 5. Connect and Transfer on Sender
    let transport_sender = TcpTransport::new();
    let session_builder = SessionBuilder::new(&transport_sender, local_peer_id.clone());
    let mut session_sender = session_builder
        .connect(&remote_peer_id, local_addr)
        .await
        .unwrap();

    TransferManager::send_file(&mut session_sender, &file_path)
        .await
        .unwrap();
    session_sender.close().await.unwrap();

    // 6. Wait for receiver to finish execution
    receiver_handle.await.unwrap();

    // 7. Verify file exists at receiver and is byte-for-byte identical
    let received_file_path = receiver_dir.join("large_payload.bin");
    assert!(received_file_path.exists());

    let received_payload = fs::read(received_file_path).await.unwrap();
    assert_eq!(received_payload, original_payload);
}

// =========================================================================
// S1.5 — Transfer Resume & Recovery Tests
// =========================================================================

// --- Chunker Seek Tests ---

#[tokio::test]
async fn test_chunker_seek_to_chunk() {
    let tmp = tempdir().unwrap();
    let file_path = tmp.path().join("seek_test.bin");
    let chunk_size: u32 = 1024;
    // 4 chunks: 0..1023, 1024..2047, 2048..3071, 3072..4095
    create_test_file(&file_path, 4096).await;

    let mut chunker = Chunker::new(&file_path, chunk_size).await.unwrap();

    // Seek to chunk 2 (skip 0 and 1)
    chunker.seek_to_chunk(2).await.unwrap();
    let (cur, _) = chunker.progress();
    assert_eq!(cur, 2);

    let (idx, data) = chunker.next_chunk().await.unwrap().unwrap();
    assert_eq!(idx, 2);
    assert_eq!(data.len(), 1024);
    // Verify the data starts at byte 2048
    assert_eq!(data[0], (2048 % 256) as u8);

    let (idx, data) = chunker.next_chunk().await.unwrap().unwrap();
    assert_eq!(idx, 3);
    assert_eq!(data.len(), 1024);

    assert!(chunker.next_chunk().await.unwrap().is_none());
}

#[tokio::test]
async fn test_chunker_seek_beyond_end() {
    let tmp = tempdir().unwrap();
    let file_path = tmp.path().join("seek_end.bin");
    create_test_file(&file_path, 2048).await;

    let mut chunker = Chunker::new(&file_path, 1024).await.unwrap();
    chunker.seek_to_chunk(100).await.unwrap();

    assert!(chunker.next_chunk().await.unwrap().is_none());
}

// --- Receiver Resume Unit Tests ---

#[tokio::test]
async fn test_receiver_resume_no_partial_state() {
    let tmp = tempdir().unwrap();
    let output_dir = tmp.path().join("received");
    fs::create_dir_all(&output_dir).await.unwrap();

    let file_data = b"No partial state here";
    let hash = compute_hash(file_data);
    let meta = TransferMetadata::new("fresh.txt".to_string(), file_data.len() as u64, hash);

    // No .part or .part.meta exists — should return None
    let result = FileReceiver::try_resume(meta, &output_dir).await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn test_receiver_resume_mismatched_state() {
    let tmp = tempdir().unwrap();
    let output_dir = tmp.path().join("received");
    fs::create_dir_all(&output_dir).await.unwrap();

    let file_data = b"Original file data for testing";
    let hash = compute_hash(file_data);
    let meta = TransferMetadata::new("mismatch.txt".to_string(), file_data.len() as u64, hash);

    // Create a .part file with some data
    let part_path = output_dir.join("mismatch.txt.part");
    fs::write(&part_path, b"partial").await.unwrap();

    // Create a .part.meta with WRONG metadata (different sha256)
    let wrong_state = PartialTransferState {
        file_name: "mismatch.txt".to_string(),
        file_size: file_data.len() as u64,
        chunk_size: 64 * 1024,
        total_chunks: 1,
        sha256: [0xFF; 32], // Wrong hash
        chunks_received: 1,
        bytes_received: 7,
    };
    let meta_bytes = bincode::serialize(&wrong_state).unwrap();
    fs::write(output_dir.join("mismatch.txt.part.meta"), &meta_bytes)
        .await
        .unwrap();

    // Should return None because metadata doesn't match
    let result = FileReceiver::try_resume(meta, &output_dir).await.unwrap();
    assert!(result.is_none());

    // Stale files should be cleaned up
    assert!(!part_path.exists());
}

#[tokio::test]
async fn test_receiver_resume_from_partial() {
    let tmp = tempdir().unwrap();
    let output_dir = tmp.path().join("received");
    fs::create_dir_all(&output_dir).await.unwrap();

    // Simulate a file with 2 chunks using the official DEFAULT_CHUNK_SIZE
    let chunk_size: u32 = DEFAULT_CHUNK_SIZE;
    let file_size: u64 = chunk_size as u64 + 1024; // 1 full chunk + 1KB partial chunk
    let full_data: Vec<u8> = (0..file_size).map(|i| (i % 256) as u8).collect();
    let hash = compute_hash(&full_data);
    let meta = TransferMetadata::new("resume.txt".to_string(), file_size, hash);

    // Write first full chunk to .part
    let part_path = output_dir.join("resume.txt.part");
    fs::write(&part_path, &full_data[..chunk_size as usize]).await.unwrap();

    // Write matching .part.meta
    let state = PartialTransferState {
        file_name: "resume.txt".to_string(),
        file_size,
        chunk_size,
        total_chunks: 2,
        sha256: hash,
        chunks_received: 1,
        bytes_received: chunk_size as u64,
    };
    let meta_bytes = bincode::serialize(&state).unwrap();
    fs::write(output_dir.join("resume.txt.part.meta"), &meta_bytes)
        .await
        .unwrap();

    // Resume should succeed
    let receiver = FileReceiver::try_resume(meta.clone(), &output_dir)
        .await
        .unwrap();
    assert!(receiver.is_some());

    let mut receiver = receiver.unwrap();
    assert!(receiver.is_resumed());
    assert_eq!(receiver.resume_from_chunk(), 1);

    // Write the remaining chunk
    receiver.write_chunk(1, &full_data[chunk_size as usize..]).await.unwrap();

    // Finalize
    let path = receiver.finalize().await.unwrap();
    assert!(path.exists());

    let received = fs::read(path).await.unwrap();
    assert_eq!(received, full_data);
}

// --- E2E Resume Transfer Test ---

#[tokio::test]
async fn test_e2e_tcp_resume_transfer() {
    let tmp = tempdir().unwrap();
    let sender_dir = tmp.path().join("sender");
    let receiver_dir = tmp.path().join("receiver");
    fs::create_dir_all(&sender_dir).await.unwrap();
    fs::create_dir_all(&receiver_dir).await.unwrap();

    // 1. Create a 200KB test file (4 chunks at 64KB: 3 full + 1 partial)
    let file_path = sender_dir.join("resume_test.bin");
    let original_payload = create_test_file(&file_path, 200 * 1024).await;
    let file_hash = compute_hash(&original_payload);

    // 2. Simulate interrupted transfer: first 2 chunks already received
    let chunk_size: u32 = 64 * 1024;
    let partial_bytes = (2 * chunk_size) as usize;
    let part_path = receiver_dir.join("resume_test.bin.part");
    fs::write(&part_path, &original_payload[..partial_bytes])
        .await
        .unwrap();

    let state = PartialTransferState {
        file_name: "resume_test.bin".to_string(),
        file_size: original_payload.len() as u64,
        chunk_size,
        total_chunks: 4,
        sha256: file_hash,
        chunks_received: 2,
        bytes_received: partial_bytes as u64,
    };
    let meta_bytes = bincode::serialize(&state).unwrap();
    let meta_path = receiver_dir.join("resume_test.bin.part.meta");
    fs::write(&meta_path, &meta_bytes).await.unwrap();

    // 3. Set up identities and transport
    let local_peer_id = PeerId::new();
    let remote_peer_id = PeerId::new();

    let transport_receiver = TcpTransport::new();
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let mut listener = transport_receiver.listen(addr).await.unwrap();
    let local_addr = listener.local_addr();

    // 4. Receiver in background — should detect partial state and resume
    let remote_id_for_spawn = remote_peer_id.clone();
    let receiver_dir_for_spawn = receiver_dir.clone();
    let receiver_handle = tokio::spawn(async move {
        let (mut conn, _) = listener.accept().await.unwrap();

        if let Some(tcp_conn) = conn
            .as_any_mut()
            .downcast_mut::<flux_core::transport::TcpConnection>()
        {
            tcp_conn
                .server_handshake(&remote_id_for_spawn)
                .await
                .unwrap();
        }

        let mut session = Session::from_connection(conn, remote_id_for_spawn);

        let first_msg = session.recv_message().await.unwrap();
        if let FluxMessage::TransferRequest { metadata } = first_msg {
            TransferManager::receive_transfer(&mut session, metadata, &receiver_dir_for_spawn)
                .await
                .unwrap();
        } else {
            panic!("Expected TransferRequest message!");
        }

        session.close().await.unwrap();
    });

    // 5. Sender — should receive TransferResume and skip first 2 chunks
    let transport_sender = TcpTransport::new();
    let session_builder = SessionBuilder::new(&transport_sender, local_peer_id.clone());
    let mut session_sender = session_builder
        .connect(&remote_peer_id, local_addr)
        .await
        .unwrap();

    TransferManager::send_file(&mut session_sender, &file_path)
        .await
        .unwrap();
    session_sender.close().await.unwrap();

    // 6. Wait for receiver
    receiver_handle.await.unwrap();

    // 7. Verify final file is byte-for-byte identical
    let received_file_path = receiver_dir.join("resume_test.bin");
    assert!(received_file_path.exists());

    let received_payload = fs::read(received_file_path).await.unwrap();
    assert_eq!(received_payload, original_payload);

    // 8. Verify partial state was cleaned up
    assert!(!part_path.exists());
    assert!(!meta_path.exists());
}
