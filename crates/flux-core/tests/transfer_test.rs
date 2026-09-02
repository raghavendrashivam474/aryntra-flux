use flux_core::identity::PeerId;
use flux_core::protocol::FluxMessage;
use flux_core::session::{Session, SessionBuilder};
use flux_core::transfer::chunker::Chunker;
use flux_core::transfer::error::TransferError;
use flux_core::transfer::metadata::TransferMetadata;
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
            TransferManager::receive_transfer(&mut session, metadata, &receiver_dir)
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
    let received_file_path = tmp.path().join("receiver").join("large_payload.bin");
    assert!(received_file_path.exists());

    let received_payload = fs::read(received_file_path).await.unwrap();
    assert_eq!(received_payload, original_payload);
}
