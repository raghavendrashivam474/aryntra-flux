use flux_core::identity::PeerId;
use flux_core::protocol::FluxMessage;
use flux_core::session::{Session, SessionBuilder};
use flux_core::transfer::chunker::Chunker;
use flux_core::transfer::error::TransferError;
use flux_core::transfer::metadata::{PartialTransferState, TransferMetadata, DEFAULT_CHUNK_SIZE};
use flux_core::transfer::receiver::FileReceiver;
use flux_core::transfer::{TransferManager, TransferPlan};
use flux_core::transport::tcp::{TcpConnection, TcpTransport};
use flux_core::transport::Transport;
use sha2::{Digest, Sha256};
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

    let mut chunker = Chunker::new(&file_path, DEFAULT_CHUNK_SIZE).await.unwrap();
    assert_eq!(chunker.total_chunks(), 0);
    assert_eq!(chunker.next_chunk().await.unwrap(), None);
}

#[tokio::test]
async fn test_chunker_exact_chunk_size() {
    let tmp = tempdir().unwrap();
    let file_path = tmp.path().join("exact.bin");
    let size = DEFAULT_CHUNK_SIZE as usize;
    let data = create_test_file(&file_path, size).await;

    let mut chunker = Chunker::new(&file_path, DEFAULT_CHUNK_SIZE).await.unwrap();
    assert_eq!(chunker.total_chunks(), 1);

    let (index, chunk) = chunker.next_chunk().await.unwrap().unwrap();
    assert_eq!(index, 0);
    assert_eq!(chunk, data);

    assert_eq!(chunker.next_chunk().await.unwrap(), None);
}

#[tokio::test]
async fn test_chunker_exact_plus_one() {
    let tmp = tempdir().unwrap();
    let file_path = tmp.path().join("exact_plus_one.bin");
    let size = (DEFAULT_CHUNK_SIZE as usize) + 1;
    let data = create_test_file(&file_path, size).await;

    let mut chunker = Chunker::new(&file_path, DEFAULT_CHUNK_SIZE).await.unwrap();
    assert_eq!(chunker.total_chunks(), 2);

    let (index1, chunk1) = chunker.next_chunk().await.unwrap().unwrap();
    assert_eq!(index1, 0);
    assert_eq!(chunk1.len(), DEFAULT_CHUNK_SIZE as usize);
    assert_eq!(chunk1, &data[..DEFAULT_CHUNK_SIZE as usize]);

    let (index2, chunk2) = chunker.next_chunk().await.unwrap().unwrap();
    assert_eq!(index2, 1);
    assert_eq!(chunk2.len(), 1);
    assert_eq!(chunk2, &data[DEFAULT_CHUNK_SIZE as usize..]);

    assert_eq!(chunker.next_chunk().await.unwrap(), None);
}

#[tokio::test]
async fn test_chunker_seek_to_chunk() {
    let tmp = tempdir().unwrap();
    let file_path = tmp.path().join("seek_test.bin");
    let size = (DEFAULT_CHUNK_SIZE as usize) * 3;
    let data = create_test_file(&file_path, size).await;

    let mut chunker = Chunker::new(&file_path, DEFAULT_CHUNK_SIZE).await.unwrap();
    assert_eq!(chunker.total_chunks(), 3);

    // Seek to chunk 2 (third chunk)
    chunker.seek_to_chunk(2).await.unwrap();
    let (cur, tot) = chunker.progress();
    assert_eq!(cur, 2);
    assert_eq!(tot, 3);
    assert_eq!(chunker.bytes_read(), (DEFAULT_CHUNK_SIZE as u64) * 2);

    let (index, chunk) = chunker.next_chunk().await.unwrap().unwrap();
    assert_eq!(index, 2);
    let expected = &data[(DEFAULT_CHUNK_SIZE as usize) * 2..];
    assert_eq!(chunk, expected);

    assert_eq!(chunker.next_chunk().await.unwrap(), None);
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

// --- Receiver Tests ---

#[tokio::test]
async fn test_receiver_happy_path() {
    let tmp = tempdir().unwrap();
    let out_dir = tmp.path().join("received");

    let size = 100 * 1024; // 100 KB
    let data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
    let hash = compute_hash(&data);

    let meta = TransferMetadata::new("happy.bin".to_string(), size as u64, hash);
    let mut receiver = FileReceiver::new(meta.clone(), &out_dir).await.unwrap();

    let chunks: Vec<&[u8]> = data.chunks(DEFAULT_CHUNK_SIZE as usize).collect();
    for (i, chunk) in chunks.iter().enumerate() {
        receiver.write_chunk(i as u32, chunk).await.unwrap();
    }

    let final_path = receiver.finalize().await.unwrap();
    assert!(final_path.exists());
    let written = fs::read(&final_path).await.unwrap();
    assert_eq!(written, data);
}

#[tokio::test]
async fn test_receiver_invalid_sequence_index() {
    let tmp = tempdir().unwrap();
    let out_dir = tmp.path().join("received");

    let meta = TransferMetadata::new("seq.bin".to_string(), 1000, [0u8; 32]);
    let mut receiver = FileReceiver::new(meta, &out_dir).await.unwrap();

    let res = receiver.write_chunk(1, &[0u8; 100]).await; // Sending 1 instead of 0
    assert!(matches!(
        res,
        Err(TransferError::InvalidChunkIndex {
            expected: 0,
            actual: 1
        })
    ));
}

#[tokio::test]
async fn test_receiver_size_mismatch() {
    let tmp = tempdir().unwrap();
    let out_dir = tmp.path().join("received");

    let meta = TransferMetadata::new("size_err.bin".to_string(), 500, [0u8; 32]);
    let mut receiver = FileReceiver::new(meta, &out_dir).await.unwrap();

    receiver.write_chunk(0, &[0u8; 100]).await.unwrap();

    let res = receiver.finalize().await;
    assert!(matches!(
        res,
        Err(TransferError::SizeMismatch {
            expected: 500,
            actual: 100
        })
    ));
}

#[tokio::test]
async fn test_receiver_integrity_mismatch() {
    let tmp = tempdir().unwrap();
    let out_dir = tmp.path().join("received");

    let fake_hash = [0xFF; 32];
    let meta = TransferMetadata::new("corrupt.bin".to_string(), 10, fake_hash);
    let mut receiver = FileReceiver::new(meta, &out_dir).await.unwrap();

    receiver.write_chunk(0, &[0x00; 10]).await.unwrap();

    let res = receiver.finalize().await;
    assert!(matches!(res, Err(TransferError::IntegrityMismatch { .. })));
}

// --- E2E Single File Tests ---

#[tokio::test]
async fn test_e2e_tcp_file_transfer() {
    let sender_dir = tempdir().unwrap();
    let receiver_dir = tempdir().unwrap();

    let file_path = sender_dir.path().join("large_payload.bin");
    let payload_size = 250 * 1024; // 250 KB (4 chunks: 3 full + 1 partial)
    let original_payload = create_test_file(&file_path, payload_size).await;

    let transport_receiver = TcpTransport::new();
    let mut listener = transport_receiver
        .listen("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let local_addr = listener.local_addr();

    let local_peer_id = PeerId::new();
    let remote_peer_id = PeerId::new();

    let remote_id_for_spawn = remote_peer_id.clone();
    let receiver_dir_for_spawn = receiver_dir.path().to_path_buf();

    let receiver_handle = tokio::spawn(async move {
        let (mut conn, _) = listener.accept().await.unwrap();

        if let Some(tcp_conn) = conn.as_any_mut().downcast_mut::<TcpConnection>() {
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

    receiver_handle.await.unwrap();

    let received_file_path = receiver_dir.path().join("large_payload.bin");
    assert!(received_file_path.exists());

    let received_payload = fs::read(received_file_path).await.unwrap();
    assert_eq!(received_payload, original_payload);
}

// --- Resume Tests ---

#[tokio::test]
async fn test_receiver_resume_no_partial_state() {
    let tmp = tempdir().unwrap();
    let out_dir = tmp.path().join("received");

    let meta = TransferMetadata::new("fresh.bin".to_string(), 1000, [0u8; 32]);
    let resume_result = FileReceiver::try_resume(meta, &out_dir).await.unwrap();
    assert!(resume_result.is_none());
}

#[tokio::test]
async fn test_receiver_resume_from_partial() {
    let tmp = tempdir().unwrap();
    let out_dir = tmp.path().join("received");
    fs::create_dir_all(&out_dir).await.unwrap();

    let size = (DEFAULT_CHUNK_SIZE as usize) * 3; // 3 chunks
    let data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
    let hash = compute_hash(&data);

    let meta = TransferMetadata::new("interrupted.bin".to_string(), size as u64, hash);

    // Simulate partial state: 2 chunks written
    let chunks_written = 2u32;
    let bytes_written = (chunks_written as u64) * (DEFAULT_CHUNK_SIZE as u64);
    let part_data = &data[..bytes_written as usize];

    let part_path = out_dir.join("interrupted.bin.part");
    let meta_path = out_dir.join("interrupted.bin.part.meta");

    fs::write(&part_path, part_data).await.unwrap();

    let partial_state = PartialTransferState {
        file_name: "interrupted.bin".to_string(),
        file_size: size as u64,
        chunk_size: DEFAULT_CHUNK_SIZE,
        total_chunks: 3,
        sha256: hash,
        chunks_received: chunks_written,
        bytes_received: bytes_written,
        relative_path: None,
    };
    let encoded_meta = bincode::serialize(&partial_state).unwrap();
    fs::write(&meta_path, &encoded_meta).await.unwrap();

    let mut receiver = FileReceiver::try_resume(meta.clone(), &out_dir)
        .await
        .unwrap()
        .expect("should find partial state");

    assert!(receiver.is_resumed());
    assert_eq!(receiver.resume_from_chunk(), 2);

    // Send final chunk (chunk index 2)
    let last_chunk = &data[(DEFAULT_CHUNK_SIZE as usize) * 2..];
    receiver.write_chunk(2, last_chunk).await.unwrap();

    let final_path = receiver.finalize().await.unwrap();
    assert!(final_path.exists());
    let written = fs::read(&final_path).await.unwrap();
    assert_eq!(written, data);

    assert!(!part_path.exists());
    assert!(!meta_path.exists());
}

#[tokio::test]
async fn test_receiver_resume_mismatched_state() {
    let tmp = tempdir().unwrap();
    let out_dir = tmp.path().join("received");
    fs::create_dir_all(&out_dir).await.unwrap();

    let part_path = out_dir.join("mismatch.bin.part");
    let meta_path = out_dir.join("mismatch.bin.part.meta");

    fs::write(&part_path, &[0u8; 100]).await.unwrap();

    let wrong_state = PartialTransferState {
        file_name: "mismatch.bin".to_string(),
        file_size: 9999, // Mismatched size
        chunk_size: DEFAULT_CHUNK_SIZE,
        total_chunks: 1,
        sha256: [0xFF; 32],
        chunks_received: 1,
        bytes_received: 100,
        relative_path: None,
    };
    let encoded_meta = bincode::serialize(&wrong_state).unwrap();
    fs::write(&meta_path, &encoded_meta).await.unwrap();

    let meta = TransferMetadata::new("mismatch.bin".to_string(), 1000, [0u8; 32]);
    let resume_result = FileReceiver::try_resume(meta, &out_dir).await.unwrap();
    assert!(resume_result.is_none());

    assert!(!part_path.exists());
    assert!(!meta_path.exists());
}

#[tokio::test]
async fn test_e2e_tcp_resume_transfer() {
    let sender_dir = tempdir().unwrap();
    let receiver_dir = tempdir().unwrap();

    let file_path = sender_dir.path().join("resume_test.bin");
    let payload_size = 200 * 1024; // 200 KB (4 chunks)
    let original_payload = create_test_file(&file_path, payload_size).await;
    let file_hash = compute_hash(&original_payload);

    let chunks_received = 2u32;
    let bytes_received = (chunks_received as u64) * (DEFAULT_CHUNK_SIZE as u64);
    let part_data = &original_payload[..bytes_received as usize];

    let part_path = receiver_dir.path().join("resume_test.bin.part");
    let meta_path = receiver_dir.path().join("resume_test.bin.part.meta");

    fs::write(&part_path, part_data).await.unwrap();

    let partial_state = PartialTransferState {
        file_name: "resume_test.bin".to_string(),
        file_size: payload_size as u64,
        chunk_size: DEFAULT_CHUNK_SIZE,
        total_chunks: (payload_size as u64).div_ceil(DEFAULT_CHUNK_SIZE as u64) as u32,
        sha256: file_hash,
        chunks_received,
        bytes_received,
        relative_path: None,
    };
    let encoded_meta = bincode::serialize(&partial_state).unwrap();
    fs::write(&meta_path, &encoded_meta).await.unwrap();

    let transport_receiver = TcpTransport::new();
    let mut listener = transport_receiver
        .listen("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let local_addr = listener.local_addr();

    let local_peer_id = PeerId::new();
    let remote_peer_id = PeerId::new();

    let remote_id_for_spawn = remote_peer_id.clone();
    let receiver_dir_for_spawn = receiver_dir.path().to_path_buf();

    let receiver_handle = tokio::spawn(async move {
        let (mut conn, _) = listener.accept().await.unwrap();

        if let Some(tcp_conn) = conn.as_any_mut().downcast_mut::<TcpConnection>() {
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

    receiver_handle.await.unwrap();

    let received_file_path = receiver_dir.path().join("resume_test.bin");
    assert!(received_file_path.exists());

    let received_payload = fs::read(received_file_path).await.unwrap();
    assert_eq!(received_payload, original_payload);

    assert!(!part_path.exists());
    assert!(!meta_path.exists());
}

// --- S1.6: Multi-File & Directory Transfer Tests ---

#[tokio::test]
async fn test_e2e_tcp_multi_file_transfer() {
    let sender_dir = tempdir().unwrap();
    let receiver_dir = tempdir().unwrap();

    let file1_path = sender_dir.path().join("file1.bin");
    let payload1 = create_test_file(&file1_path, 1024).await;

    let file2_path = sender_dir.path().join("file2.bin");
    let payload2 = create_test_file(&file2_path, 150 * 1024).await;

    let file3_path = sender_dir.path().join("file3.bin");
    let payload3 = create_test_file(&file3_path, 42).await;

    let plan =
        TransferPlan::from_paths(&[file1_path.clone(), file2_path.clone(), file3_path.clone()])
            .unwrap();
    assert_eq!(plan.len(), 3);

    let transport_receiver = TcpTransport::new();
    let mut listener = transport_receiver
        .listen("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let local_addr = listener.local_addr();

    let local_peer_id = PeerId::new();
    let remote_peer_id = PeerId::new();

    let remote_id_for_spawn = remote_peer_id.clone();
    let receiver_dir_for_spawn = receiver_dir.path().to_path_buf();

    let receiver_handle = tokio::spawn(async move {
        let (mut conn, _) = listener.accept().await.unwrap();

        if let Some(tcp_conn) = conn.as_any_mut().downcast_mut::<TcpConnection>() {
            tcp_conn
                .server_handshake(&remote_id_for_spawn)
                .await
                .unwrap();
        }

        let mut session = Session::from_connection(conn, remote_id_for_spawn);
        TransferManager::receive_collection(&mut session, &receiver_dir_for_spawn)
            .await
            .unwrap();

        session.close().await.unwrap();
    });

    let transport_sender = TcpTransport::new();
    let session_builder = SessionBuilder::new(&transport_sender, local_peer_id.clone());
    let mut session_sender = session_builder
        .connect(&remote_peer_id, local_addr)
        .await
        .unwrap();

    TransferManager::send_collection(&mut session_sender, &plan)
        .await
        .unwrap();
    session_sender.close().await.unwrap();

    receiver_handle.await.unwrap();

    for (filename, expected_payload) in [
        ("file1.bin", &payload1),
        ("file2.bin", &payload2),
        ("file3.bin", &payload3),
    ] {
        let path = receiver_dir.path().join(filename);
        assert!(path.exists(), "File {} should exist on receiver", filename);
        let actual = fs::read(&path).await.unwrap();
        assert_eq!(&actual, expected_payload);
    }
}

#[tokio::test]
async fn test_e2e_tcp_directory_transfer() {
    let sender_root = tempdir().unwrap();
    let receiver_dir = tempdir().unwrap();

    let root_file = sender_root.path().join("root_file.txt");
    let payload_root = create_test_file(&root_file, 500).await;

    let sub_a = sender_root.path().join("subA");
    fs::create_dir_all(&sub_a).await.unwrap();
    let nested1 = sub_a.join("nested1.bin");
    let payload_nested1 = create_test_file(&nested1, 80 * 1024).await;

    let deep_dir = sender_root.path().join("subB").join("deep");
    fs::create_dir_all(&deep_dir).await.unwrap();
    let nested2 = deep_dir.join("nested2.bin");
    let payload_nested2 = create_test_file(&nested2, 1200).await;

    let plan = TransferPlan::from_paths(&[sender_root.path().to_path_buf()]).unwrap();
    assert_eq!(plan.len(), 3);

    let transport_receiver = TcpTransport::new();
    let mut listener = transport_receiver
        .listen("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let local_addr = listener.local_addr();

    let local_peer_id = PeerId::new();
    let remote_peer_id = PeerId::new();

    let remote_id_for_spawn = remote_peer_id.clone();
    let receiver_dir_for_spawn = receiver_dir.path().to_path_buf();

    let receiver_handle = tokio::spawn(async move {
        let (mut conn, _) = listener.accept().await.unwrap();

        if let Some(tcp_conn) = conn.as_any_mut().downcast_mut::<TcpConnection>() {
            tcp_conn
                .server_handshake(&remote_id_for_spawn)
                .await
                .unwrap();
        }

        let mut session = Session::from_connection(conn, remote_id_for_spawn);
        TransferManager::receive_collection(&mut session, &receiver_dir_for_spawn)
            .await
            .unwrap();

        session.close().await.unwrap();
    });

    let transport_sender = TcpTransport::new();
    let session_builder = SessionBuilder::new(&transport_sender, local_peer_id.clone());
    let mut session_sender = session_builder
        .connect(&remote_peer_id, local_addr)
        .await
        .unwrap();

    TransferManager::send_collection(&mut session_sender, &plan)
        .await
        .unwrap();
    session_sender.close().await.unwrap();

    receiver_handle.await.unwrap();

    let received_root_file = receiver_dir.path().join("root_file.txt");
    assert!(received_root_file.exists());
    assert_eq!(fs::read(&received_root_file).await.unwrap(), payload_root);

    let received_nested1 = receiver_dir.path().join("subA").join("nested1.bin");
    assert!(received_nested1.exists());
    assert_eq!(fs::read(&received_nested1).await.unwrap(), payload_nested1);

    let received_nested2 = receiver_dir
        .path()
        .join("subB")
        .join("deep")
        .join("nested2.bin");
    assert!(received_nested2.exists());
    assert_eq!(fs::read(&received_nested2).await.unwrap(), payload_nested2);
}

#[tokio::test]
async fn test_e2e_tcp_multi_file_resume() {
    let sender_dir = tempdir().unwrap();
    let receiver_dir = tempdir().unwrap();

    let file1_path = sender_dir.path().join("fresh.bin");
    let payload1 = create_test_file(&file1_path, 10 * 1024).await;

    let file2_path = sender_dir.path().join("resumed.bin");
    let payload2 = create_test_file(&file2_path, 150 * 1024).await;
    let file2_hash = compute_hash(&payload2);

    let plan = TransferPlan::from_paths(&[file1_path.clone(), file2_path.clone()]).unwrap();

    let chunks_received = 2u32;
    let bytes_received = (chunks_received as u64) * (DEFAULT_CHUNK_SIZE as u64);
    let part_data = &payload2[..bytes_received as usize];

    let part_path = receiver_dir.path().join("resumed.bin.part");
    let meta_path = receiver_dir.path().join("resumed.bin.part.meta");

    fs::write(&part_path, part_data).await.unwrap();

    let partial_state = PartialTransferState {
        file_name: "resumed.bin".to_string(),
        file_size: payload2.len() as u64,
        chunk_size: DEFAULT_CHUNK_SIZE,
        total_chunks: (payload2.len() as u64).div_ceil(DEFAULT_CHUNK_SIZE as u64) as u32,
        sha256: file2_hash,
        chunks_received,
        bytes_received,
        relative_path: Some("resumed.bin".to_string()),
    };
    let encoded_meta = bincode::serialize(&partial_state).unwrap();
    fs::write(&meta_path, &encoded_meta).await.unwrap();

    let transport_receiver = TcpTransport::new();
    let mut listener = transport_receiver
        .listen("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let local_addr = listener.local_addr();

    let local_peer_id = PeerId::new();
    let remote_peer_id = PeerId::new();

    let remote_id_for_spawn = remote_peer_id.clone();
    let receiver_dir_for_spawn = receiver_dir.path().to_path_buf();

    let receiver_handle = tokio::spawn(async move {
        let (mut conn, _) = listener.accept().await.unwrap();

        if let Some(tcp_conn) = conn.as_any_mut().downcast_mut::<TcpConnection>() {
            tcp_conn
                .server_handshake(&remote_id_for_spawn)
                .await
                .unwrap();
        }

        let mut session = Session::from_connection(conn, remote_id_for_spawn);
        TransferManager::receive_collection(&mut session, &receiver_dir_for_spawn)
            .await
            .unwrap();

        session.close().await.unwrap();
    });

    let transport_sender = TcpTransport::new();
    let session_builder = SessionBuilder::new(&transport_sender, local_peer_id.clone());
    let mut session_sender = session_builder
        .connect(&remote_peer_id, local_addr)
        .await
        .unwrap();

    TransferManager::send_collection(&mut session_sender, &plan)
        .await
        .unwrap();
    session_sender.close().await.unwrap();

    receiver_handle.await.unwrap();

    let rec1 = receiver_dir.path().join("fresh.bin");
    assert!(rec1.exists());
    assert_eq!(fs::read(&rec1).await.unwrap(), payload1);

    let rec2 = receiver_dir.path().join("resumed.bin");
    assert!(rec2.exists());
    assert_eq!(fs::read(&rec2).await.unwrap(), payload2);

    assert!(!part_path.exists());
    assert!(!meta_path.exists());
}

// --- S3.3: Cancellation and Resume Integration Tests ---

#[tokio::test]
async fn test_e2e_cancellation_pre_transfer() {
    let sender_dir = tempdir().unwrap();
    let receiver_dir = tempdir().unwrap();

    let file_path = sender_dir.path().join("cancel_pre.bin");
    create_test_file(&file_path, 128 * 1024).await;

    let plan = TransferPlan::from_paths(&[file_path]).unwrap();

    let local_peer_id = PeerId::new();
    let remote_peer_id = PeerId::new();

    let transport_receiver = TcpTransport::new();
    let mut listener = transport_receiver
        .listen("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let local_addr = listener.local_addr();

    let r_dir = receiver_dir.path().to_path_buf();
    let r_id = remote_peer_id.clone();
    let receiver_handle = tokio::spawn(async move {
        let (mut conn, _) = listener.accept().await.unwrap();
        if let Some(tcp_conn) = conn.as_any_mut().downcast_mut::<TcpConnection>() {
            tcp_conn.server_handshake(&r_id).await.unwrap();
        }
        let mut session = Session::from_connection(conn, r_id);
        let _ = TransferManager::receive_collection(&mut session, &r_dir).await;
        let _ = session.close().await;
    });

    let transport_sender = TcpTransport::new();
    let session_builder = SessionBuilder::new(&transport_sender, local_peer_id.clone());
    let mut session_sender = session_builder
        .connect(&remote_peer_id, local_addr)
        .await
        .unwrap();

    let cancel = flux_core::transfer::TransferCancellation::new();
    cancel.cancel(); // Cancel before initiating

    let res =
        TransferManager::send_collection_with_cancel(&mut session_sender, &plan, &cancel).await;
    assert!(matches!(res, Err(TransferError::Cancelled)));

    let _ = session_sender.close().await;
    let _ = receiver_handle.await;
}

#[tokio::test]
async fn test_e2e_cancellation_preserves_partial_state_and_resumes() {
    let sender_dir = tempdir().unwrap();
    let receiver_dir = tempdir().unwrap();

    // 500 KiB file (~8 chunks of 64 KiB)
    let file_path = sender_dir.path().join("cancel_resume.bin");
    let payload = create_test_file(&file_path, 500 * 1024).await;
    let expected_hash = compute_hash(&payload);

    let plan = TransferPlan::from_paths(std::slice::from_ref(&file_path)).unwrap();

    let local_peer_id = PeerId::new();
    let remote_peer_id = PeerId::new();

    // Phase 1: Cancel mid-transfer
    let transport_receiver = TcpTransport::new();
    let mut listener = transport_receiver
        .listen("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let local_addr = listener.local_addr();

    let cancel_token = flux_core::transfer::TransferCancellation::new();
    let cancel_token_clone = cancel_token.clone();

    let r_dir = receiver_dir.path().to_path_buf();
    let r_id = remote_peer_id.clone();
    let receiver_handle = tokio::spawn(async move {
        let (mut conn, _) = listener.accept().await.unwrap();
        if let Some(tcp_conn) = conn.as_any_mut().downcast_mut::<TcpConnection>() {
            tcp_conn.server_handshake(&r_id).await.unwrap();
        }
        let mut session = Session::from_connection(conn, r_id);
        let _ = TransferManager::receive_collection(&mut session, &r_dir).await;
        let _ = session.close().await;
    });

    let transport_sender = TcpTransport::new();
    let session_builder = SessionBuilder::new(&transport_sender, local_peer_id.clone());
    let mut session_sender = session_builder
        .connect(&remote_peer_id, local_addr)
        .await
        .unwrap();

    // Trigger cancel concurrently after a tiny delay
    tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_millis(15)).await;
        cancel_token_clone.cancel();
    });

    let _ = TransferManager::send_collection_with_cancel(&mut session_sender, &plan, &cancel_token)
        .await;
    let _ = session_sender.close().await;
    let _ = receiver_handle.await;

    // Phase 2: Resume transfer to completion
    let transport_receiver2 = TcpTransport::new();
    let mut listener2 = transport_receiver2
        .listen("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let local_addr2 = listener2.local_addr();

    let r_dir2 = receiver_dir.path().to_path_buf();
    let r_id2 = remote_peer_id.clone();
    let receiver_handle2 = tokio::spawn(async move {
        let (mut conn, _) = listener2.accept().await.unwrap();
        if let Some(tcp_conn) = conn.as_any_mut().downcast_mut::<TcpConnection>() {
            tcp_conn.server_handshake(&r_id2).await.unwrap();
        }
        let mut session = Session::from_connection(conn, r_id2);
        TransferManager::receive_collection(&mut session, &r_dir2)
            .await
            .unwrap();
        session.close().await.unwrap();
    });

    let transport_sender2 = TcpTransport::new();
    let session_builder2 = SessionBuilder::new(&transport_sender2, local_peer_id.clone());
    let mut session_sender2 = session_builder2
        .connect(&remote_peer_id, local_addr2)
        .await
        .unwrap();

    TransferManager::send_collection(&mut session_sender2, &plan)
        .await
        .unwrap();
    session_sender2.close().await.unwrap();

    receiver_handle2.await.unwrap();

    // Verify completed file integrity
    let final_path = receiver_dir.path().join("cancel_resume.bin");
    assert!(final_path.exists());
    let received_data = fs::read(&final_path).await.unwrap();
    assert_eq!(compute_hash(&received_data), expected_hash);
}

#[tokio::test]
async fn test_e2e_tcp_multi_file_progress_observability() {
    let sender_dir = tempdir().unwrap();
    let receiver_dir = tempdir().unwrap();

    let file1_path = sender_dir.path().join("f1.bin");
    let payload1 = create_test_file(&file1_path, 100 * 1024).await;
    let hash1 = compute_hash(&payload1);

    let file2_path = sender_dir.path().join("f2.bin");
    let payload2 = create_test_file(&file2_path, 200 * 1024).await;
    let hash2 = compute_hash(&payload2);

    let file3_path = sender_dir.path().join("f3.bin");
    let payload3 = create_test_file(&file3_path, 300 * 1024).await;
    let hash3 = compute_hash(&payload3);

    let total_expected_bytes = (payload1.len() + payload2.len() + payload3.len()) as u64;

    let plan = TransferPlan::from_paths(&[file1_path, file2_path, file3_path]).unwrap();
    assert_eq!(plan.len(), 3);

    let sender_progress = flux_core::transfer::TransferProgress::new();
    let receiver_progress = flux_core::transfer::TransferProgress::new();
    let cancel = flux_core::transfer::TransferCancellation::new();

    let transport_receiver = TcpTransport::new();
    let mut listener = transport_receiver
        .listen("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let local_addr = listener.local_addr();

    let local_peer_id = PeerId::new();
    let remote_peer_id = PeerId::new();

    let r_dir = receiver_dir.path().to_path_buf();
    let r_id = remote_peer_id.clone();
    let r_cancel = cancel.clone();
    let r_prog = receiver_progress.clone();

    let receiver_handle = tokio::spawn(async move {
        let (mut conn, _) = listener.accept().await.unwrap();
        if let Some(tcp_conn) = conn.as_any_mut().downcast_mut::<TcpConnection>() {
            tcp_conn.server_handshake(&r_id).await.unwrap();
        }
        let mut session = Session::from_connection(conn, r_id);
        TransferManager::receive_collection_with_cancel_and_progress(
            &mut session,
            &r_dir,
            &r_cancel,
            &r_prog,
        )
        .await
        .unwrap();
        session.close().await.unwrap();
    });

    let transport_sender = TcpTransport::new();
    let session_builder = SessionBuilder::new(&transport_sender, local_peer_id.clone());
    let mut session_sender = session_builder
        .connect(&remote_peer_id, local_addr)
        .await
        .unwrap();

    TransferManager::send_collection_with_cancel_and_progress(
        &mut session_sender,
        &plan,
        &cancel,
        &sender_progress,
    )
    .await
    .unwrap();

    session_sender.close().await.unwrap();
    receiver_handle.await.unwrap();

    // Verify progress observability
    assert_eq!(sender_progress.files_completed(), 3);
    assert_eq!(sender_progress.bytes_transferred(), total_expected_bytes);

    assert_eq!(receiver_progress.files_completed(), 3);
    assert_eq!(receiver_progress.bytes_transferred(), total_expected_bytes);

    // Verify byte integrity on disk
    let data1 = fs::read(receiver_dir.path().join("f1.bin")).await.unwrap();
    assert_eq!(compute_hash(&data1), hash1);

    let data2 = fs::read(receiver_dir.path().join("f2.bin")).await.unwrap();
    assert_eq!(compute_hash(&data2), hash2);

    let data3 = fs::read(receiver_dir.path().join("f3.bin")).await.unwrap();
    assert_eq!(compute_hash(&data3), hash3);
}

#[tokio::test]
async fn test_e2e_tcp_resume_progress_observability() {
    let sender_dir = tempdir().unwrap();
    let receiver_dir = tempdir().unwrap();

    // 200 KiB file
    let file_path = sender_dir.path().join("resume_observable.bin");
    let payload = create_test_file(&file_path, 200 * 1024).await;
    let expected_hash = compute_hash(&payload);

    let plan = TransferPlan::from_paths(std::slice::from_ref(&file_path)).unwrap();

    // Pre-create partial state with 1 chunk (64 KiB)
    let chunks_received = 1u32;
    let bytes_received = (chunks_received as u64) * (DEFAULT_CHUNK_SIZE as u64);
    let part_data = &payload[..bytes_received as usize];

    let part_path = receiver_dir.path().join("resume_observable.bin.part");
    let meta_path = receiver_dir.path().join("resume_observable.bin.part.meta");

    fs::write(&part_path, part_data).await.unwrap();

    let partial_state = PartialTransferState {
        file_name: "resume_observable.bin".to_string(),
        file_size: payload.len() as u64,
        chunk_size: DEFAULT_CHUNK_SIZE,
        total_chunks: (payload.len() as u64).div_ceil(DEFAULT_CHUNK_SIZE as u64) as u32,
        sha256: expected_hash,
        chunks_received,
        bytes_received,
        relative_path: Some("resume_observable.bin".to_string()),
    };
    let encoded_meta = bincode::serialize(&partial_state).unwrap();
    fs::write(&meta_path, &encoded_meta).await.unwrap();

    let sender_progress = flux_core::transfer::TransferProgress::new();
    let receiver_progress = flux_core::transfer::TransferProgress::new();
    let cancel = flux_core::transfer::TransferCancellation::new();

    let transport_receiver = TcpTransport::new();
    let mut listener = transport_receiver
        .listen("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let local_addr = listener.local_addr();

    let local_peer_id = PeerId::new();
    let remote_peer_id = PeerId::new();

    let r_dir = receiver_dir.path().to_path_buf();
    let r_id = remote_peer_id.clone();
    let r_cancel = cancel.clone();
    let r_prog = receiver_progress.clone();

    let receiver_handle = tokio::spawn(async move {
        let (mut conn, _) = listener.accept().await.unwrap();
        if let Some(tcp_conn) = conn.as_any_mut().downcast_mut::<TcpConnection>() {
            tcp_conn.server_handshake(&r_id).await.unwrap();
        }
        let mut session = Session::from_connection(conn, r_id);
        TransferManager::receive_collection_with_cancel_and_progress(
            &mut session,
            &r_dir,
            &r_cancel,
            &r_prog,
        )
        .await
        .unwrap();
        session.close().await.unwrap();
    });

    let transport_sender = TcpTransport::new();
    let session_builder = SessionBuilder::new(&transport_sender, local_peer_id.clone());
    let mut session_sender = session_builder
        .connect(&remote_peer_id, local_addr)
        .await
        .unwrap();

    TransferManager::send_collection_with_cancel_and_progress(
        &mut session_sender,
        &plan,
        &cancel,
        &sender_progress,
    )
    .await
    .unwrap();

    session_sender.close().await.unwrap();
    receiver_handle.await.unwrap();

    // Verify progress across resume accounts for total logical file size
    assert_eq!(sender_progress.files_completed(), 1);
    assert_eq!(sender_progress.bytes_transferred(), payload.len() as u64);

    assert_eq!(receiver_progress.files_completed(), 1);
    assert_eq!(receiver_progress.bytes_transferred(), payload.len() as u64);

    // Verify final file
    let final_path = receiver_dir.path().join("resume_observable.bin");
    let received_data = fs::read(&final_path).await.unwrap();
    assert_eq!(compute_hash(&received_data), expected_hash);
}

// --- S3.5: Path-Aware Transfer Continuity E2E Tests ---

#[tokio::test]
async fn test_e2e_transfer_continues_on_alternate_path() {
    use flux_core::transfer::{TransferCancellation, TransferProgress};

    let sender_dir = tempdir().unwrap();
    let receiver_dir = tempdir().unwrap();

    // 250 KiB file (~4 chunks: 3 full 64 KiB chunks + 1 partial)
    let file_path = sender_dir.path().join("path_continuity.bin");
    let payload_size = 250 * 1024;
    let original_payload = create_test_file(&file_path, payload_size).await;
    let expected_hash = compute_hash(&original_payload);

    let local_peer_id = PeerId::new();
    let remote_peer_id = PeerId::new();

    // --- Phase 1: Establish Path A (Listener A / Session 1) and partially transfer ---
    let transport_receiver_a = TcpTransport::new();
    let mut listener_a = transport_receiver_a
        .listen("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let addr_path_a = listener_a.local_addr();

    let remote_id_spawn_a = remote_peer_id.clone();
    let receiver_dir_spawn_a = receiver_dir.path().to_path_buf();
    let sender_progress = TransferProgress::new();
    let sender_cancel = TransferCancellation::new();

    // Receiver on Path A: receives only 2 chunks, then forcefully breaks session
    let receiver_handle_a = tokio::spawn(async move {
        let (mut conn, _) = listener_a.accept().await.unwrap();
        if let Some(tcp_conn) = conn.as_any_mut().downcast_mut::<TcpConnection>() {
            tcp_conn.server_handshake(&remote_id_spawn_a).await.unwrap();
        }

        let mut session = Session::from_connection(conn, remote_id_spawn_a);
        let first_msg = session.recv_message().await.unwrap();

        if let FluxMessage::TransferRequest { metadata } = first_msg {
            let mut receiver = FileReceiver::new(metadata.clone(), &receiver_dir_spawn_a)
                .await
                .unwrap();
            session
                .send_message(&FluxMessage::TransferAccept {
                    transfer_id: metadata.transfer_id,
                })
                .await
                .unwrap();

            // Receive exactly 2 chunks
            for _ in 0..2 {
                if let FluxMessage::TransferChunk { index, data, .. } =
                    session.recv_message().await.unwrap()
                {
                    receiver.write_chunk(index, &data).await.unwrap();
                }
            }
            // Simulating sudden path failure: drop session abruptly without finalize
            drop(session);
        }
    });

    let transport_sender = TcpTransport::new();
    let session_builder = SessionBuilder::new(&transport_sender, local_peer_id.clone());
    let mut session_sender_a = session_builder
        .connect(&remote_peer_id, addr_path_a)
        .await
        .unwrap();

    // Sender sends file on Path A; it will hit a connection error when receiver drops
    let _ = TransferManager::send_file_with_cancel_and_progress(
        &mut session_sender_a,
        &file_path,
        &sender_cancel,
        &sender_progress,
    )
    .await;

    let _ = receiver_handle_a.await;

    // Verify partial state exists on disk (.part and .part.meta)
    let part_path = receiver_dir.path().join("path_continuity.bin.part");
    let meta_path = receiver_dir.path().join("path_continuity.bin.part.meta");
    assert!(
        part_path.exists(),
        "Checkpoint .part must exist on Path A failure"
    );
    assert!(
        meta_path.exists(),
        "Checkpoint .part.meta must exist on Path A failure"
    );

    // Bytes transferred on Path A should be at least the 2 chunks
    assert!(sender_progress.bytes_transferred() >= (2 * DEFAULT_CHUNK_SIZE as u64));

    // --- Phase 2: Establish Path B (Listener B / Session 2) and resume the same transfer ---
    let transport_receiver_b = TcpTransport::new();
    let mut listener_b = transport_receiver_b
        .listen("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let addr_path_b = listener_b.local_addr();

    let remote_id_spawn_b = remote_peer_id.clone();
    let receiver_dir_spawn_b = receiver_dir.path().to_path_buf();
    let receiver_progress_b = TransferProgress::new();

    let receiver_handle_b = tokio::spawn(async move {
        let (mut conn, _) = listener_b.accept().await.unwrap();
        if let Some(tcp_conn) = conn.as_any_mut().downcast_mut::<TcpConnection>() {
            tcp_conn.server_handshake(&remote_id_spawn_b).await.unwrap();
        }

        let mut session = Session::from_connection(conn, remote_id_spawn_b);
        let first_msg = session.recv_message().await.unwrap();

        if let FluxMessage::TransferRequest { metadata } = first_msg {
            let cancel = TransferCancellation::new();
            TransferManager::receive_transfer_with_cancel_and_progress(
                &mut session,
                metadata,
                &receiver_dir_spawn_b,
                &cancel,
                &receiver_progress_b,
            )
            .await
            .unwrap();
        }

        session.close().await.unwrap();
        receiver_progress_b
    });

    let mut session_sender_b = session_builder
        .connect(&remote_peer_id, addr_path_b)
        .await
        .unwrap();

    // Continue the exact same logical transfer over Path B using the same progress tracker
    // Reset progress tracking to match the resume starting point (sender will add resumed bytes)
    let resumed_sender_progress = TransferProgress::new();
    TransferManager::send_file_with_cancel_and_progress(
        &mut session_sender_b,
        &file_path,
        &sender_cancel,
        &resumed_sender_progress,
    )
    .await
    .unwrap();

    session_sender_b.close().await.unwrap();
    let final_receiver_progress = receiver_handle_b.await.unwrap();

    // Verify logical transfer completion & integrity
    let final_file = receiver_dir.path().join("path_continuity.bin");
    assert!(
        final_file.exists(),
        "Final file must exist after Path B continuation"
    );
    assert!(
        !part_path.exists(),
        "Temporary .part file must be cleaned up"
    );
    assert!(
        !meta_path.exists(),
        "Temporary .part.meta file must be cleaned up"
    );

    let received_data = fs::read(&final_file).await.unwrap();
    assert_eq!(received_data, original_payload);
    assert_eq!(compute_hash(&received_data), expected_hash);

    // Verify progress metrics
    assert_eq!(
        resumed_sender_progress.bytes_transferred(),
        payload_size as u64
    );
    assert_eq!(
        final_receiver_progress.bytes_transferred(),
        payload_size as u64
    );
}

#[tokio::test]
async fn test_e2e_multi_file_transfer_continues_on_alternate_path() {
    use flux_core::transfer::continuity::TransferContinuation;
    use flux_core::transfer::{TransferCancellation, TransferProgress};

    let sender_dir = tempdir().unwrap();
    let receiver_dir = tempdir().unwrap();

    // 3 files: 50 KB, 200 KB (multichunk), 100 KB
    let f1_path = sender_dir.path().join("doc1.bin");
    let p1 = create_test_file(&f1_path, 50 * 1024).await;
    let h1 = compute_hash(&p1);

    let f2_path = sender_dir.path().join("doc2.bin");
    let p2 = create_test_file(&f2_path, 200 * 1024).await;
    let h2 = compute_hash(&p2);

    let f3_path = sender_dir.path().join("doc3.bin");
    let p3 = create_test_file(&f3_path, 100 * 1024).await;
    let h3 = compute_hash(&p3);

    let total_bytes = (p1.len() + p2.len() + p3.len()) as u64;

    let plan =
        TransferPlan::from_paths(&[f1_path.clone(), f2_path.clone(), f3_path.clone()]).unwrap();

    let local_peer_id = PeerId::new();
    let remote_peer_id = PeerId::new();

    // --- Phase 1: Path A completes f1, partially writes f2, then fails ---
    let transport_receiver_a = TcpTransport::new();
    let mut listener_a = transport_receiver_a
        .listen("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let addr_path_a = listener_a.local_addr();

    let remote_id_spawn_a = remote_peer_id.clone();
    let receiver_dir_spawn_a = receiver_dir.path().to_path_buf();
    let shared_progress = TransferProgress::new();
    let shared_cancel = TransferCancellation::new();

    let receiver_handle_a = tokio::spawn(async move {
        let (mut conn, _) = listener_a.accept().await.unwrap();
        if let Some(tcp_conn) = conn.as_any_mut().downcast_mut::<TcpConnection>() {
            tcp_conn.server_handshake(&remote_id_spawn_a).await.unwrap();
        }

        let mut session = Session::from_connection(conn, remote_id_spawn_a);

        // Receive doc1.bin fully
        let msg1 = session.recv_message().await.unwrap();
        if let FluxMessage::TransferRequest { metadata } = msg1 {
            let cancel = TransferCancellation::new();
            let prog = TransferProgress::new();
            TransferManager::receive_transfer_with_cancel_and_progress(
                &mut session,
                metadata,
                &receiver_dir_spawn_a,
                &cancel,
                &prog,
            )
            .await
            .unwrap();
        }

        // Receive doc2.bin partially (2 chunks)
        let msg2 = session.recv_message().await.unwrap();
        if let FluxMessage::TransferRequest { metadata } = msg2 {
            let mut receiver = FileReceiver::new(metadata.clone(), &receiver_dir_spawn_a)
                .await
                .unwrap();
            session
                .send_message(&FluxMessage::TransferAccept {
                    transfer_id: metadata.transfer_id,
                })
                .await
                .unwrap();

            for _ in 0..2 {
                if let FluxMessage::TransferChunk { index, data, .. } =
                    session.recv_message().await.unwrap()
                {
                    receiver.write_chunk(index, &data).await.unwrap();
                }
            }
            // Sudden Path A connection drop
            drop(session);
        }
    });

    let transport_sender = TcpTransport::new();
    let session_builder = SessionBuilder::new(&transport_sender, local_peer_id.clone());
    let mut session_sender_a = session_builder
        .connect(&remote_peer_id, addr_path_a)
        .await
        .unwrap();

    let _ = TransferManager::send_collection_with_cancel_and_progress(
        &mut session_sender_a,
        &plan,
        &shared_cancel,
        &shared_progress,
    )
    .await;

    let _ = receiver_handle_a.await;

    // Verify doc1 completed, doc2 is partial (.part exists), doc3 unstarted
    assert!(receiver_dir.path().join("doc1.bin").exists());
    assert!(receiver_dir.path().join("doc2.bin.part").exists());
    assert!(!receiver_dir.path().join("doc3.bin").exists());

    // 1 file was completed on Path A
    assert_eq!(shared_progress.files_completed(), 1);

    // --- Phase 2: Create TransferContinuation and execute over Path B ---
    let continuation = TransferContinuation::from_interrupted(
        plan.clone(),
        shared_progress.clone(),
        shared_cancel.clone(),
        1, // 1 file completed
    );

    assert_eq!(continuation.remaining_files(), 2);
    assert!(!continuation.is_complete());

    let transport_receiver_b = TcpTransport::new();
    let mut listener_b = transport_receiver_b
        .listen("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let addr_path_b = listener_b.local_addr();

    let remote_id_spawn_b = remote_peer_id.clone();
    let receiver_dir_spawn_b = receiver_dir.path().to_path_buf();
    let receiver_progress_b = TransferProgress::new();

    let receiver_handle_b = tokio::spawn(async move {
        let (mut conn, _) = listener_b.accept().await.unwrap();
        if let Some(tcp_conn) = conn.as_any_mut().downcast_mut::<TcpConnection>() {
            tcp_conn.server_handshake(&remote_id_spawn_b).await.unwrap();
        }

        let mut session = Session::from_connection(conn, remote_id_spawn_b);
        let cancel = TransferCancellation::new();
        TransferManager::receive_collection_with_cancel_and_progress(
            &mut session,
            &receiver_dir_spawn_b,
            &cancel,
            &receiver_progress_b,
        )
        .await
        .unwrap();

        session.close().await.unwrap();
        receiver_progress_b
    });

    let mut session_sender_b = session_builder
        .connect(&remote_peer_id, addr_path_b)
        .await
        .unwrap();

    // Continue over Path B
    TransferManager::continue_collection(&mut session_sender_b, &continuation)
        .await
        .unwrap();

    session_sender_b.close().await.unwrap();
    let _ = receiver_handle_b.await.unwrap();

    // Verify all 3 files exist and are intact
    let rf1 = receiver_dir.path().join("doc1.bin");
    let rf2 = receiver_dir.path().join("doc2.bin");
    let rf3 = receiver_dir.path().join("doc3.bin");

    assert!(rf1.exists());
    assert!(rf2.exists());
    assert!(rf3.exists());

    assert_eq!(compute_hash(&fs::read(&rf1).await.unwrap()), h1);
    assert_eq!(compute_hash(&fs::read(&rf2).await.unwrap()), h2);
    assert_eq!(compute_hash(&fs::read(&rf3).await.unwrap()), h3);

    // Verify checkpoints are cleaned up
    assert!(!receiver_dir.path().join("doc2.bin.part").exists());
    assert!(!receiver_dir.path().join("doc2.bin.part.meta").exists());

    // Logical progress must reflect all 3 files completed and total bytes transferred
    assert_eq!(shared_progress.files_completed(), 3);
    assert_eq!(shared_progress.bytes_transferred(), total_bytes);
}

// --- S3.6: Autonomous Carrier Migration E2E Tests ---

#[tokio::test]
async fn test_e2e_carrier_autonomous_migration_single_file() {
    use flux_core::path::metrics::PathMetrics;
    use flux_core::path::{Path, PathRegistry, PathState, TransportKind};
    use flux_core::transfer::{TransferCancellation, TransferCarrier, TransferProgress};
    use std::sync::Arc;
    use std::time::Duration;

    let sender_dir = tempdir().unwrap();
    let receiver_dir = tempdir().unwrap();

    // 250 KiB file (~4 chunks: 3 full 64 KiB chunks + 1 partial)
    let file_path = sender_dir.path().join("migrated_single.bin");
    let payload_size = 250 * 1024;
    let original_payload = create_test_file(&file_path, payload_size).await;
    let expected_hash = compute_hash(&original_payload);

    let local_peer_id = PeerId::new();
    let remote_peer_id = PeerId::new();

    // Setup Path A Listener
    let transport_receiver_a = TcpTransport::new();
    let mut listener_a = transport_receiver_a
        .listen("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let addr_path_a = listener_a.local_addr();
    let remote_id_spawn_a = remote_peer_id.clone();
    let receiver_dir_spawn_a = receiver_dir.path().to_path_buf();

    // Setup Path B Listener
    let transport_receiver_b = TcpTransport::new();
    let mut listener_b = transport_receiver_b
        .listen("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let addr_path_b = listener_b.local_addr();
    let remote_id_spawn_b = remote_peer_id.clone();
    let receiver_dir_spawn_b = receiver_dir.path().to_path_buf();

    // Setup PathRegistry with both paths
    let registry = PathRegistry::new();

    let mut path_a = Path::new(remote_peer_id.clone(), TransportKind::Tcp, addr_path_a);
    path_a.state = PathState::Available;
    path_a.metrics = Some(PathMetrics::with_rtt(Duration::from_millis(10)));
    let path_a_id = path_a.id.clone();
    registry.register_path(path_a);

    let mut path_b = Path::new(remote_peer_id.clone(), TransportKind::Tcp, addr_path_b);
    path_b.state = PathState::Available;
    path_b.metrics = Some(PathMetrics::with_rtt(Duration::from_millis(25)));
    let path_b_id = path_b.id.clone();
    registry.register_path(path_b);

    // Receiver on Path A: receives 2 chunks, then drops connection abruptly
    let receiver_handle_a = tokio::spawn(async move {
        let (mut conn, _) = listener_a.accept().await.unwrap();
        if let Some(tcp_conn) = conn.as_any_mut().downcast_mut::<TcpConnection>() {
            tcp_conn.server_handshake(&remote_id_spawn_a).await.unwrap();
        }
        let mut session = Session::from_connection(conn, remote_id_spawn_a);
        let first_msg = session.recv_message().await.unwrap();
        if let FluxMessage::TransferRequest { metadata } = first_msg {
            let mut receiver = FileReceiver::new(metadata.clone(), &receiver_dir_spawn_a)
                .await
                .unwrap();
            session
                .send_message(&FluxMessage::TransferAccept {
                    transfer_id: metadata.transfer_id,
                })
                .await
                .unwrap();
            for _ in 0..2 {
                if let FluxMessage::TransferChunk { index, data, .. } =
                    session.recv_message().await.unwrap()
                {
                    receiver.write_chunk(index, &data).await.unwrap();
                }
            }
            // Abrupt carrier failure simulation
            drop(session);
        }
    });

    // Receiver on Path B: receives the remainder of the collection / resumed file
    let receiver_handle_b = tokio::spawn(async move {
        let (mut conn, _) = listener_b.accept().await.unwrap();
        if let Some(tcp_conn) = conn.as_any_mut().downcast_mut::<TcpConnection>() {
            tcp_conn.server_handshake(&remote_id_spawn_b).await.unwrap();
        }
        let mut session = Session::from_connection(conn, remote_id_spawn_b);
        TransferManager::receive_collection(&mut session, &receiver_dir_spawn_b)
            .await
            .unwrap();
        session.close().await.unwrap();
    });

    // Sender setup: establish initial session on Path A and wrap in TransferCarrier
    let transport_sender = Arc::new(TcpTransport::new());
    let session_builder = SessionBuilder::new(transport_sender.as_ref(), local_peer_id.clone());
    let session_sender_a = session_builder
        .connect(&remote_peer_id, addr_path_a)
        .await
        .unwrap();

    let mut carrier = TransferCarrier::new(
        remote_peer_id.clone(),
        path_a_id.clone(),
        session_sender_a,
        registry.clone(),
        transport_sender.clone(),
        local_peer_id.clone(),
    );

    let sender_cancel = TransferCancellation::new();
    let sender_progress = TransferProgress::new();

    // Send file using autonomous carrier migration
    TransferManager::send_file_with_carrier(
        &mut carrier,
        &file_path,
        &sender_cancel,
        &sender_progress,
    )
    .await
    .expect("Carrier migration must autonomously complete the transfer");

    let _ = receiver_handle_a.await;
    let _ = receiver_handle_b.await;

    // Verify Carrier ended in Completed state and migrated to Path B
    assert_eq!(
        carrier.state(),
        &flux_core::transfer::MigrationState::Completed
    );
    assert_eq!(carrier.current_path_id, path_b_id);
    assert_eq!(carrier.migration_count, 1);

    // Verify Path A is marked Unavailable in the registry
    let paths = registry.get_paths(&remote_peer_id).unwrap();
    assert_eq!(paths.get(&path_a_id).unwrap().state, PathState::Unavailable);

    // Verify final file integrity
    let final_file = receiver_dir.path().join("migrated_single.bin");
    assert!(final_file.exists(), "Final file must exist after migration");
    let received_data = fs::read(&final_file).await.unwrap();
    assert_eq!(received_data, original_payload);
    assert_eq!(compute_hash(&received_data), expected_hash);

    // Verify no stray partial files
    assert!(!receiver_dir
        .path()
        .join("migrated_single.bin.part")
        .exists());
    assert!(!receiver_dir
        .path()
        .join("migrated_single.bin.part.meta")
        .exists());

    // Verify progress matches total
    assert_eq!(sender_progress.bytes_transferred(), payload_size as u64);
    assert_eq!(sender_progress.files_completed(), 1);
}

#[tokio::test]
async fn test_e2e_carrier_autonomous_migration_multi_file() {
    use flux_core::path::metrics::PathMetrics;
    use flux_core::path::{Path, PathRegistry, PathState, TransportKind};
    use flux_core::transfer::{TransferCancellation, TransferCarrier, TransferProgress};
    use std::sync::Arc;
    use std::time::Duration;

    let sender_dir = tempdir().unwrap();
    let receiver_dir = tempdir().unwrap();

    let f1_path = sender_dir.path().join("file1.bin");
    let p1 = create_test_file(&f1_path, 40 * 1024).await;
    let h1 = compute_hash(&p1);

    let f2_path = sender_dir.path().join("file2.bin");
    let p2 = create_test_file(&f2_path, 200 * 1024).await;
    let h2 = compute_hash(&p2);

    let f3_path = sender_dir.path().join("file3.bin");
    let p3 = create_test_file(&f3_path, 80 * 1024).await;
    let h3 = compute_hash(&p3);

    let total_bytes = (p1.len() + p2.len() + p3.len()) as u64;
    let plan = TransferPlan::from_paths(&[f1_path, f2_path, f3_path]).unwrap();

    let local_peer_id = PeerId::new();
    let remote_peer_id = PeerId::new();

    // Setup Path A Listener
    let transport_receiver_a = TcpTransport::new();
    let mut listener_a = transport_receiver_a
        .listen("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let addr_path_a = listener_a.local_addr();
    let remote_id_spawn_a = remote_peer_id.clone();
    let receiver_dir_spawn_a = receiver_dir.path().to_path_buf();

    // Setup Path B Listener
    let transport_receiver_b = TcpTransport::new();
    let mut listener_b = transport_receiver_b
        .listen("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let addr_path_b = listener_b.local_addr();
    let remote_id_spawn_b = remote_peer_id.clone();
    let receiver_dir_spawn_b = receiver_dir.path().to_path_buf();

    // Setup PathRegistry
    let registry = PathRegistry::new();

    let mut path_a = Path::new(remote_peer_id.clone(), TransportKind::Tcp, addr_path_a);
    path_a.state = PathState::Available;
    path_a.metrics = Some(PathMetrics::with_rtt(Duration::from_millis(15)));
    let path_a_id = path_a.id.clone();
    registry.register_path(path_a);

    let mut path_b = Path::new(remote_peer_id.clone(), TransportKind::Tcp, addr_path_b);
    path_b.state = PathState::Available;
    path_b.metrics = Some(PathMetrics::with_rtt(Duration::from_millis(30)));
    let path_b_id = path_b.id.clone();
    registry.register_path(path_b);

    // Receiver on Path A: receives file 1 fully, 1 chunk of file 2, then drops
    let receiver_handle_a = tokio::spawn(async move {
        let (mut conn, _) = listener_a.accept().await.unwrap();
        if let Some(tcp_conn) = conn.as_any_mut().downcast_mut::<TcpConnection>() {
            tcp_conn.server_handshake(&remote_id_spawn_a).await.unwrap();
        }
        let mut session = Session::from_connection(conn, remote_id_spawn_a);

        // Receive file1.bin
        let msg1 = session.recv_message().await.unwrap();
        if let FluxMessage::TransferRequest { metadata } = msg1 {
            TransferManager::receive_transfer(&mut session, metadata, &receiver_dir_spawn_a)
                .await
                .unwrap();
        }

        // Receive 1 chunk of file2.bin
        let msg2 = session.recv_message().await.unwrap();
        if let FluxMessage::TransferRequest { metadata } = msg2 {
            let mut receiver = FileReceiver::new(metadata.clone(), &receiver_dir_spawn_a)
                .await
                .unwrap();
            session
                .send_message(&FluxMessage::TransferAccept {
                    transfer_id: metadata.transfer_id,
                })
                .await
                .unwrap();
            if let FluxMessage::TransferChunk { index, data, .. } =
                session.recv_message().await.unwrap()
            {
                receiver.write_chunk(index, &data).await.unwrap();
            }
            // Sudden connection drop
            drop(session);
        }
    });

    // Receiver on Path B: receives remainder of collection
    let receiver_handle_b = tokio::spawn(async move {
        let (mut conn, _) = listener_b.accept().await.unwrap();
        if let Some(tcp_conn) = conn.as_any_mut().downcast_mut::<TcpConnection>() {
            tcp_conn.server_handshake(&remote_id_spawn_b).await.unwrap();
        }
        let mut session = Session::from_connection(conn, remote_id_spawn_b);
        TransferManager::receive_collection(&mut session, &receiver_dir_spawn_b)
            .await
            .unwrap();
        session.close().await.unwrap();
    });

    // Sender setup
    let transport_sender = Arc::new(TcpTransport::new());
    let session_builder = SessionBuilder::new(transport_sender.as_ref(), local_peer_id.clone());
    let session_sender_a = session_builder
        .connect(&remote_peer_id, addr_path_a)
        .await
        .unwrap();

    let mut carrier = TransferCarrier::new(
        remote_peer_id.clone(),
        path_a_id.clone(),
        session_sender_a,
        registry.clone(),
        transport_sender.clone(),
        local_peer_id.clone(),
    );

    let sender_cancel = TransferCancellation::new();
    let sender_progress = TransferProgress::new();

    // Send collection with carrier
    TransferManager::send_collection_with_carrier(
        &mut carrier,
        &plan,
        &sender_cancel,
        &sender_progress,
    )
    .await
    .expect("Multi-file transfer should succeed after carrier migration");

    let _ = receiver_handle_a.await;
    let _ = receiver_handle_b.await;

    // Verify Carrier state
    assert_eq!(
        carrier.state(),
        &flux_core::transfer::MigrationState::Completed
    );
    assert_eq!(carrier.current_path_id, path_b_id);
    assert_eq!(carrier.migration_count, 1);

    // Verify all 3 files exist and are intact
    let rf1 = receiver_dir.path().join("file1.bin");
    let rf2 = receiver_dir.path().join("file2.bin");
    let rf3 = receiver_dir.path().join("file3.bin");
    assert!(rf1.exists());
    assert!(rf2.exists());
    assert!(rf3.exists());
    assert_eq!(compute_hash(&fs::read(&rf1).await.unwrap()), h1);
    assert_eq!(compute_hash(&fs::read(&rf2).await.unwrap()), h2);
    assert_eq!(compute_hash(&fs::read(&rf3).await.unwrap()), h3);

    // Verify progress tracking
    assert_eq!(sender_progress.files_completed(), 3);
    assert_eq!(sender_progress.bytes_transferred(), total_bytes);
}

#[tokio::test]
async fn test_e2e_carrier_migration_fails_when_no_backup_path() {
    use flux_core::path::metrics::PathMetrics;
    use flux_core::path::{Path, PathRegistry, PathState, TransportKind};
    use flux_core::transfer::{TransferCancellation, TransferCarrier, TransferProgress};
    use std::sync::Arc;
    use std::time::Duration;

    let sender_dir = tempdir().unwrap();
    let receiver_dir = tempdir().unwrap();

    let file_path = sender_dir.path().join("no_backup.bin");
    let payload = create_test_file(&file_path, 150 * 1024).await;
    let _expected_hash = compute_hash(&payload);

    let local_peer_id = PeerId::new();
    let remote_peer_id = PeerId::new();

    // Setup single path listener
    let transport_receiver = TcpTransport::new();
    let mut listener = transport_receiver
        .listen("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let addr_path = listener.local_addr();
    let remote_id_spawn = remote_peer_id.clone();
    let receiver_dir_spawn = receiver_dir.path().to_path_buf();

    // Registry with ONLY ONE path
    let registry = PathRegistry::new();
    let mut path = Path::new(remote_peer_id.clone(), TransportKind::Tcp, addr_path);
    path.state = PathState::Available;
    path.metrics = Some(PathMetrics::with_rtt(Duration::from_millis(10)));
    let path_id = path.id.clone();
    registry.register_path(path);

    // Receiver receives 1 chunk then drops
    let receiver_handle = tokio::spawn(async move {
        let (mut conn, _) = listener.accept().await.unwrap();
        if let Some(tcp_conn) = conn.as_any_mut().downcast_mut::<TcpConnection>() {
            tcp_conn.server_handshake(&remote_id_spawn).await.unwrap();
        }
        let mut session = Session::from_connection(conn, remote_id_spawn);
        let first_msg = session.recv_message().await.unwrap();
        if let FluxMessage::TransferRequest { metadata } = first_msg {
            let mut receiver = FileReceiver::new(metadata.clone(), &receiver_dir_spawn)
                .await
                .unwrap();
            session
                .send_message(&FluxMessage::TransferAccept {
                    transfer_id: metadata.transfer_id,
                })
                .await
                .unwrap();
            if let FluxMessage::TransferChunk { index, data, .. } =
                session.recv_message().await.unwrap()
            {
                receiver.write_chunk(index, &data).await.unwrap();
            }
            drop(session);
        }
    });

    let transport_sender = Arc::new(TcpTransport::new());
    let session_builder = SessionBuilder::new(transport_sender.as_ref(), local_peer_id.clone());
    let session_sender = session_builder
        .connect(&remote_peer_id, addr_path)
        .await
        .unwrap();

    let mut carrier = TransferCarrier::new(
        remote_peer_id.clone(),
        path_id.clone(),
        session_sender,
        registry.clone(),
        transport_sender.clone(),
        local_peer_id.clone(),
    );

    let sender_cancel = TransferCancellation::new();
    let sender_progress = TransferProgress::new();

    let res = TransferManager::send_file_with_carrier(
        &mut carrier,
        &file_path,
        &sender_cancel,
        &sender_progress,
    )
    .await;

    let _ = receiver_handle.await;

    // Must return an error and carrier state must be Paused
    assert!(res.is_err());
    assert!(matches!(
        carrier.state(),
        flux_core::transfer::MigrationState::Paused { .. }
    ));

    // Partial state must be preserved on disk
    let part_path = receiver_dir.path().join("no_backup.bin.part");
    let meta_path = receiver_dir.path().join("no_backup.bin.part.meta");
    assert!(part_path.exists(), "Partial .part file must be preserved");
    assert!(
        meta_path.exists(),
        "Partial .part.meta file must be preserved"
    );
}

// ============================================================================

// ============================================================================
// --- S3.8: Sender-Side Chunk Checkpointing & Efficient Migration Tests ---
// ============================================================================

#[tokio::test]
async fn test_s38_chunker_resume_efficiency() {
    // Proves that seeking to chunk N does not read chunks 0..N-1
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("efficient_seek.bin");
    let chunk_size = 64 * 1024;
    let total_chunks_count = 10;
    let payload_size = (total_chunks_count * chunk_size) as usize;
    let original = create_test_file(&file_path, payload_size).await;

    let mut chunker = Chunker::new(&file_path, chunk_size).await.unwrap();
    assert_eq!(chunker.total_chunks(), total_chunks_count);

    // Seek directly to chunk 6 (skipping 0..5)
    chunker.seek_to_chunk(6).await.unwrap();
    assert_eq!(chunker.bytes_read(), (6 * chunk_size) as u64);

    let (idx, data) = chunker.next_chunk().await.unwrap().unwrap();
    assert_eq!(idx, 6);
    assert_eq!(data.len(), chunk_size as usize);
    let expected_slice = &original[(6 * chunk_size as usize)..(7 * chunk_size as usize)];
    assert_eq!(&data[..], expected_slice);
}

#[tokio::test]
async fn test_s38_e2e_efficient_carrier_migration_chunk_boundary() {
    use flux_core::path::metrics::PathMetrics;
    use flux_core::path::{Path, PathRegistry, PathState, TransportKind};
    use flux_core::session::SessionBuilder;
    use flux_core::transfer::{
        MigrationState, TransferCancellation, TransferCarrier, TransferProgress,
    };
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    let sender_dir = tempdir().unwrap();
    let receiver_dir = tempdir().unwrap();

    // 16 chunks of 64 KiB = 1,048,576 bytes (1 MiB)
    let chunk_size = 64 * 1024;
    let total_chunks = 16u32;
    let file_size = (total_chunks * chunk_size) as usize;
    let file_path = sender_dir.path().join("chunk_efficient.bin");
    let original_payload = create_test_file(&file_path, file_size).await;
    let expected_hash = compute_hash(&original_payload);

    let local_peer_id = PeerId::new();
    let remote_peer_id = PeerId::new();

    // Setup Path A
    let transport_receiver_a = TcpTransport::new();
    let mut listener_a = transport_receiver_a
        .listen("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let addr_path_a = listener_a.local_addr();
    let remote_id_spawn_a = remote_peer_id.clone();
    let receiver_dir_spawn_a = receiver_dir.path().to_path_buf();

    // Setup Path B
    let transport_receiver_b = TcpTransport::new();
    let mut listener_b = transport_receiver_b
        .listen("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let addr_path_b = listener_b.local_addr();
    let remote_id_spawn_b = remote_peer_id.clone();
    let receiver_dir_spawn_b = receiver_dir.path().to_path_buf();

    // Register paths
    let registry = PathRegistry::new();
    let mut path_a = Path::new(remote_peer_id.clone(), TransportKind::Tcp, addr_path_a);
    path_a.state = PathState::Available;
    path_a.metrics = Some(PathMetrics::with_rtt(Duration::from_millis(10)));
    let path_a_id = path_a.id.clone();
    registry.register_path(path_a);

    let mut path_b = Path::new(remote_peer_id.clone(), TransportKind::Tcp, addr_path_b);
    path_b.state = PathState::Available;
    path_b.metrics = Some(PathMetrics::with_rtt(Duration::from_millis(30)));
    let path_b_id = path_b.id.clone();
    registry.register_path(path_b);

    let fail_at_chunk = 6u32; // Chunks 0..5 completed on Path A, chunk 6 causes failure
    let path_b_chunks_received = Arc::new(AtomicU32::new(0));
    let path_b_first_chunk = Arc::new(AtomicU32::new(9999));

    // Receiver on Path A: receives exactly `fail_at_chunk` (0..5), then abruptly drops connection
    let receiver_handle_a = tokio::spawn(async move {
        let (mut conn, _) = listener_a.accept().await.unwrap();
        if let Some(tcp_conn) = conn.as_any_mut().downcast_mut::<TcpConnection>() {
            tcp_conn.server_handshake(&remote_id_spawn_a).await.unwrap();
        }
        let mut session = Session::from_connection(conn, remote_id_spawn_a);
        let first_msg = session.recv_message().await.unwrap();
        if let FluxMessage::TransferRequest { metadata } = first_msg {
            let mut receiver = FileReceiver::new(metadata.clone(), &receiver_dir_spawn_a)
                .await
                .unwrap();
            session
                .send_message(&FluxMessage::TransferAccept {
                    transfer_id: metadata.transfer_id,
                })
                .await
                .unwrap();

            for _ in 0..fail_at_chunk {
                if let FluxMessage::TransferChunk { index, data, .. } =
                    session.recv_message().await.unwrap()
                {
                    receiver.write_chunk(index, &data).await.unwrap();
                }
            }
            // Abrupt carrier failure simulation
            drop(session);
        }
    });

    // Receiver on Path B: records the first chunk index received and counts total received chunks
    let p_b_count = path_b_chunks_received.clone();
    let p_b_first = path_b_first_chunk.clone();
    let receiver_dir_spawn_b_clone = receiver_dir_spawn_b.clone();
    let receiver_handle_b = tokio::spawn(async move {
        let (mut conn, _) = listener_b.accept().await.unwrap();
        if let Some(tcp_conn) = conn.as_any_mut().downcast_mut::<TcpConnection>() {
            tcp_conn.server_handshake(&remote_id_spawn_b).await.unwrap();
        }
        let mut session = Session::from_connection(conn, remote_id_spawn_b);

        let msg = session.recv_message().await.unwrap();
        if let FluxMessage::TransferRequest { metadata } = msg {
            let mut receiver =
                FileReceiver::try_resume(metadata.clone(), &receiver_dir_spawn_b_clone)
                    .await
                    .unwrap()
                    .expect("Receiver on Path B should find partial state on disk");

            let start_chunk = receiver.resume_from_chunk();
            assert_eq!(start_chunk, fail_at_chunk);
            p_b_first.store(start_chunk, Ordering::SeqCst);

            session
                .send_message(&FluxMessage::TransferResume {
                    transfer_id: metadata.transfer_id,
                    resume_from_chunk: start_chunk,
                })
                .await
                .unwrap();

            let remaining = metadata.total_chunks - start_chunk;
            for _ in 0..remaining {
                if let FluxMessage::TransferChunk { index, data, .. } =
                    session.recv_message().await.unwrap()
                {
                    receiver.write_chunk(index, &data).await.unwrap();
                    p_b_count.fetch_add(1, Ordering::SeqCst);
                }
            }

            // Expect Complete
            if let FluxMessage::TransferComplete { transfer_id } =
                session.recv_message().await.unwrap()
            {
                assert_eq!(transfer_id, metadata.transfer_id);
            }

            let final_path = receiver.finalize().await.unwrap();
            assert!(final_path.exists());

            session
                .send_message(&FluxMessage::TransferResult {
                    transfer_id: metadata.transfer_id,
                    success: true,
                    message: "Completed after migration".to_string(),
                })
                .await
                .unwrap();
        }

        // Collection handshake
        let goodbye = session.recv_message().await.unwrap();
        assert_eq!(goodbye, FluxMessage::Goodbye);
        session.close().await.unwrap();
    });

    // Sender Setup
    let sender_transport = Arc::new(TcpTransport::new());
    let session_builder = SessionBuilder::new(sender_transport.as_ref(), local_peer_id.clone());
    let initial_session = session_builder
        .connect(&remote_peer_id, addr_path_a)
        .await
        .unwrap();

    let mut carrier = TransferCarrier::new(
        remote_peer_id.clone(),
        path_a_id.clone(),
        initial_session,
        registry.clone(),
        sender_transport.clone(),
        local_peer_id.clone(),
    );

    let cancel = TransferCancellation::new();
    let progress = TransferProgress::new();

    // Execute transfer with carrier autonomous migration
    let transfer_result =
        TransferManager::send_file_with_carrier(&mut carrier, &file_path, &cancel, &progress).await;
    assert!(transfer_result.is_ok());

    receiver_handle_a.await.unwrap();
    receiver_handle_b.await.unwrap();

    // Verification Assertions
    assert_eq!(carrier.current_path_id, path_b_id);
    assert_eq!(carrier.migration_count, 1);
    assert_eq!(carrier.state(), &MigrationState::Completed);

    // Efficiency assertions:
    // Path B should have started exactly at chunk 6 (not chunk 0!)
    assert_eq!(path_b_first_chunk.load(Ordering::SeqCst), fail_at_chunk);
    // Path B should have received exactly 10 chunks (16 - 6), NOT all 16!
    assert_eq!(
        path_b_chunks_received.load(Ordering::SeqCst),
        total_chunks - fail_at_chunk
    );

    // Verify final file integrity
    let dest_file = receiver_dir.path().join("chunk_efficient.bin");
    assert!(dest_file.exists());
    let dest_bytes = tokio::fs::read(&dest_file).await.unwrap();
    assert_eq!(dest_bytes.len(), file_size);
    assert_eq!(compute_hash(&dest_bytes), expected_hash);
}

#[tokio::test]
async fn test_s38_e2e_multi_file_chunk_efficient_carrier_migration() {
    use flux_core::path::metrics::PathMetrics;
    use flux_core::path::{Path, PathRegistry, PathState, TransportKind};
    use flux_core::session::SessionBuilder;
    use flux_core::transfer::{
        MigrationState, TransferCancellation, TransferCarrier, TransferPlan, TransferProgress,
    };
    use std::sync::Arc;
    use std::time::Duration;

    let sender_dir = tempdir().unwrap();
    let receiver_dir = tempdir().unwrap();

    // 3 files:
    // file_0: 128 KiB (2 chunks)
    // file_1: 256 KiB (4 chunks) -> will fail after chunk 1 (2 chunks total transferred: 0 and 1)
    // file_2: 128 KiB (2 chunks)
    let file_0 = sender_dir.path().join("f0.bin");
    let file_1 = sender_dir.path().join("f1.bin");
    let file_2 = sender_dir.path().join("f2.bin");

    let p0 = create_test_file(&file_0, 128 * 1024).await;
    let p1 = create_test_file(&file_1, 256 * 1024).await;
    let p2 = create_test_file(&file_2, 128 * 1024).await;

    let h0 = compute_hash(&p0);
    let h1 = compute_hash(&p1);
    let h2 = compute_hash(&p2);

    let plan = TransferPlan::from_paths(&[file_0, file_1, file_2]).unwrap();

    let local_peer_id = PeerId::new();
    let remote_peer_id = PeerId::new();

    // Setup Path A Listener
    let transport_receiver_a = TcpTransport::new();
    let mut listener_a = transport_receiver_a
        .listen("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let addr_path_a = listener_a.local_addr();
    let remote_id_spawn_a = remote_peer_id.clone();
    let receiver_dir_spawn_a = receiver_dir.path().to_path_buf();

    // Setup Path B Listener
    let transport_receiver_b = TcpTransport::new();
    let mut listener_b = transport_receiver_b
        .listen("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let addr_path_b = listener_b.local_addr();
    let remote_id_spawn_b = remote_peer_id.clone();
    let receiver_dir_spawn_b = receiver_dir.path().to_path_buf();

    // Setup PathRegistry
    let registry = PathRegistry::new();
    let mut path_a = Path::new(remote_peer_id.clone(), TransportKind::Tcp, addr_path_a);
    path_a.state = PathState::Available;
    path_a.metrics = Some(PathMetrics::with_rtt(Duration::from_millis(10)));
    let path_a_id = path_a.id.clone();
    registry.register_path(path_a);

    let mut path_b = Path::new(remote_peer_id.clone(), TransportKind::Tcp, addr_path_b);
    path_b.state = PathState::Available;
    path_b.metrics = Some(PathMetrics::with_rtt(Duration::from_millis(20)));
    let path_b_id = path_b.id.clone();
    registry.register_path(path_b);

    // Receiver on Path A: receives file 0 completely, then receives 2 chunks of file 1, then drops connection
    let receiver_handle_a = tokio::spawn(async move {
        let (mut conn, _) = listener_a.accept().await.unwrap();
        if let Some(tcp_conn) = conn.as_any_mut().downcast_mut::<TcpConnection>() {
            tcp_conn.server_handshake(&remote_id_spawn_a).await.unwrap();
        }
        let mut session = Session::from_connection(conn, remote_id_spawn_a);

        // Receive File 0
        let msg0 = session.recv_message().await.unwrap();
        if let FluxMessage::TransferRequest { metadata } = msg0 {
            TransferManager::receive_transfer(&mut session, metadata, &receiver_dir_spawn_a)
                .await
                .unwrap();
        }

        // Receive File 1 partially (2 chunks) then drop
        let msg1 = session.recv_message().await.unwrap();
        if let FluxMessage::TransferRequest { metadata } = msg1 {
            let mut receiver = FileReceiver::new(metadata.clone(), &receiver_dir_spawn_a)
                .await
                .unwrap();
            session
                .send_message(&FluxMessage::TransferAccept {
                    transfer_id: metadata.transfer_id,
                })
                .await
                .unwrap();

            for _ in 0..2 {
                if let FluxMessage::TransferChunk { index, data, .. } =
                    session.recv_message().await.unwrap()
                {
                    receiver.write_chunk(index, &data).await.unwrap();
                }
            }

            // Connection drop
            drop(session);
        }
    });

    // Receiver on Path B: receives the remaining collection
    let receiver_handle_b = tokio::spawn(async move {
        let (mut conn, _) = listener_b.accept().await.unwrap();
        if let Some(tcp_conn) = conn.as_any_mut().downcast_mut::<TcpConnection>() {
            tcp_conn.server_handshake(&remote_id_spawn_b).await.unwrap();
        }
        let mut session = Session::from_connection(conn, remote_id_spawn_b);
        TransferManager::receive_collection(&mut session, &receiver_dir_spawn_b)
            .await
            .unwrap();
        session.close().await.unwrap();
    });

    // Sender setup
    let sender_transport = Arc::new(TcpTransport::new());
    let session_builder = SessionBuilder::new(sender_transport.as_ref(), local_peer_id.clone());
    let initial_session = session_builder
        .connect(&remote_peer_id, addr_path_a)
        .await
        .unwrap();

    let mut carrier = TransferCarrier::new(
        remote_peer_id.clone(),
        path_a_id.clone(),
        initial_session,
        registry.clone(),
        sender_transport.clone(),
        local_peer_id.clone(),
    );

    let cancel = TransferCancellation::new();
    let progress = TransferProgress::new();

    let res =
        TransferManager::send_collection_with_carrier(&mut carrier, &plan, &cancel, &progress)
            .await;
    assert!(res.is_ok());

    receiver_handle_a.await.unwrap();
    receiver_handle_b.await.unwrap();

    // Verify carrier completed migration
    assert_eq!(carrier.current_path_id, path_b_id);
    assert_eq!(carrier.migration_count, 1);
    assert_eq!(carrier.state(), &MigrationState::Completed);

    // Verify all 3 files exist and hashes match perfectly
    let out0 = receiver_dir.path().join("f0.bin");
    let out1 = receiver_dir.path().join("f1.bin");
    let out2 = receiver_dir.path().join("f2.bin");

    assert!(out0.exists());
    assert!(out1.exists());
    assert!(out2.exists());

    let b0 = tokio::fs::read(&out0).await.unwrap();
    let b1 = tokio::fs::read(&out1).await.unwrap();
    let b2 = tokio::fs::read(&out2).await.unwrap();

    assert_eq!(compute_hash(&b0), h0);
    assert_eq!(compute_hash(&b1), h1);
    assert_eq!(compute_hash(&b2), h2);
}

#[tokio::test]
async fn test_s38_cancellation_during_migration_checkpoint() {
    use flux_core::path::metrics::PathMetrics;
    use flux_core::path::{Path, PathRegistry, PathState, TransportKind};
    use flux_core::session::SessionBuilder;
    use flux_core::transfer::{
        TransferCancellation, TransferCarrier, TransferError, TransferProgress,
    };
    use std::sync::Arc;
    use std::time::Duration;

    let sender_dir = tempdir().unwrap();
    let receiver_dir = tempdir().unwrap();

    let file_path = sender_dir.path().join("cancel_chunk.bin");
    let payload = create_test_file(&file_path, 256 * 1024).await;
    let _ = compute_hash(&payload);

    let local_peer_id = PeerId::new();
    let remote_peer_id = PeerId::new();

    // Setup Path A
    let transport_receiver_a = TcpTransport::new();
    let mut listener_a = transport_receiver_a
        .listen("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let addr_path_a = listener_a.local_addr();
    let remote_id_spawn_a = remote_peer_id.clone();
    let receiver_dir_spawn_a = receiver_dir.path().to_path_buf();

    let registry = PathRegistry::new();
    let mut path_a = Path::new(remote_peer_id.clone(), TransportKind::Tcp, addr_path_a);
    path_a.state = PathState::Available;
    path_a.metrics = Some(PathMetrics::with_rtt(Duration::from_millis(10)));
    let path_a_id = path_a.id.clone();
    registry.register_path(path_a);

    let cancel = TransferCancellation::new();
    let cancel_clone = cancel.clone();

    // Receiver on Path A: receives 1 chunk, triggers cancellation token locally, then drops
    let receiver_handle_a = tokio::spawn(async move {
        let (mut conn, _) = listener_a.accept().await.unwrap();
        if let Some(tcp_conn) = conn.as_any_mut().downcast_mut::<TcpConnection>() {
            tcp_conn.server_handshake(&remote_id_spawn_a).await.unwrap();
        }
        let mut session = Session::from_connection(conn, remote_id_spawn_a);
        let first_msg = session.recv_message().await.unwrap();
        if let FluxMessage::TransferRequest { metadata } = first_msg {
            let mut receiver = FileReceiver::new(metadata.clone(), &receiver_dir_spawn_a)
                .await
                .unwrap();
            session
                .send_message(&FluxMessage::TransferAccept {
                    transfer_id: metadata.transfer_id,
                })
                .await
                .unwrap();

            if let FluxMessage::TransferChunk { index, data, .. } =
                session.recv_message().await.unwrap()
            {
                receiver.write_chunk(index, &data).await.unwrap();
                // Cancel sender midway
                cancel_clone.cancel();
            }
        }
    });

    let sender_transport = Arc::new(TcpTransport::new());
    let session_builder = SessionBuilder::new(sender_transport.as_ref(), local_peer_id.clone());
    let initial_session = session_builder
        .connect(&remote_peer_id, addr_path_a)
        .await
        .unwrap();

    let mut carrier = TransferCarrier::new(
        remote_peer_id.clone(),
        path_a_id.clone(),
        initial_session,
        registry.clone(),
        sender_transport.clone(),
        local_peer_id.clone(),
    );

    let progress = TransferProgress::new();
    let result =
        TransferManager::send_file_with_carrier(&mut carrier, &file_path, &cancel, &progress).await;

    assert!(matches!(result, Err(TransferError::Cancelled)));
    receiver_handle_a.await.unwrap();
}

#[tokio::test]
async fn test_s38_failure_before_any_chunk_migrates_cleanly() {
    use flux_core::path::metrics::PathMetrics;
    use flux_core::path::{Path, PathRegistry, PathState, TransportKind};
    use flux_core::session::SessionBuilder;
    use flux_core::transfer::{
        MigrationState, TransferCancellation, TransferCarrier, TransferProgress,
    };
    use std::sync::Arc;
    use std::time::Duration;

    let sender_dir = tempdir().unwrap();
    let receiver_dir = tempdir().unwrap();

    let file_path = sender_dir.path().join("zero_chunk_fail.bin");
    let payload = create_test_file(&file_path, 128 * 1024).await;
    let expected_hash = compute_hash(&payload);

    let local_peer_id = PeerId::new();
    let remote_peer_id = PeerId::new();

    // Setup Path A (fails immediately after handshake, before accepting any chunk)
    let transport_receiver_a = TcpTransport::new();
    let mut listener_a = transport_receiver_a
        .listen("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let addr_path_a = listener_a.local_addr();
    let remote_id_spawn_a = remote_peer_id.clone();

    // Setup Path B
    let transport_receiver_b = TcpTransport::new();
    let mut listener_b = transport_receiver_b
        .listen("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let addr_path_b = listener_b.local_addr();
    let remote_id_spawn_b = remote_peer_id.clone();
    let receiver_dir_spawn_b = receiver_dir.path().to_path_buf();

    let registry = PathRegistry::new();
    let mut path_a = Path::new(remote_peer_id.clone(), TransportKind::Tcp, addr_path_a);
    path_a.state = PathState::Available;
    path_a.metrics = Some(PathMetrics::with_rtt(Duration::from_millis(10)));
    let path_a_id = path_a.id.clone();
    registry.register_path(path_a);

    let mut path_b = Path::new(remote_peer_id.clone(), TransportKind::Tcp, addr_path_b);
    path_b.state = PathState::Available;
    path_b.metrics = Some(PathMetrics::with_rtt(Duration::from_millis(25)));
    let path_b_id = path_b.id.clone();
    registry.register_path(path_b);

    // Receiver A: closes connection immediately upon TransferRequest
    let receiver_handle_a = tokio::spawn(async move {
        let (mut conn, _) = listener_a.accept().await.unwrap();
        if let Some(tcp_conn) = conn.as_any_mut().downcast_mut::<TcpConnection>() {
            tcp_conn.server_handshake(&remote_id_spawn_a).await.unwrap();
        }
        let mut session = Session::from_connection(conn, remote_id_spawn_a);
        let _ = session.recv_message().await;
        // Drop before sending Accept/Chunks
        drop(session);
    });

    // Receiver B: receives full transfer
    let receiver_handle_b = tokio::spawn(async move {
        let (mut conn, _) = listener_b.accept().await.unwrap();
        if let Some(tcp_conn) = conn.as_any_mut().downcast_mut::<TcpConnection>() {
            tcp_conn.server_handshake(&remote_id_spawn_b).await.unwrap();
        }
        let mut session = Session::from_connection(conn, remote_id_spawn_b);
        TransferManager::receive_collection(&mut session, &receiver_dir_spawn_b)
            .await
            .unwrap();
        session.close().await.unwrap();
    });

    let sender_transport = Arc::new(TcpTransport::new());
    let session_builder = SessionBuilder::new(sender_transport.as_ref(), local_peer_id.clone());
    let initial_session = session_builder
        .connect(&remote_peer_id, addr_path_a)
        .await
        .unwrap();

    let mut carrier = TransferCarrier::new(
        remote_peer_id.clone(),
        path_a_id.clone(),
        initial_session,
        registry.clone(),
        sender_transport.clone(),
        local_peer_id.clone(),
    );

    let cancel = TransferCancellation::new();
    let progress = TransferProgress::new();

    let res =
        TransferManager::send_file_with_carrier(&mut carrier, &file_path, &cancel, &progress).await;
    assert!(res.is_ok());

    receiver_handle_a.await.unwrap();
    receiver_handle_b.await.unwrap();

    assert_eq!(carrier.current_path_id, path_b_id);
    assert_eq!(carrier.migration_count, 1);
    assert_eq!(carrier.state(), &MigrationState::Completed);

    let dest_file = receiver_dir.path().join("zero_chunk_fail.bin");
    assert!(dest_file.exists());
    let dest_bytes = tokio::fs::read(&dest_file).await.unwrap();
    assert_eq!(compute_hash(&dest_bytes), expected_hash);
}

#[tokio::test]
async fn test_s39_e2e_double_migration_cascade() {
    use flux_core::path::metrics::PathMetrics;
    use flux_core::path::{Path, PathRegistry, PathState, TransportKind};
    use flux_core::transfer::{TransferCancellation, TransferCarrier, TransferProgress};
    use std::sync::Arc;
    use std::time::Duration;

    let sender_dir = tempdir().unwrap();
    let receiver_dir = tempdir().unwrap();

    // 300 KiB file (~5 chunks of 64 KiB)
    let file_path = sender_dir.path().join("cascade_double.bin");
    let payload_size = 300 * 1024;
    let original_payload = create_test_file(&file_path, payload_size).await;
    let expected_hash = compute_hash(&original_payload);

    let local_peer_id = PeerId::new();
    let remote_peer_id = PeerId::new();

    // Setup Path A Listener (Fails after 1 chunk)
    let transport_receiver_a = TcpTransport::new();
    let mut listener_a = transport_receiver_a
        .listen("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let addr_path_a = listener_a.local_addr();
    let remote_id_spawn_a = remote_peer_id.clone();
    let receiver_dir_spawn_a = receiver_dir.path().to_path_buf();

    // Setup Path B Listener (Fails after 2 more chunks)
    let transport_receiver_b = TcpTransport::new();
    let mut listener_b = transport_receiver_b
        .listen("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let addr_path_b = listener_b.local_addr();
    let remote_id_spawn_b = remote_peer_id.clone();
    let receiver_dir_spawn_b = receiver_dir.path().to_path_buf();

    // Setup Path C Listener (Completes the transfer)
    let transport_receiver_c = TcpTransport::new();
    let mut listener_c = transport_receiver_c
        .listen("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let addr_path_c = listener_c.local_addr();
    let remote_id_spawn_c = remote_peer_id.clone();
    let receiver_dir_spawn_c = receiver_dir.path().to_path_buf();

    // Register all 3 paths:
    // Path B has 50ms RTT -> selected 1st after Path A fails
    // Path C has 100ms RTT -> selected 2nd after Path B fails
    let registry = PathRegistry::new();

    let mut path_a = Path::new(remote_peer_id.clone(), TransportKind::Tcp, addr_path_a);
    path_a.state = PathState::Available;
    path_a.metrics = Some(PathMetrics::with_rtt(Duration::from_millis(150)));
    let path_a_id = path_a.id.clone();
    registry.register_path(path_a);

    let mut path_b = Path::new(remote_peer_id.clone(), TransportKind::Tcp, addr_path_b);
    path_b.state = PathState::Available;
    path_b.metrics = Some(PathMetrics::with_rtt(Duration::from_millis(50))); // Best RTT -> Selected 1st
    let path_b_id = path_b.id.clone();
    registry.register_path(path_b);

    let mut path_c = Path::new(remote_peer_id.clone(), TransportKind::Tcp, addr_path_c);
    path_c.state = PathState::Available;
    path_c.metrics = Some(PathMetrics::with_rtt(Duration::from_millis(100))); // Selected 2nd
    let path_c_id = path_c.id.clone();
    registry.register_path(path_c);

    // Receiver A Spawns: writes chunk 0, then kills carrier
    let receiver_handle_a = tokio::spawn(async move {
        let (mut conn, _) = listener_a.accept().await.unwrap();
        if let Some(tcp_conn) = conn.as_any_mut().downcast_mut::<TcpConnection>() {
            tcp_conn.server_handshake(&remote_id_spawn_a).await.unwrap();
        }
        let mut session = Session::from_connection(conn, remote_id_spawn_a);
        let request_msg = session.recv_message().await.unwrap();
        if let FluxMessage::TransferRequest { metadata } = request_msg {
            let mut receiver = FileReceiver::new(metadata.clone(), &receiver_dir_spawn_a)
                .await
                .unwrap();
            session
                .send_message(&FluxMessage::TransferAccept {
                    transfer_id: metadata.transfer_id,
                })
                .await
                .unwrap();
            // Receive exactly 1 chunk
            if let FluxMessage::TransferChunk { index, data, .. } =
                session.recv_message().await.unwrap()
            {
                receiver.write_chunk(index, &data).await.unwrap();
            }
            drop(session); // Simulate path A failure
        }
    });

    // Receiver B Spawns: resumes from chunk 1, writes up to chunk 3, then drops
    let receiver_handle_b = tokio::spawn(async move {
        let (mut conn, _) = listener_b.accept().await.unwrap();
        if let Some(tcp_conn) = conn.as_any_mut().downcast_mut::<TcpConnection>() {
            tcp_conn.server_handshake(&remote_id_spawn_b).await.unwrap();
        }
        let mut session = Session::from_connection(conn, remote_id_spawn_b);
        let request_msg = session.recv_message().await.unwrap();
        if let FluxMessage::TransferRequest { metadata } = request_msg {
            // S3.8 Resume handshake
            let mut receiver = FileReceiver::try_resume(metadata.clone(), &receiver_dir_spawn_b)
                .await
                .unwrap()
                .expect("Receiver B should find partial file state from Receiver A");
            let resume_chunk = receiver.resume_from_chunk();
            session
                .send_message(&FluxMessage::TransferResume {
                    transfer_id: metadata.transfer_id,
                    resume_from_chunk: resume_chunk,
                })
                .await
                .unwrap();

            // Write 2 chunks and fail
            for _ in 0..2 {
                if let FluxMessage::TransferChunk { index, data, .. } =
                    session.recv_message().await.unwrap()
                {
                    receiver.write_chunk(index, &data).await.unwrap();
                }
            }
            drop(session); // Simulate path B failure
        }
    });

    // Receiver C Spawns: resumes from chunk 3 and runs to completion
    let receiver_handle_c = tokio::spawn(async move {
        let (mut conn, _) = listener_c.accept().await.unwrap();
        if let Some(tcp_conn) = conn.as_any_mut().downcast_mut::<TcpConnection>() {
            tcp_conn.server_handshake(&remote_id_spawn_c).await.unwrap();
        }
        let mut session = Session::from_connection(conn, remote_id_spawn_c);
        TransferManager::receive_collection(&mut session, &receiver_dir_spawn_c)
            .await
            .unwrap();
        session.close().await.unwrap();
    });

    // Sender Setup
    let transport_sender = Arc::new(TcpTransport::new());
    let session_builder = SessionBuilder::new(transport_sender.as_ref(), local_peer_id.clone());
    let session_sender_initial = session_builder
        .connect(&remote_peer_id, addr_path_a)
        .await
        .unwrap();

    let mut carrier = TransferCarrier::new(
        remote_peer_id.clone(),
        path_a_id.clone(),
        session_sender_initial,
        registry.clone(),
        transport_sender,
        local_peer_id,
    );

    let sender_cancel = TransferCancellation::new();
    let sender_progress = TransferProgress::new();

    TransferManager::send_file_with_carrier(
        &mut carrier,
        &file_path,
        &sender_cancel,
        &sender_progress,
    )
    .await
    .expect("Double sequential migration cascade must complete successfully");

    // Await spawned handles
    let _ = receiver_handle_a.await;
    let _ = receiver_handle_b.await;
    let _ = receiver_handle_c.await;

    // Verify carrier is successfully completed and on Path C
    assert_eq!(
        carrier.state(),
        &flux_core::transfer::MigrationState::Completed
    );
    assert_eq!(carrier.current_path_id, path_c_id);
    assert_eq!(carrier.migration_count, 2);

    // Verify paths state
    let paths = registry.get_paths(&remote_peer_id).unwrap();
    assert_eq!(paths.get(&path_a_id).unwrap().state, PathState::Unavailable);
    assert_eq!(paths.get(&path_b_id).unwrap().state, PathState::Unavailable);
    assert_eq!(paths.get(&path_c_id).unwrap().state, PathState::Available);

    // Confirm file content integrity
    let final_file = receiver_dir.path().join("cascade_double.bin");
    assert!(final_file.exists());
    let received_data = fs::read(&final_file).await.unwrap();
    assert_eq!(received_data, original_payload);
    assert_eq!(compute_hash(&received_data), expected_hash);
}

#[tokio::test]
async fn test_s39_e2e_connection_fallback_cascade() {
    use flux_core::path::metrics::PathMetrics;
    use flux_core::path::{Path, PathRegistry, PathState, TransportKind};
    use flux_core::transfer::{TransferCancellation, TransferCarrier, TransferProgress};
    use std::sync::Arc;
    use std::time::Duration;

    let sender_dir = tempdir().unwrap();
    let receiver_dir = tempdir().unwrap();

    let file_path = sender_dir.path().join("cascade_connection_fail.bin");
    let payload_size = 120 * 1024;
    let original_payload = create_test_file(&file_path, payload_size).await;
    let expected_hash = compute_hash(&original_payload);

    let local_peer_id = PeerId::new();
    let remote_peer_id = PeerId::new();

    // Setup Path A Listener (Drops after handshake/accepting request)
    let transport_receiver_a = TcpTransport::new();
    let mut listener_a = transport_receiver_a
        .listen("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let addr_path_a = listener_a.local_addr();
    let remote_id_spawn_a = remote_peer_id.clone();
    let receiver_dir_spawn_a = receiver_dir.path().to_path_buf();

    // Path B Listener (We will NEVER accept connection on this port to simulate socket refusal/failure)
    let transport_receiver_b = TcpTransport::new();
    let listener_b = transport_receiver_b
        .listen("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let addr_path_b = listener_b.local_addr();
    // Drop listener_b immediately to guarantee connection failure
    drop(listener_b);

    // Setup Path C Listener (Completes the transfer successfully)
    let transport_receiver_c = TcpTransport::new();
    let mut listener_c = transport_receiver_c
        .listen("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let addr_path_c = listener_c.local_addr();
    let remote_id_spawn_c = remote_peer_id.clone();
    let receiver_dir_spawn_c = receiver_dir.path().to_path_buf();

    // Register paths:
    // Path B has 10ms RTT -> selected 1st, fails connection
    // Path C has 50ms RTT -> selected 2nd, succeeds
    let registry = PathRegistry::new();

    let mut path_a = Path::new(remote_peer_id.clone(), TransportKind::Tcp, addr_path_a);
    path_a.state = PathState::Available;
    path_a.metrics = Some(PathMetrics::with_rtt(Duration::from_millis(150)));
    let path_a_id = path_a.id.clone();
    registry.register_path(path_a);

    let mut path_b = Path::new(remote_peer_id.clone(), TransportKind::Tcp, addr_path_b);
    path_b.state = PathState::Available;
    path_b.metrics = Some(PathMetrics::with_rtt(Duration::from_millis(10))); // Best RTT -> Selected 1st
    let path_b_id = path_b.id.clone();
    registry.register_path(path_b);

    let mut path_c = Path::new(remote_peer_id.clone(), TransportKind::Tcp, addr_path_c);
    path_c.state = PathState::Available;
    path_c.metrics = Some(PathMetrics::with_rtt(Duration::from_millis(50))); // Alternative RTT -> Selected 2nd
    let path_c_id = path_c.id.clone();
    registry.register_path(path_c);

    // Receiver A: Accepts request, then closes socket without writing anything
    let receiver_handle_a = tokio::spawn(async move {
        let (mut conn, _) = listener_a.accept().await.unwrap();
        if let Some(tcp_conn) = conn.as_any_mut().downcast_mut::<TcpConnection>() {
            tcp_conn.server_handshake(&remote_id_spawn_a).await.unwrap();
        }
        let mut session = Session::from_connection(conn, remote_id_spawn_a);
        let request_msg = session.recv_message().await.unwrap();
        if let FluxMessage::TransferRequest { metadata } = request_msg {
            let _receiver = FileReceiver::new(metadata.clone(), &receiver_dir_spawn_a)
                .await
                .unwrap();
            session
                .send_message(&FluxMessage::TransferAccept {
                    transfer_id: metadata.transfer_id,
                })
                .await
                .unwrap();
            drop(session); // Immediate fail
        }
    });

    // Receiver C: Handles the transfer to completion
    let receiver_handle_c = tokio::spawn(async move {
        let (mut conn, _) = listener_c.accept().await.unwrap();
        if let Some(tcp_conn) = conn.as_any_mut().downcast_mut::<TcpConnection>() {
            tcp_conn.server_handshake(&remote_id_spawn_c).await.unwrap();
        }
        let mut session = Session::from_connection(conn, remote_id_spawn_c);
        TransferManager::receive_collection(&mut session, &receiver_dir_spawn_c)
            .await
            .unwrap();
        session.close().await.unwrap();
    });

    // Sender setup
    let transport_sender = Arc::new(TcpTransport::new());
    let session_builder = SessionBuilder::new(transport_sender.as_ref(), local_peer_id.clone());
    let session_sender_initial = session_builder
        .connect(&remote_peer_id, addr_path_a)
        .await
        .unwrap();

    let mut carrier = TransferCarrier::new(
        remote_peer_id.clone(),
        path_a_id.clone(),
        session_sender_initial,
        registry.clone(),
        transport_sender,
        local_peer_id,
    );

    let sender_cancel = TransferCancellation::new();
    let sender_progress = TransferProgress::new();

    // Trigger migration: Path A fails -> picks Path B -> fails to connect -> falls back to Path C!
    TransferManager::send_file_with_carrier(
        &mut carrier,
        &file_path,
        &sender_cancel,
        &sender_progress,
    )
    .await
    .expect(
        "Connection fallback migration must autonomously bypass dead Path B and succeed on Path C",
    );

    let _ = receiver_handle_a.await;
    let _ = receiver_handle_c.await;

    // Check states
    assert_eq!(
        carrier.state(),
        &flux_core::transfer::MigrationState::Completed
    );
    assert_eq!(carrier.current_path_id, path_c_id);
    assert_eq!(carrier.migration_count, 1);

    // Confirm paths
    let paths = registry.get_paths(&remote_peer_id).unwrap();
    assert_eq!(paths.get(&path_a_id).unwrap().state, PathState::Unavailable);
    assert_eq!(paths.get(&path_b_id).unwrap().state, PathState::Unavailable);
    assert_eq!(paths.get(&path_c_id).unwrap().state, PathState::Available);

    let final_file = receiver_dir.path().join("cascade_connection_fail.bin");
    assert!(final_file.exists());
    let received_data = fs::read(&final_file).await.unwrap();
    assert_eq!(received_data, original_payload);
    assert_eq!(compute_hash(&received_data), expected_hash);
}

#[tokio::test]
async fn test_s39_e2e_triple_migration_cascade() {
    use flux_core::path::metrics::PathMetrics;
    use flux_core::path::{Path, PathRegistry, PathState, TransportKind};
    use flux_core::transfer::{TransferCancellation, TransferCarrier, TransferProgress};
    use std::sync::Arc;
    use std::time::Duration;

    let sender_dir = tempdir().unwrap();
    let receiver_dir = tempdir().unwrap();

    // 400 KiB file (~7 chunks of 64 KiB)
    let file_path = sender_dir.path().join("cascade_triple.bin");
    let payload_size = 400 * 1024;
    let original_payload = create_test_file(&file_path, payload_size).await;
    let expected_hash = compute_hash(&original_payload);

    let local_peer_id = PeerId::new();
    let remote_peer_id = PeerId::new();

    // Setup Path A Listener (Initial path, fails after 1 chunk)
    let transport_receiver_a = TcpTransport::new();
    let mut listener_a = transport_receiver_a
        .listen("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let addr_path_a = listener_a.local_addr();
    let remote_id_spawn_a = remote_peer_id.clone();
    let receiver_dir_spawn_a = receiver_dir.path().to_path_buf();

    // Setup Path B Listener (Fails after 1 more chunk: chunk 1)
    let transport_receiver_b = TcpTransport::new();
    let mut listener_b = transport_receiver_b
        .listen("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let addr_path_b = listener_b.local_addr();
    let remote_id_spawn_b = remote_peer_id.clone();
    let receiver_dir_spawn_b = receiver_dir.path().to_path_buf();

    // Setup Path C Listener (Fails after 2 more chunks: chunk 2, 3)
    let transport_receiver_c = TcpTransport::new();
    let mut listener_c = transport_receiver_c
        .listen("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let addr_path_c = listener_c.local_addr();
    let remote_id_spawn_c = remote_peer_id.clone();
    let receiver_dir_spawn_c = receiver_dir.path().to_path_buf();

    // Setup Path D Listener (Runs to completion)
    let transport_receiver_d = TcpTransport::new();
    let mut listener_d = transport_receiver_d
        .listen("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let addr_path_d = listener_d.local_addr();
    let remote_id_spawn_d = remote_peer_id.clone();
    let receiver_dir_spawn_d = receiver_dir.path().to_path_buf();

    // Register all 4 paths with RTT values to control deterministic selection order:
    // Path A: 200ms (Initial)
    // Path B: 30ms  (Selected 1st when A fails)
    // Path C: 60ms  (Selected 2nd when B fails)
    // Path D: 90ms  (Selected 3rd when C fails)
    let registry = PathRegistry::new();

    let mut path_a = Path::new(remote_peer_id.clone(), TransportKind::Tcp, addr_path_a);
    path_a.state = PathState::Available;
    path_a.metrics = Some(PathMetrics::with_rtt(Duration::from_millis(200)));
    let path_a_id = path_a.id.clone();
    registry.register_path(path_a);

    let mut path_b = Path::new(remote_peer_id.clone(), TransportKind::Tcp, addr_path_b);
    path_b.state = PathState::Available;
    path_b.metrics = Some(PathMetrics::with_rtt(Duration::from_millis(30)));
    let path_b_id = path_b.id.clone();
    registry.register_path(path_b);

    let mut path_c = Path::new(remote_peer_id.clone(), TransportKind::Tcp, addr_path_c);
    path_c.state = PathState::Available;
    path_c.metrics = Some(PathMetrics::with_rtt(Duration::from_millis(60)));
    let path_c_id = path_c.id.clone();
    registry.register_path(path_c);

    let mut path_d = Path::new(remote_peer_id.clone(), TransportKind::Tcp, addr_path_d);
    path_d.state = PathState::Available;
    path_d.metrics = Some(PathMetrics::with_rtt(Duration::from_millis(90)));
    let path_d_id = path_d.id.clone();
    registry.register_path(path_d);

    // Receiver A: chunk 0 -> drop
    let receiver_handle_a = tokio::spawn(async move {
        let (mut conn, _) = listener_a.accept().await.unwrap();
        if let Some(tcp_conn) = conn.as_any_mut().downcast_mut::<TcpConnection>() {
            tcp_conn.server_handshake(&remote_id_spawn_a).await.unwrap();
        }
        let mut session = Session::from_connection(conn, remote_id_spawn_a);
        let request_msg = session.recv_message().await.unwrap();
        if let FluxMessage::TransferRequest { metadata } = request_msg {
            let mut receiver = FileReceiver::new(metadata.clone(), &receiver_dir_spawn_a)
                .await
                .unwrap();
            session
                .send_message(&FluxMessage::TransferAccept {
                    transfer_id: metadata.transfer_id,
                })
                .await
                .unwrap();
            if let FluxMessage::TransferChunk { index, data, .. } =
                session.recv_message().await.unwrap()
            {
                receiver.write_chunk(index, &data).await.unwrap();
            }
            drop(session);
        }
    });

    // Receiver B: resume chunk 1 -> write chunk 1 -> drop
    let receiver_handle_b = tokio::spawn(async move {
        let (mut conn, _) = listener_b.accept().await.unwrap();
        if let Some(tcp_conn) = conn.as_any_mut().downcast_mut::<TcpConnection>() {
            tcp_conn.server_handshake(&remote_id_spawn_b).await.unwrap();
        }
        let mut session = Session::from_connection(conn, remote_id_spawn_b);
        let request_msg = session.recv_message().await.unwrap();
        if let FluxMessage::TransferRequest { metadata } = request_msg {
            let mut receiver = FileReceiver::try_resume(metadata.clone(), &receiver_dir_spawn_b)
                .await
                .unwrap()
                .expect("Receiver B should find partial file state");
            let resume_chunk = receiver.resume_from_chunk();
            session
                .send_message(&FluxMessage::TransferResume {
                    transfer_id: metadata.transfer_id,
                    resume_from_chunk: resume_chunk,
                })
                .await
                .unwrap();

            if let FluxMessage::TransferChunk { index, data, .. } =
                session.recv_message().await.unwrap()
            {
                receiver.write_chunk(index, &data).await.unwrap();
            }
            drop(session);
        }
    });

    // Receiver C: resume chunk 2 -> write chunk 2, 3 -> drop
    let receiver_handle_c = tokio::spawn(async move {
        let (mut conn, _) = listener_c.accept().await.unwrap();
        if let Some(tcp_conn) = conn.as_any_mut().downcast_mut::<TcpConnection>() {
            tcp_conn.server_handshake(&remote_id_spawn_c).await.unwrap();
        }
        let mut session = Session::from_connection(conn, remote_id_spawn_c);
        let request_msg = session.recv_message().await.unwrap();
        if let FluxMessage::TransferRequest { metadata } = request_msg {
            let mut receiver = FileReceiver::try_resume(metadata.clone(), &receiver_dir_spawn_c)
                .await
                .unwrap()
                .expect("Receiver C should find partial file state");
            let resume_chunk = receiver.resume_from_chunk();
            session
                .send_message(&FluxMessage::TransferResume {
                    transfer_id: metadata.transfer_id,
                    resume_from_chunk: resume_chunk,
                })
                .await
                .unwrap();

            for _ in 0..2 {
                if let FluxMessage::TransferChunk { index, data, .. } =
                    session.recv_message().await.unwrap()
                {
                    receiver.write_chunk(index, &data).await.unwrap();
                }
            }
            drop(session);
        }
    });

    // Receiver D: complete transfer
    let receiver_handle_d = tokio::spawn(async move {
        let (mut conn, _) = listener_d.accept().await.unwrap();
        if let Some(tcp_conn) = conn.as_any_mut().downcast_mut::<TcpConnection>() {
            tcp_conn.server_handshake(&remote_id_spawn_d).await.unwrap();
        }
        let mut session = Session::from_connection(conn, remote_id_spawn_d);
        TransferManager::receive_collection(&mut session, &receiver_dir_spawn_d)
            .await
            .unwrap();
        session.close().await.unwrap();
    });

    // Sender setup
    let transport_sender = Arc::new(TcpTransport::new());
    let session_builder = SessionBuilder::new(transport_sender.as_ref(), local_peer_id.clone());
    let session_sender_initial = session_builder
        .connect(&remote_peer_id, addr_path_a)
        .await
        .unwrap();

    let mut carrier = TransferCarrier::new(
        remote_peer_id.clone(),
        path_a_id.clone(),
        session_sender_initial,
        registry.clone(),
        transport_sender,
        local_peer_id,
    );

    let sender_cancel = TransferCancellation::new();
    let sender_progress = TransferProgress::new();

    // Run the triple-cascading migration: A -> B -> C -> D
    TransferManager::send_file_with_carrier(
        &mut carrier,
        &file_path,
        &sender_cancel,
        &sender_progress,
    )
    .await
    .expect("Triple sequential migration cascade (A -> B -> C -> D) must complete successfully");

    let _ = receiver_handle_a.await;
    let _ = receiver_handle_b.await;
    let _ = receiver_handle_c.await;
    let _ = receiver_handle_d.await;

    // Verify Carrier completed on Path D after 3 migrations
    assert_eq!(
        carrier.state(),
        &flux_core::transfer::MigrationState::Completed
    );
    assert_eq!(carrier.current_path_id, path_d_id);
    assert_eq!(carrier.migration_count, 3);

    // Verify all 3 previous paths marked Unavailable and Path D remains Available
    let paths = registry.get_paths(&remote_peer_id).unwrap();
    assert_eq!(paths.get(&path_a_id).unwrap().state, PathState::Unavailable);
    assert_eq!(paths.get(&path_b_id).unwrap().state, PathState::Unavailable);
    assert_eq!(paths.get(&path_c_id).unwrap().state, PathState::Unavailable);
    assert_eq!(paths.get(&path_d_id).unwrap().state, PathState::Available);

    // Verify integrity
    let final_file = receiver_dir.path().join("cascade_triple.bin");
    assert!(final_file.exists());
    let received_data = fs::read(&final_file).await.unwrap();
    assert_eq!(received_data, original_payload);
    assert_eq!(compute_hash(&received_data), expected_hash);
}

#[tokio::test]
async fn test_s39_e2e_multi_file_cascading_migration() {
    use flux_core::path::metrics::PathMetrics;
    use flux_core::path::{Path, PathRegistry, PathState, TransportKind};
    use flux_core::transfer::{
        TransferCancellation, TransferCarrier, TransferPlan, TransferProgress,
    };
    use std::sync::Arc;
    use std::time::Duration;

    let sender_dir = tempdir().unwrap();
    let receiver_dir = tempdir().unwrap();

    let f1_path = sender_dir.path().join("file1.bin");
    let f2_path = sender_dir.path().join("file2.bin");
    let f1_data = create_test_file(&f1_path, 150 * 1024).await;
    let f2_data = create_test_file(&f2_path, 200 * 1024).await;

    let plan = TransferPlan::from_paths(&[f1_path.clone(), f2_path.clone()]).unwrap();
    assert_eq!(plan.items.len(), 2);

    let local_peer_id = PeerId::new();
    let remote_peer_id = PeerId::new();

    // Setup Path A Listener (Fails during File 1)
    let transport_receiver_a = TcpTransport::new();
    let mut listener_a = transport_receiver_a
        .listen("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let addr_path_a = listener_a.local_addr();
    let remote_id_spawn_a = remote_peer_id.clone();
    let receiver_dir_spawn_a = receiver_dir.path().to_path_buf();

    // Setup Path B Listener (Completes File 1, fails during File 2)
    let transport_receiver_b = TcpTransport::new();
    let mut listener_b = transport_receiver_b
        .listen("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let addr_path_b = listener_b.local_addr();
    let remote_id_spawn_b = remote_peer_id.clone();
    let receiver_dir_spawn_b = receiver_dir.path().to_path_buf();

    // Setup Path C Listener (Completes File 2 to full collection completion)
    let transport_receiver_c = TcpTransport::new();
    let mut listener_c = transport_receiver_c
        .listen("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let addr_path_c = listener_c.local_addr();
    let remote_id_spawn_c = remote_peer_id.clone();
    let receiver_dir_spawn_c = receiver_dir.path().to_path_buf();

    let registry = PathRegistry::new();

    let mut path_a = Path::new(remote_peer_id.clone(), TransportKind::Tcp, addr_path_a);
    path_a.state = PathState::Available;
    path_a.metrics = Some(PathMetrics::with_rtt(Duration::from_millis(150)));
    let path_a_id = path_a.id.clone();
    registry.register_path(path_a);

    let mut path_b = Path::new(remote_peer_id.clone(), TransportKind::Tcp, addr_path_b);
    path_b.state = PathState::Available;
    path_b.metrics = Some(PathMetrics::with_rtt(Duration::from_millis(30)));
    let _path_b_id = path_b.id.clone();
    registry.register_path(path_b);

    let mut path_c = Path::new(remote_peer_id.clone(), TransportKind::Tcp, addr_path_c);
    path_c.state = PathState::Available;
    path_c.metrics = Some(PathMetrics::with_rtt(Duration::from_millis(80)));
    let path_c_id = path_c.id.clone();
    registry.register_path(path_c);

    // Receiver A: receives 1 chunk of File 1, then drops
    let receiver_handle_a = tokio::spawn(async move {
        let (mut conn, _) = listener_a.accept().await.unwrap();
        if let Some(tcp_conn) = conn.as_any_mut().downcast_mut::<TcpConnection>() {
            tcp_conn.server_handshake(&remote_id_spawn_a).await.unwrap();
        }
        let mut session = Session::from_connection(conn, remote_id_spawn_a);
        if let FluxMessage::TransferRequest { metadata } = session.recv_message().await.unwrap() {
            let mut receiver = FileReceiver::new(metadata.clone(), &receiver_dir_spawn_a)
                .await
                .unwrap();
            session
                .send_message(&FluxMessage::TransferAccept {
                    transfer_id: metadata.transfer_id,
                })
                .await
                .unwrap();
            if let FluxMessage::TransferChunk { index, data, .. } =
                session.recv_message().await.unwrap()
            {
                receiver.write_chunk(index, &data).await.unwrap();
            }
            drop(session);
        }
    });

    // Receiver B: finishes File 1, accepts File 2, receives 1 chunk of File 2, then drops
    let receiver_handle_b = tokio::spawn(async move {
        let (mut conn, _) = listener_b.accept().await.unwrap();
        if let Some(tcp_conn) = conn.as_any_mut().downcast_mut::<TcpConnection>() {
            tcp_conn.server_handshake(&remote_id_spawn_b).await.unwrap();
        }
        let mut session = Session::from_connection(conn, remote_id_spawn_b);

        // File 1 Resume & finish
        if let FluxMessage::TransferRequest { metadata } = session.recv_message().await.unwrap() {
            let mut receiver = FileReceiver::try_resume(metadata.clone(), &receiver_dir_spawn_b)
                .await
                .unwrap()
                .expect("Receiver B should find partial File 1 state");
            let total_chunks = metadata.total_chunks;
            let resume_chunk = receiver.resume_from_chunk();
            session
                .send_message(&FluxMessage::TransferResume {
                    transfer_id: metadata.transfer_id,
                    resume_from_chunk: resume_chunk,
                })
                .await
                .unwrap();

            let mut chunks_written = resume_chunk;
            while let FluxMessage::TransferChunk { index, data, .. } =
                session.recv_message().await.unwrap()
            {
                receiver.write_chunk(index, &data).await.unwrap();
                chunks_written += 1;
                if chunks_written >= total_chunks {
                    let _ = receiver.finalize().await.unwrap();
                    break;
                }
            }
            // Wait for TransferComplete and reply with TransferResult
            if let FluxMessage::TransferComplete { .. } = session.recv_message().await.unwrap() {
                session
                    .send_message(&FluxMessage::TransferResult {
                        transfer_id: metadata.transfer_id,
                        success: true,
                        message: "File 1 verified".to_string(),
                    })
                    .await
                    .unwrap();
            }
        }

        // File 2: receive 1 chunk and drop session
        if let FluxMessage::TransferRequest { metadata } = session.recv_message().await.unwrap() {
            let mut receiver = FileReceiver::new(metadata.clone(), &receiver_dir_spawn_b)
                .await
                .unwrap();
            session
                .send_message(&FluxMessage::TransferAccept {
                    transfer_id: metadata.transfer_id,
                })
                .await
                .unwrap();
            if let FluxMessage::TransferChunk { index, data, .. } =
                session.recv_message().await.unwrap()
            {
                receiver.write_chunk(index, &data).await.unwrap();
            }
            drop(session);
        }
    });

    // Receiver C: resumes collection and completes all remaining files
    let receiver_handle_c = tokio::spawn(async move {
        let (mut conn, _) = listener_c.accept().await.unwrap();
        if let Some(tcp_conn) = conn.as_any_mut().downcast_mut::<TcpConnection>() {
            tcp_conn.server_handshake(&remote_id_spawn_c).await.unwrap();
        }
        let mut session = Session::from_connection(conn, remote_id_spawn_c);
        TransferManager::receive_collection(&mut session, &receiver_dir_spawn_c)
            .await
            .unwrap();
        session.close().await.unwrap();
    });

    // Sender setup
    let transport_sender = Arc::new(TcpTransport::new());
    let session_builder = SessionBuilder::new(transport_sender.as_ref(), local_peer_id.clone());
    let session_sender_initial = session_builder
        .connect(&remote_peer_id, addr_path_a)
        .await
        .unwrap();

    let mut carrier = TransferCarrier::new(
        remote_peer_id.clone(),
        path_a_id.clone(),
        session_sender_initial,
        registry.clone(),
        transport_sender,
        local_peer_id,
    );

    let sender_cancel = TransferCancellation::new();
    let sender_progress = TransferProgress::new();

    TransferManager::send_collection_with_carrier(
        &mut carrier,
        &plan,
        &sender_cancel,
        &sender_progress,
    )
    .await
    .expect("Multi-file cascading migration must complete successfully");

    let _ = receiver_handle_a.await;
    let _ = receiver_handle_b.await;
    let _ = receiver_handle_c.await;

    // Verify carrier state
    assert_eq!(
        carrier.state(),
        &flux_core::transfer::MigrationState::Completed
    );
    assert_eq!(carrier.current_path_id, path_c_id);
    assert_eq!(carrier.migration_count, 2);

    // Verify all files match source hash
    let out_f1 = receiver_dir.path().join("file1.bin");
    let out_f2 = receiver_dir.path().join("file2.bin");
    assert_eq!(fs::read(&out_f1).await.unwrap(), f1_data);
    assert_eq!(fs::read(&out_f2).await.unwrap(), f2_data);
}

#[tokio::test]
async fn test_s39_cancellation_during_cascading_candidate_evaluation() {
    use flux_core::path::metrics::PathMetrics;
    use flux_core::path::{Path, PathRegistry, PathState, TransportKind};
    use flux_core::transfer::{
        TransferCancellation, TransferCarrier, TransferError, TransferProgress,
    };
    use std::sync::Arc;
    use std::time::Duration;

    let sender_dir = tempdir().unwrap();
    let receiver_dir = tempdir().unwrap();

    let file_path = sender_dir.path().join("cancel_cascade.bin");
    create_test_file(&file_path, 200 * 1024).await;

    let local_peer_id = PeerId::new();
    let remote_peer_id = PeerId::new();

    let transport_receiver = TcpTransport::new();
    let mut listener_a = transport_receiver
        .listen("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let addr_path_a = listener_a.local_addr();
    let remote_id_spawn_a = remote_peer_id.clone();
    let receiver_dir_spawn_a = receiver_dir.path().to_path_buf();

    let registry = PathRegistry::new();

    let mut path_a = Path::new(remote_peer_id.clone(), TransportKind::Tcp, addr_path_a);
    path_a.state = PathState::Available;
    path_a.metrics = Some(PathMetrics::with_rtt(Duration::from_millis(150)));
    let path_a_id = path_a.id.clone();
    registry.register_path(path_a);

    // Provide 2 backup paths
    let mut path_b = Path::new(
        remote_peer_id.clone(),
        TransportKind::Tcp,
        "127.0.0.1:9201".parse().unwrap(),
    );
    path_b.state = PathState::Available;
    path_b.metrics = Some(PathMetrics::with_rtt(Duration::from_millis(50)));
    registry.register_path(path_b);

    let mut path_c = Path::new(
        remote_peer_id.clone(),
        TransportKind::Tcp,
        "127.0.0.1:9202".parse().unwrap(),
    );
    path_c.state = PathState::Available;
    path_c.metrics = Some(PathMetrics::with_rtt(Duration::from_millis(100)));
    registry.register_path(path_c);

    let sender_cancel = TransferCancellation::new();
    let cancel_for_receiver = sender_cancel.clone();

    // Receiver on Path A: receives 1 chunk, cancels the token, and drops connection
    let receiver_handle = tokio::spawn(async move {
        let (mut conn, _) = listener_a.accept().await.unwrap();
        if let Some(tcp_conn) = conn.as_any_mut().downcast_mut::<TcpConnection>() {
            tcp_conn.server_handshake(&remote_id_spawn_a).await.unwrap();
        }
        let mut session = Session::from_connection(conn, remote_id_spawn_a);
        if let FluxMessage::TransferRequest { metadata } = session.recv_message().await.unwrap() {
            let mut receiver = FileReceiver::new(metadata.clone(), &receiver_dir_spawn_a)
                .await
                .unwrap();
            session
                .send_message(&FluxMessage::TransferAccept {
                    transfer_id: metadata.transfer_id,
                })
                .await
                .unwrap();
            if let FluxMessage::TransferChunk { index, data, .. } =
                session.recv_message().await.unwrap()
            {
                receiver.write_chunk(index, &data).await.unwrap();
            }
            // Trigger cancellation before connection break
            cancel_for_receiver.cancel();
            drop(session);
        }
    });

    let transport_sender = Arc::new(TcpTransport::new());
    let session_builder = SessionBuilder::new(transport_sender.as_ref(), local_peer_id.clone());
    let session_sender_initial = session_builder
        .connect(&remote_peer_id, addr_path_a)
        .await
        .unwrap();

    let mut carrier = TransferCarrier::new(
        remote_peer_id.clone(),
        path_a_id,
        session_sender_initial,
        registry,
        transport_sender,
        local_peer_id,
    );

    let sender_progress = TransferProgress::new();

    let res = TransferManager::send_file_with_carrier(
        &mut carrier,
        &file_path,
        &sender_cancel,
        &sender_progress,
    )
    .await;

    let _ = receiver_handle.await;

    // Must return TransferError::Cancelled and NOT perform migration attempts!
    assert!(matches!(res, Err(TransferError::Cancelled)));
    assert_eq!(
        carrier.migration_count, 0,
        "No migration should have occurred after cancellation"
    );
}
