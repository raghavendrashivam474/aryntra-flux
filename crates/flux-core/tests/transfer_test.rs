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
