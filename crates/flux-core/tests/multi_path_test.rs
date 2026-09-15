use flux_core::identity::PeerId;
use flux_core::path::{Path, PathRegistry, PathState, TransportKind};
use flux_core::session::SessionBuilder;
use flux_core::transport::TcpTransport;
use flux_core::transport::Transport;
use std::net::SocketAddr;
use tokio::time::Duration;

#[tokio::test]
async fn test_multi_path_discovery_and_verification() {
    // 1. Setup two peers
    let peer_a = PeerId::new();
    let peer_b = PeerId::new();

    // 2. Setup path registries
    let registry_a = PathRegistry::new();

    // 3. Simulate discovering multiple paths to Peer B
    let addr_b1: SocketAddr = "127.0.0.1:9091".parse().unwrap();
    let addr_b2: SocketAddr = "127.0.0.1:9092".parse().unwrap();

    let path1 = Path::new(peer_b.clone(), TransportKind::Tcp, addr_b1);
    let path2 = Path::new(peer_b.clone(), TransportKind::Tcp, addr_b2);

    let path1_id = path1.id.clone();
    let path2_id = path2.id.clone();

    registry_a.register_path(path1);
    registry_a.register_path(path2);

    // Verify both are registered
    let paths = registry_a.get_paths(&peer_b).expect("Should find paths");
    assert_eq!(paths.count(), 2);
    assert_eq!(paths.available().len(), 0, "No paths verified yet");

    // 4. Start actual listeners for both candidate ports to simulate Node B running on multi-path interfaces
    let transport_b = TcpTransport::default();
    let listener_b1 = transport_b.listen(addr_b1).await.expect("Listen 1 failed");
    let listener_b2 = transport_b.listen(addr_b2).await.expect("Listen 2 failed");

    // Spawn accepting tasks for Node B
    // Handshake expects Node B's local peer ID (peer_b), which the client (peer_a) will receive and verify.
    let p_b_for_l1 = peer_b.clone();
    tokio::spawn(async move {
        let mut l1 = listener_b1;
        if let Ok((mut conn, _)) = l1.accept().await {
            let tcp_conn = conn
                .as_any_mut()
                .downcast_mut::<flux_core::transport::TcpConnection>()
                .expect("Should be TCP connection");
            let _ = tcp_conn.server_handshake(&p_b_for_l1).await;
        }
    });

    let p_b_for_l2 = peer_b.clone();
    tokio::spawn(async move {
        let mut l2 = listener_b2;
        if let Ok((mut conn, _)) = l2.accept().await {
            let tcp_conn = conn
                .as_any_mut()
                .downcast_mut::<flux_core::transport::TcpConnection>()
                .expect("Should be TCP connection");
            let _ = tcp_conn.server_handshake(&p_b_for_l2).await;
        }
    });

    // 5. Node A verifies paths by establishing sessions
    let transport_a = TcpTransport::default();
    let builder_a = SessionBuilder::new(&transport_a, peer_a.clone());

    // Retrieve paths from registry to select them
    let active_paths = registry_a.get_paths(&peer_b).unwrap();
    let p1_record = active_paths.get(&path1_id).unwrap();
    let p2_record = active_paths.get(&path2_id).unwrap();

    // Verify Path 1
    registry_a.set_path_state(&peer_b, &path1_id, PathState::Connecting);
    match tokio::time::timeout(
        Duration::from_secs(1),
        builder_a.connect(&peer_b, p1_record.remote_addr),
    )
    .await
    {
        Ok(Ok(session)) => {
            assert_eq!(session.peer_id(), Some(&peer_b));
            registry_a.set_path_state(&peer_b, &path1_id, PathState::Available);
            let _ = session.close().await;
        }
        _ => {
            registry_a.set_path_state(&peer_b, &path1_id, PathState::Unavailable);
        }
    }

    // Verify Path 2
    registry_a.set_path_state(&peer_b, &path2_id, PathState::Connecting);
    match tokio::time::timeout(
        Duration::from_secs(1),
        builder_a.connect(&peer_b, p2_record.remote_addr),
    )
    .await
    {
        Ok(Ok(session)) => {
            assert_eq!(session.peer_id(), Some(&peer_b));
            registry_a.set_path_state(&peer_b, &path2_id, PathState::Available);
            let _ = session.close().await;
        }
        _ => {
            registry_a.set_path_state(&peer_b, &path2_id, PathState::Unavailable);
        }
    }

    // 6. Check final registry states
    let final_paths = registry_a.get_paths(&peer_b).unwrap();
    assert_eq!(
        final_paths.available().len(),
        2,
        "Both paths should now be verified as Available"
    );

    let path_1_final = final_paths.get(&path1_id).unwrap();
    assert_eq!(path_1_final.state, PathState::Available);

    let path_2_final = final_paths.get(&path2_id).unwrap();
    assert_eq!(path_2_final.state, PathState::Available);
}
