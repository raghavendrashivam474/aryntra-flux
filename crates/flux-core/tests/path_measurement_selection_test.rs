use flux_core::identity::PeerId;
use flux_core::path::{Path, PathProber, PathRegistry, PathSelector, PathState, TransportKind};
use flux_core::protocol::FluxMessage;
use flux_core::session::SessionBuilder;
use flux_core::transport::traits::Connection;
use flux_core::transport::{TcpTransport, Transport};
use std::net::SocketAddr;
use std::time::Duration;

#[tokio::test]
async fn test_path_measurement_and_deterministic_auto_selection() {
    // 1. Setup Node A (prober/selector client) and Node B (multi-interface server)
    let peer_a = PeerId::new();
    let peer_b = PeerId::new();

    let registry_a = PathRegistry::new();

    // 2. Register candidate paths for Node B (Path 1 and Path 2 are live, Path 3 is unreachable)
    let addr_b1: SocketAddr = "127.0.0.1:9281".parse().unwrap();
    let addr_b2: SocketAddr = "127.0.0.1:9282".parse().unwrap();
    let addr_b3: SocketAddr = "127.0.0.1:9283".parse().unwrap(); // No listener

    let path1 = Path::new(peer_b.clone(), TransportKind::Tcp, addr_b1);
    let path2 = Path::new(peer_b.clone(), TransportKind::Tcp, addr_b2);
    let path3 = Path::new(peer_b.clone(), TransportKind::Tcp, addr_b3);

    let path1_id = path1.id.clone();
    let path2_id = path2.id.clone();
    let path3_id = path3.id.clone();

    registry_a.register_path(path1);
    registry_a.register_path(path2);
    registry_a.register_path(path3);

    // Verify initial registered state
    let initial_paths = registry_a
        .get_paths(&peer_b)
        .expect("Paths must be registered");
    assert_eq!(initial_paths.count(), 3);
    assert_eq!(initial_paths.available().len(), 0);

    // 3. Start listeners for Node B's active interfaces
    // Interface 1 responds fast
    let transport_b = TcpTransport::default();
    let listener_b1 = transport_b.listen(addr_b1).await.expect("Listen b1 failed");
    let p_b1 = peer_b.clone();

    tokio::spawn(async move {
        let mut l = listener_b1;
        while let Ok((mut conn, _)) = l.accept().await {
            let tcp_conn = conn
                .as_any_mut()
                .downcast_mut::<flux_core::transport::TcpConnection>()
                .unwrap();
            if tcp_conn.server_handshake(&p_b1).await.is_ok() {
                while let Ok(msg) = tcp_conn.recv_message().await {
                    if let FluxMessage::Ping { sequence, payload } = msg {
                        let _ = tcp_conn
                            .send_message(&FluxMessage::pong(sequence, payload))
                            .await;
                    }
                }
            }
        }
    });

    // Interface 2 responds with artificial delay
    let listener_b2 = transport_b.listen(addr_b2).await.expect("Listen b2 failed");
    let p_b2 = peer_b.clone();

    tokio::spawn(async move {
        let mut l = listener_b2;
        while let Ok((mut conn, _)) = l.accept().await {
            let tcp_conn = conn
                .as_any_mut()
                .downcast_mut::<flux_core::transport::TcpConnection>()
                .unwrap();
            if tcp_conn.server_handshake(&p_b2).await.is_ok() {
                while let Ok(msg) = tcp_conn.recv_message().await {
                    if let FluxMessage::Ping { sequence, payload } = msg {
                        // Artificial delay on Path 2
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        let _ = tcp_conn
                            .send_message(&FluxMessage::pong(sequence, payload))
                            .await;
                    }
                }
            }
        }
    });

    // 4. Node A probes all candidate paths using PathProber
    let transport_a = TcpTransport::default();
    let prober = PathProber::new(&transport_a, peer_a.clone(), registry_a.clone())
        .with_timeout(Duration::from_millis(300));

    let probe_results = prober.probe_peer_paths(&peer_b).await;
    assert_eq!(probe_results.len(), 3);

    // 5. Verify states and metrics in PathRegistry
    let measured_paths = registry_a.get_paths(&peer_b).unwrap();
    let p1_record = measured_paths.get(&path1_id).unwrap();
    let p2_record = measured_paths.get(&path2_id).unwrap();
    let p3_record = measured_paths.get(&path3_id).unwrap();

    assert_eq!(p1_record.state, PathState::Available);
    assert!(p1_record.metrics.is_some());

    assert_eq!(p2_record.state, PathState::Available);
    assert!(p2_record.metrics.is_some());

    assert_eq!(p3_record.state, PathState::Unavailable);
    assert!(p3_record.metrics.is_none());

    let rtt1 = p1_record.metrics.unwrap().rtt_ms.unwrap();
    let rtt2 = p2_record.metrics.unwrap().rtt_ms.unwrap();
    assert!(
        rtt1 < rtt2,
        "Path 1 ({rtt1}ms) should have lower RTT than delayed Path 2 ({rtt2}ms)"
    );

    // 6. PathSelector deterministically selects the optimal path (Path 1)
    let selector = PathSelector::new(registry_a.clone());
    let selected_path = selector
        .select_path(&peer_b)
        .expect("Should select an optimal path");

    assert_eq!(selected_path.id, path1_id);
    assert_eq!(selected_path.remote_addr, addr_b1);

    // 7. Establish transfer-ready session on the selected optimal path
    let builder_a = SessionBuilder::new(&transport_a, peer_a.clone());
    let session = builder_a
        .connect(&peer_b, selected_path.remote_addr)
        .await
        .expect("Session connect on selected path must succeed");

    assert_eq!(session.peer_id(), Some(&peer_b));
    assert_eq!(session.remote_addr(), addr_b1);

    let _ = session.close().await;
}
