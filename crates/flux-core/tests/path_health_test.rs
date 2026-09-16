use flux_core::identity::PeerId;
use flux_core::path::{
    Path, PathHealthConfig, PathHealthMonitor, PathId, PathProber, PathRegistry, PathSelector,
    PathState, TransportKind,
};
use flux_core::protocol::FluxMessage;
use flux_core::transport::traits::Connection;
use flux_core::transport::{TcpTransport, Transport};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[tokio::test]
async fn test_concurrent_probing_timing() {
    let peer_a = PeerId::new();
    let peer_b = PeerId::new();
    let registry = PathRegistry::new();

    let addr1: SocketAddr = "127.0.0.1:9301".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:9302".parse().unwrap();
    let addr3: SocketAddr = "127.0.0.1:9303".parse().unwrap();

    registry.register_path(Path::new(peer_b.clone(), TransportKind::Tcp, addr1));
    registry.register_path(Path::new(peer_b.clone(), TransportKind::Tcp, addr2));
    registry.register_path(Path::new(peer_b.clone(), TransportKind::Tcp, addr3));

    let transport_b = TcpTransport::default();
    let p_b = peer_b.clone();

    // Spawn 3 listeners each introducing an artificial 100ms delay
    for addr in [addr1, addr2, addr3] {
        let listener = transport_b.listen(addr).await.unwrap();
        let p = p_b.clone();
        tokio::spawn(async move {
            let mut l = listener;
            while let Ok((mut conn, _)) = l.accept().await {
                let tcp_conn = conn
                    .as_any_mut()
                    .downcast_mut::<flux_core::transport::TcpConnection>()
                    .unwrap();
                if tcp_conn.server_handshake(&p).await.is_ok() {
                    while let Ok(msg) = tcp_conn.recv_message().await {
                        if let FluxMessage::Ping { sequence, payload } = msg {
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            let _ = tcp_conn
                                .send_message(&FluxMessage::pong(sequence, payload))
                                .await;
                        }
                    }
                }
            }
        });
    }

    let transport_a = TcpTransport::default();
    let prober = PathProber::new(&transport_a, peer_a, registry.clone())
        .with_timeout(Duration::from_secs(1));

    let t0 = Instant::now();
    let results = prober.probe_all_concurrent().await;
    let elapsed = t0.elapsed();

    assert_eq!(results.len(), 3);
    for (_, res) in results {
        assert!(res.is_ok(), "All delayed probes should succeed");
    }

    assert!(
        elapsed < Duration::from_millis(250),
        "Concurrent probe took {:?}, expected < 250ms (sequential would be >= 300ms)",
        elapsed
    );
}

#[tokio::test]
async fn test_path_health_lifecycle_failure_and_recovery() {
    let peer_a = PeerId::new();
    let peer_b = PeerId::new();
    let registry = PathRegistry::new();

    let addr1: SocketAddr = "127.0.0.1:9311".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:9312".parse().unwrap();
    let addr3: SocketAddr = "127.0.0.1:9313".parse().unwrap(); // dead port

    let p1 = Path::new(peer_b.clone(), TransportKind::Tcp, addr1);
    let p2 = Path::new(peer_b.clone(), TransportKind::Tcp, addr2);
    let p3 = Path::new(peer_b.clone(), TransportKind::Tcp, addr3);

    let id1 = p1.id.clone();
    let id2 = p2.id.clone();
    let id3 = p3.id.clone();

    registry.register_path(p1);
    registry.register_path(p2);
    registry.register_path(p3);

    let transport_b = Arc::new(TcpTransport::default());
    let p_b = peer_b.clone();

    // Path 1 responder: fast (0ms delay), switchable via atomic flag
    let p1_active = Arc::new(AtomicBool::new(true));
    let p1_active_clone = p1_active.clone();
    let transport_b1 = transport_b.clone();
    let p_b1 = p_b.clone();

    tokio::spawn(async move {
        let listener = transport_b1.listen(addr1).await.unwrap();
        let mut l = listener;
        while let Ok((mut conn, _)) = l.accept().await {
            if !p1_active_clone.load(Ordering::SeqCst) {
                drop(conn);
                continue;
            }
            let tcp_conn = conn
                .as_any_mut()
                .downcast_mut::<flux_core::transport::TcpConnection>()
                .unwrap();
            if tcp_conn.server_handshake(&p_b1).await.is_ok() {
                while let Ok(msg) = tcp_conn.recv_message().await {
                    if !p1_active_clone.load(Ordering::SeqCst) {
                        break;
                    }
                    if let FluxMessage::Ping { sequence, payload } = msg {
                        let _ = tcp_conn
                            .send_message(&FluxMessage::pong(sequence, payload))
                            .await;
                    }
                }
            }
        }
    });

    // Path 2 responder: always live, with 20ms artificial latency
    let listener_b2 = transport_b.listen(addr2).await.unwrap();
    let p_b2 = p_b.clone();
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
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        let _ = tcp_conn
                            .send_message(&FluxMessage::pong(sequence, payload))
                            .await;
                    }
                }
            }
        }
    });

    // Start PathHealthMonitor with generous timeout
    let config = PathHealthConfig {
        probe_interval: Duration::from_millis(60),
        probe_timeout: Duration::from_millis(200),
        stale_after: Duration::from_secs(2),
    };

    let transport_a = Arc::new(TcpTransport::default());
    let mut monitor = PathHealthMonitor::new(
        transport_a.clone(),
        peer_a.clone(),
        registry.clone(),
        config,
    );
    monitor.start();

    // Polling helpers
    let wait_for_state = |target_id: PathId, target_state: PathState| {
        let reg = registry.clone();
        let p_b = peer_b.clone();
        async move {
            let start = Instant::now();
            while start.elapsed() < Duration::from_secs(3) {
                if let Some(paths) = reg.get_paths(&p_b) {
                    if let Some(p) = paths.get(&target_id) {
                        if p.state == target_state {
                            return true;
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(15)).await;
            }
            false
        }
    };

    let selector = PathSelector::new(registry.clone());
    let wait_for_selected = |target_id: PathId| {
        let sel = selector.clone();
        let p_b = peer_b.clone();
        async move {
            let start = Instant::now();
            while start.elapsed() < Duration::from_secs(3) {
                if let Some(selected) = sel.select_path(&p_b) {
                    if selected.id == target_id {
                        return true;
                    }
                }
                tokio::time::sleep(Duration::from_millis(15)).await;
            }
            false
        }
    };

    // 1. Initial State: Wait for Path 1 and 2 to become Available
    assert!(
        wait_for_state(id1.clone(), PathState::Available).await,
        "Path 1 should become Available"
    );
    assert!(
        wait_for_state(id2.clone(), PathState::Available).await,
        "Path 2 should become Available"
    );

    // Path 3 is dead: it must never become Available
    let paths_snap = registry.get_paths(&peer_b).unwrap();
    let p3_state = paths_snap.get(&id3).unwrap().state;
    assert_ne!(
        p3_state,
        PathState::Available,
        "Unreachable Path 3 must never be Available"
    );

    // Path 1 should be selected (lower latency than Path 2)
    assert!(
        wait_for_selected(id1.clone()).await,
        "Selector should select lowest-RTT Path 1"
    );

    // 2. Failure Simulation: Disable Path 1
    p1_active.store(false, Ordering::SeqCst);

    // Wait for monitor to detect failure and mark Path 1 as Unavailable
    assert!(
        wait_for_state(id1.clone(), PathState::Unavailable).await,
        "Path 1 should become Unavailable after failure"
    );

    let paths_after_fail = registry.get_paths(&peer_b).unwrap();
    let p1_rec = paths_after_fail.get(&id1).unwrap();
    assert!(p1_rec.metrics.unwrap().consecutive_failures >= 1);

    // Path 2 should now be selected automatically as fallback
    assert!(
        wait_for_selected(id2.clone()).await,
        "Selector should fall back to Path 2"
    );

    // 3. Recovery Simulation: Re-enable Path 1
    p1_active.store(true, Ordering::SeqCst);

    // Wait for monitor to detect recovery and transition Path 1 back to Available
    assert!(
        wait_for_state(id1.clone(), PathState::Available).await,
        "Path 1 must recover to Available"
    );

    let paths_after_recovery = registry.get_paths(&peer_b).unwrap();
    let p1_rec_revived = paths_after_recovery.get(&id1).unwrap();
    assert_eq!(
        p1_rec_revived.metrics.unwrap().consecutive_failures,
        0,
        "Failure count resets to 0 on recovery"
    );

    // Path 1 should once again be selected over Path 2 due to lower RTT
    assert!(
        wait_for_selected(id1.clone()).await,
        "Selector should return to Path 1 on recovery"
    );

    // Clean shutdown of monitor
    monitor.shutdown().await;
    assert!(!monitor.is_running());
}
