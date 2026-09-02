use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::timeout;

use flux_core::identity::PeerId;
use flux_core::protocol::{read_message, write_message, FluxMessage};
use flux_core::session::SessionBuilder;
use flux_core::transport::{TcpConnection, TcpTransport, Transport, TransportError};

#[tokio::test]
async fn test_tcp_transport_handshake_and_ping_pong() {
    let transport = TcpTransport::new();

    // 1. Bind to ephemeral port
    let listen_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let mut listener = transport
        .listen(listen_addr)
        .await
        .expect("Failed to listen");
    let actual_addr = listener.local_addr();

    let server_peer_id = PeerId::new();
    let client_peer_id = PeerId::new();

    let s_id = server_peer_id.clone();
    let expected_client_id = client_peer_id.clone();

    // 2. Server task: accept, server_handshake, receive ping, send pong
    let server_handle = tokio::spawn(async move {
        let (mut conn, _) = listener.accept().await.expect("Accept failed");

        let tcp_conn = conn
            .as_any_mut()
            .downcast_mut::<TcpConnection>()
            .expect("Expected TcpConnection");

        tcp_conn
            .server_handshake(&s_id)
            .await
            .expect("Server handshake failed");

        assert_eq!(conn.peer_id(), Some(&expected_client_id));

        let msg = conn
            .recv_message()
            .await
            .expect("Server recv message failed");
        match msg {
            FluxMessage::Ping { sequence, payload } => {
                assert_eq!(sequence, 1);
                assert_eq!(payload, "Hello Flux");
                let pong = FluxMessage::pong(sequence, "Pong from server".to_string());
                conn.send_message(&pong)
                    .await
                    .expect("Server send pong failed");
            }
            other => panic!("Expected Ping, got {:?}", other),
        }

        conn.close().await.expect("Server close failed");
    });

    // 3. Client task: connect via SessionBuilder, send ping, receive pong
    let client_handle = tokio::spawn(async move {
        let session_builder = SessionBuilder::new(&transport, client_peer_id);
        let mut session = session_builder
            .connect(&server_peer_id, actual_addr)
            .await
            .expect("Client connect & handshake failed");

        assert_eq!(session.peer_id(), Some(&server_peer_id));

        let ping = FluxMessage::ping(1, "Hello Flux".to_string());
        session
            .send_message(&ping)
            .await
            .expect("Client send ping failed");

        let response = timeout(Duration::from_secs(5), session.recv_message())
            .await
            .expect("Client recv timeout")
            .expect("Client recv failed");

        match response {
            FluxMessage::Pong { sequence, payload } => {
                assert_eq!(sequence, 1);
                assert_eq!(payload, "Pong from server");
            }
            other => panic!("Expected Pong, got {:?}", other),
        }

        session.close().await.expect("Client session close failed");
    });

    let (s_res, c_res) = tokio::join!(server_handle, client_handle);
    s_res.expect("Server task panicked");
    c_res.expect("Client task panicked");
}

#[tokio::test]
async fn test_handshake_version_mismatch() {
    let transport = TcpTransport::new();
    let listen_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let mut listener = transport.listen(listen_addr).await.unwrap();
    let actual_addr = listener.local_addr();

    let server_peer_id = PeerId::new();
    let client_peer_id = PeerId::new();

    let s_id = server_peer_id.clone();
    let server_handle = tokio::spawn(async move {
        let (mut conn, _) = listener.accept().await.unwrap();
        let tcp_conn = conn.as_any_mut().downcast_mut::<TcpConnection>().unwrap();
        let result = tcp_conn.server_handshake(&s_id).await;
        assert!(result.is_err());
        match result {
            Err(TransportError::HandshakeFailed(msg)) => {
                assert!(msg.contains("Protocol version mismatch"));
            }
            other => panic!("Expected HandshakeFailed, got {:?}", other),
        }
    });

    let client_handle = tokio::spawn(async move {
        use tokio::net::TcpStream;
        let mut stream = TcpStream::connect(actual_addr).await.unwrap();

        // Send Hello with wrong version
        let invalid_hello = FluxMessage::Hello {
            version: 999,
            peer_id: client_peer_id,
        };
        write_message(&mut stream, &invalid_hello).await.unwrap();
    });

    let (s_res, c_res) = tokio::join!(server_handle, client_handle);
    s_res.unwrap();
    c_res.unwrap();
}

#[tokio::test]
async fn test_multiple_messages_in_stream() {
    let (mut client_stream, mut server_stream) = tokio::io::duplex(4096);

    let sender = tokio::spawn(async move {
        for i in 1..=5 {
            let msg = FluxMessage::ping(i, format!("Message {}", i));
            write_message(&mut client_stream, &msg).await.unwrap();
        }
    });

    let receiver = tokio::spawn(async move {
        for i in 1..=5 {
            let msg = read_message(&mut server_stream).await.unwrap();
            match msg {
                FluxMessage::Ping { sequence, payload } => {
                    assert_eq!(sequence, i);
                    assert_eq!(payload, format!("Message {}", i));
                }
                other => panic!("Expected Ping, got {:?}", other),
            }
        }
    });

    let (s_res, r_res) = tokio::join!(sender, receiver);
    s_res.unwrap();
    r_res.unwrap();
}
