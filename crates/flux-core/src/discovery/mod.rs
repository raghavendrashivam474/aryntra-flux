use crate::identity::PeerId;
use crate::peer::{Peer, PeerRegistry};
use mdns_sd::{ServiceDaemon, ServiceEvent, ServiceInfo};
use socket2::{Domain, Protocol, Socket, Type};
use std::net::{SocketAddr, UdpSocket};
use std::time::{Duration, Instant};

pub const SERVICE_TYPE: &str = "_flux._udp.local.";
pub const BROADCAST_PORT: u16 = 9001;

pub fn start_discovery(peer_id: PeerId, registry: PeerRegistry) -> anyhow::Result<()> {
    // --- 1. mDNS (Standard Discovery) ---
    let mdns = ServiceDaemon::new()?;
    let service_name = format!("{}.{}", peer_id, SERVICE_TYPE);
    let my_info = ServiceInfo::new(
        SERVICE_TYPE,
        &peer_id.to_string(),
        &service_name,
        "",
        9000,
        None,
    )?;
    mdns.register(my_info)?;
    let receiver = mdns.browse(SERVICE_TYPE)?;

    let p_id = peer_id.clone();
    let reg = registry.clone();
    tokio::spawn(async move {
        while let Ok(event) = receiver.recv_async().await {
            if let ServiceEvent::ServiceResolved(info) = event {
                let id_str = info.get_fullname().split('.').next().unwrap_or("");
                if id_str != p_id.to_string() {
                    if let Ok(found_id) = serde_json::from_str::<PeerId>(&format!("\"{}\"", id_str))
                    {
                        reg.update(Peer {
                            id: found_id,
                            address: info
                                .get_addresses()
                                .iter()
                                .next()
                                .map(|a| a.to_string())
                                .unwrap_or_default(),
                            last_seen: Instant::now(),
                        });
                    }
                }
            }
        }
    });

    // --- 2. UDP Heartbeat (Resilient Fallback) ---
    // Transmitter
    let tx_socket = UdpSocket::bind("0.0.0.0:0")?;
    tx_socket.set_broadcast(true)?;
    let p_id_tx = peer_id.clone();
    tokio::spawn(async move {
        let msg = p_id_tx.to_string();
        loop {
            let _ = tx_socket.send_to(
                msg.as_bytes(),
                format!("255.255.255.255:{}", BROADCAST_PORT),
            );
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    });

    // Receiver (Using socket2 for address reuse)
    let addr: SocketAddr = format!("0.0.0.0:{}", BROADCAST_PORT).parse()?;
    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;

    // THE FIX: Allow multiple processes to bind to the same port
    socket.set_reuse_address(true)?;
    #[cfg(not(windows))]
    socket.set_reuse_port(true)?;

    socket.bind(&addr.into())?;
    let rx_socket: UdpSocket = socket.into();
    rx_socket.set_nonblocking(true)?;

    tokio::spawn(async move {
        let mut buf = [0u8; 1024];
        loop {
            if let Ok((len, addr)) = rx_socket.recv_from(&mut buf) {
                let id_str = String::from_utf8_lossy(&buf[..len]);
                if id_str != peer_id.to_string() {
                    if let Ok(found_id) = serde_json::from_str::<PeerId>(&format!("\"{}\"", id_str))
                    {
                        registry.update(Peer {
                            id: found_id,
                            address: addr.ip().to_string(),
                            last_seen: Instant::now(),
                        });
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    });

    Ok(())
}
