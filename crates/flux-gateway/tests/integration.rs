use reqwest::StatusCode;
use serde_json::json;
use std::sync::Arc;
use tokio::net::TcpListener;

use flux_core::node::{FluxNode, NodeState};
use flux_core::peer::Peer;
use flux_core::transfer::TransferCancellation;
use flux_gateway::GatewayState;

// Helper to spawn a gateway instance on an ephemeral port
async fn spawn_test_gateway() -> (String, Arc<FluxNode>, GatewayState) {
    let mut node = FluxNode::new("test_profile");
    // Explicitly set state to Running to avoid Starting checks in status
    node.state = NodeState::Running;

    let node_arc = Arc::new(node);
    let state = GatewayState::new(node_arc.clone());

    // Bind to port 0 for ephemeral OS-allocated port
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{}", addr);

    let state_clone = state.clone();
    tokio::spawn(async move {
        let app = flux_gateway::routes::create_router(state_clone);
        axum::serve(listener, app).await.unwrap();
    });

    (base_url, node_arc, state)
}

#[tokio::test]
async fn test_get_identity() {
    let (url, node, _) = spawn_test_gateway().await;
    let client = reqwest::Client::new();

    let res = client
        .get(format!("{}/flux/v1/identity", url))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body["peer_id"], node.identity.to_string());
}

#[tokio::test]
async fn test_get_status() {
    let (url, node, _) = spawn_test_gateway().await;
    let client = reqwest::Client::new();

    let res = client
        .get(format!("{}/flux/v1/status", url))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let body: serde_json::Value = res.json().await.unwrap();

    assert_eq!(body["state"], "running");
    assert_eq!(body["peer_id"], node.identity.to_string());
    assert!(body["discovered_peer_count"].is_number());
    assert!(body["active_path_count"].is_number());
    assert!(body["active_transfer_count"].is_number());
}

#[tokio::test]
async fn test_peers_empty_and_retrieval() {
    let (url, node, _) = spawn_test_gateway().await;
    let client = reqwest::Client::new();

    // 1. List peers (should be empty initially)
    let res = client
        .get(format!("{}/flux/v1/peers", url))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body["peers"].as_array().unwrap().len(), 0);

    // 2. Inject a peer directly into Node's PeerRegistry to simulate discovery
    let peer_id = flux_core::identity::PeerId::new();
    let mock_peer = Peer {
        id: peer_id.clone(),
        address: "127.0.0.1:8080".to_string(),
        last_seen: std::time::Instant::now(),
    };
    node.registry.update(mock_peer);

    // 3. Request peer list again
    let res = client
        .get(format!("{}/flux/v1/peers", url))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body["peers"].as_array().unwrap().len(), 1);
    assert_eq!(body["peers"][0]["peer_id"], peer_id.to_string());
    assert_eq!(body["peers"][0]["address"], "127.0.0.1:8080");

    // 4. Get specific peer details
    let res = client
        .get(format!("{}/flux/v1/peers/{}", url, peer_id))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body["peer_id"], peer_id.to_string());

    // 5. Query non-existent peer
    let fake_id = uuid::Uuid::new_v4().to_string();
    let res = client
        .get(format!("{}/flux/v1/peers/{}", url, fake_id))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_connect_failures_and_validation() {
    let (url, _, _) = spawn_test_gateway().await;
    let client = reqwest::Client::new();

    // Bad UUID format
    let res = client
        .post(format!("{}/flux/v1/connect", url))
        .json(&json!({ "peer_id": "invalid-uuid" }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);

    // Non-existent peer
    let fake_id = uuid::Uuid::new_v4().to_string();
    let res = client
        .post(format!("{}/flux/v1/connect", url))
        .json(&json!({ "peer_id": fake_id }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_transfer_lifecycle_tracker() {
    let (url, _, state) = spawn_test_gateway().await;
    let client = reqwest::Client::new();

    let transfer_id = "test-transfer-1234".to_string();
    let peer_id = "some-peer-uuid".to_string();

    // 1. Validate manual registration in tracker works
    state
        .tracker
        .register(
            transfer_id.clone(),
            peer_id.clone(),
            2048,
            2,
            TransferCancellation::new(),
            None,
        )
        .await;

    // 2. Query state via HTTP
    let res = client
        .get(format!("{}/flux/v1/transfer/{}", url, transfer_id))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body["transfer_id"], transfer_id);
    assert_eq!(body["peer_id"], peer_id);
    assert_eq!(body["status"], "RUNNING");
    assert_eq!(body["total_bytes"], 2048);
    assert_eq!(body["total_files"], 2);

    // 3. Mark completed and re-query
    state.tracker.mark_completed(&transfer_id).await;
    let res = client
        .get(format!("{}/flux/v1/transfer/{}", url, transfer_id))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body["status"], "COMPLETED");

    // 4. Non-existent transfer
    let res = client
        .get(format!("{}/flux/v1/transfer/missing-id", url))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_gateway_transfer_cancel_lifecycle() {
    let (url, _, state) = spawn_test_gateway().await;
    let client = reqwest::Client::new();

    let transfer_id = "cancel-test-id-1".to_string();
    let peer_id = "some-peer-uuid".to_string();
    let cancel_token = TransferCancellation::new();

    state
        .tracker
        .register(
            transfer_id.clone(),
            peer_id.clone(),
            4096,
            1,
            cancel_token.clone(),
            None,
        )
        .await;

    // Cancel running transfer via HTTP endpoint
    let res = client
        .post(format!("{}/flux/v1/transfer/{}/cancel", url, transfer_id))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body["cancelled"], true);
    assert!(cancel_token.is_cancelled());

    // Verify status is now CANCELLED
    let res = client
        .get(format!("{}/flux/v1/transfer/{}", url, transfer_id))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body["status"], "CANCELLED");

    // Cancelling a completed transfer returns cancelled: false
    let completed_id = "completed-test-id-2".to_string();
    state
        .tracker
        .register(
            completed_id.clone(),
            peer_id.clone(),
            1024,
            1,
            TransferCancellation::new(),
            None,
        )
        .await;
    state.tracker.mark_completed(&completed_id).await;

    let res = client
        .post(format!("{}/flux/v1/transfer/{}/cancel", url, completed_id))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body["cancelled"], false);
}