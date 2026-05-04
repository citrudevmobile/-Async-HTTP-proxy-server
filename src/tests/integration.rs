//! Integration tests for the proxy server

use proxy_server::{ProxyServer, Result};
use std::time::Duration;
use std::sync::Arc;

#[tokio::test]
async fn test_rate_limiter() {
    let server = ProxyServer::new(5, 10); // 5 RPM
    
    // First 5 requests should succeed
    for i in 0..5 {
        let result = server.rate_limiter.acquire().await;
        assert!(result.is_ok(), "Request {} should succeed", i);
    }
    
    // 6th request should fail
    let result = server.rate_limiter.acquire().await;
    assert!(result.is_err(), "6th request should be rate limited");
}

#[tokio::test]
async fn test_route_management() {
    let server = ProxyServer::new(100, 10);
    
    // Add a route
    server.add_route("/test", "https://httpbin.org/get").await;
    
    // Verify it exists by trying to handle (will fail due to network, but that's ok)
    let result = server.handle_request("/test").await;
    // Network error is expected, but not route not found
    match result {
        Err(proxy_server::ProxyError::RouteNotFound(_)) => panic!("Route should exist"),
        _ => {} // Other errors are fine for this test
    }
    
    // Remove route
    server.remove_route("/test").await;
    
    // Now should get route not found
    let result = server.handle_request("/test").await;
    assert!(matches!(result, Err(proxy_server::ProxyError::RouteNotFound(_))));
}

#[tokio::test]
async fn test_connection_pool() {
    let pool = proxy_server::ConnectionPool::new(2);
    
    // Get two connections
    let client1 = pool.get_connection("example.com").await.unwrap();
    let client2 = pool.get_connection("example.com").await.unwrap();
    
    // Third should hit limit
    let result = pool.get_connection("example.com").await;
    assert!(result.is_err());
    
    // Return one connection
    pool.return_connection("example.com", client1).await;
    
    // Now can get another
    let client3 = pool.get_connection("example.com").await;
    assert!(client3.is_ok());
    
    drop(client2);
    drop(client3);
}

#[tokio::test]
async fn test_concurrent_requests() {
    let server = Arc::new(ProxyServer::new(100, 10));
    server.add_route("/api/users", "https://jsonplaceholder.typicode.com/users").await;
    
    let mut handles = vec![];
    
    for _ in 0..10 {
        let server_clone = server.clone();
        handles.push(tokio::spawn(async move {
            server_clone.handle_request("/api/users").await
        }));
    }
    
    let results = futures::future::join_all(handles).await;
    let success_count = results.iter()
        .filter(|r| r.as_ref().unwrap().is_ok())
        .count();
    
    // At least some should succeed
    assert!(success_count > 0);
}

#[test]
fn test_send_sync() {
    // Compile-time test to ensure types are Send + Sync
    fn assert_send_sync<T: Send + Sync>() {}
    
    assert_send_sync::<proxy_server::RateLimiter>();
    assert_send_sync::<proxy_server::ConnectionPool>();
    assert_send_sync::<proxy_server::ProxyServer>();
}
