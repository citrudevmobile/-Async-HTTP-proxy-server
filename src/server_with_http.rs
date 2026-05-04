//! HTTP server version of the proxy - accepts real HTTP requests

use proxy_server::ProxyServer;
use std::sync::Arc;
use std::net::SocketAddr;
use axum::{
    extract::{Path, Query},
    response::{IntoResponse, Json},
    routing::{get, post},
    Router,
    Extension,
};
use serde_json::json;
use std::collections::HashMap;
use tracing_subscriber;
use tracing::info;

#[tokio::main]
async fn main() {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();
    
    // Create the proxy server
    let server = Arc::new(ProxyServer::new(60, 10));
    
    // Add routes
    server.add_route("/api/users", "https://jsonplaceholder.typicode.com/users").await;
    server.add_route("/api/posts", "https://jsonplaceholder.typicode.com/posts").await;
    server.add_route("/api/comments", "https://jsonplaceholder.typicode.com/comments").await;
    server.add_route("/api/todos", "https://jsonplaceholder.typicode.com/todos").await;
    
    info!("Proxy server configured with routes");
    
    // Build the application
    let app = Router::new()
        .route("/health", get(health_handler))
        .route("/metrics", get(metrics_handler))
        .route("/routes", get(routes_handler))
        .route("/proxy/*path", get(proxy_handler))
        .route("/proxy/stream/*path", get(proxy_stream_handler))
        .layer(Extension(server));
    
    // Start the server
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    info!("HTTP proxy server listening on http://{}", addr);
    info!("Try: curl http://localhost:3000/proxy/api/users");
    info!("     curl http://localhost:3000/health");
    
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

/// Proxy handler for single requests
async fn proxy_handler(
    Extension(server): Extension<Arc<ProxyServer>>,
    Path(path): Path<String>,
) -> impl IntoResponse {
    let full_path = format!("/api/{}", path);
    match server.handle_request(&full_path).await {
        Ok(body) => {
            // Try to parse as JSON for pretty output
            if let Ok(json) = serde_json::from_str::<serde_json::Value>(&body) {
                Json(json).into_response()
            } else {
                body.into_response()
            }
        }
        Err(e) => {
            let error_response = json!({
                "error": e.to_string(),
                "path": full_path
            });
            (axum::http::StatusCode::TOO_MANY_REQUESTS, Json(error_response)).into_response()
        }
    }
}

/// Streaming proxy handler
async fn proxy_stream_handler(
    Extension(server): Extension<Arc<ProxyServer>>,
    Path(path): Path<String>,
) -> impl IntoResponse {
    use axum::response::sse::{Event, Sse};
    use futures::stream::StreamExt;
    
    let full_path = format!("/api/{}", path);
    let stream = server.stream_response(&full_path).await;
    
    Sse::new(stream.map(|chunk| {
        match chunk {
            Ok(text) => Ok(Event::default().data(text)),
            Err(e) => Ok(Event::default().data(format!("Error: {}", e))),
        }
    }))
}

/// Health check endpoint
async fn health_handler(Extension(server): Extension<Arc<ProxyServer>>) -> impl IntoResponse {
    let healthy = server.health_check().await;
    let status = if healthy {
        axum::http::StatusCode::OK
    } else {
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    };
    
    (status, Json(json!({
        "status": if healthy { "healthy" } else { "degraded" },
        "timestamp": chrono::Utc::now().to_rfc3339()
    })))
}

/// Metrics endpoint (Prometheus format)
async fn metrics_handler(Extension(server): Extension<Arc<ProxyServer>>) -> impl IntoResponse {
    let metrics = server.get_metrics().await;
    (axum::http::StatusCode::OK, metrics)
}

/// List configured routes
async fn routes_handler(Extension(server): Extension<Arc<ProxyServer>>) -> impl IntoResponse {
    // We'd need to expose a method to get routes
    Json(json!({
        "routes": [
            "/api/users -> jsonplaceholder.typicode.com/users",
            "/api/posts -> jsonplaceholder.typicode.com/posts",
            "/api/comments -> jsonplaceholder.typicode.com/comments",
            "/api/todos -> jsonplaceholder.typicode.com/todos"
        ]
    }))
}
