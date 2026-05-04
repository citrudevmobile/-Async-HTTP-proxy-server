//! Proxy server demo application

use proxy_server::{ProxyServer, WithRateLimiting};
use std::sync::Arc;
use std::time::Duration;
use tracing_subscriber;
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();
    
    println!("=== Rust Async Proxy Server Demo ===\n");
    
    // Create server with 60 requests per minute, 5 max connections per host
    let server = Arc::new(ProxyServer::new(60, 5));
    
    // Add route mappings
    server.add_route("/api/users", "https://jsonplaceholder.typicode.com/users").await;
    server.add_route("/api/posts", "https://jsonplaceholder.typicode.com/posts").await;
    server.add_route("/api/comments", "https://jsonplaceholder.typicode.com/comments").await;
    
    println!("Routes configured:");
    println!("  /api/users -> jsonplaceholder.typicode.com/users");
    println!("  /api/posts -> jsonplaceholder.typicode.com/posts");
    println!("  /api/comments -> jsonplaceholder.typicode.com/comments");
    println!("\nStarting concurrent requests...\n");
    
    // Demonstrate concurrent requests with shared ownership
    let mut handles = vec![];
    
    for i in 0..10 {
        let server_clone = server.clone();
        handles.push(tokio::spawn(async move {
            let result = server_clone.handle_request("/api/users").await;
            match result {
                Ok(body) => {
                    let preview = if body.len() > 50 {
                        format!("{}...", &body[..50])
                    } else {
                        body.clone()
                    };
                    println!("✅ Request {} succeeded ({} bytes) - {}", i, body.len(), preview);
                }
                Err(e) => println!("❌ Request {} failed: {}", i, e),
            }
        }));
    }
    
    // Wait for all concurrent requests
    for handle in handles {
        handle.await?;
    }
    
    // Demonstrate streaming
    println!("\n=== Streaming Demo ===");
    let stream = server.stream_response("/api/posts").await;
    let mut stream = std::pin::pin!(stream);
    
    let mut chunk_count = 0;
    let mut total_bytes = 0;
    
    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(text) => {
                chunk_count += 1;
                total_bytes += text.len();
                println!("  Chunk {}: {} chars", chunk_count, text.len());
                if chunk_count >= 5 {
                    println!("  ... (streaming continues, stopping early for demo)");
                    break;
                }
            }
            Err(e) => println!("  Stream error: {}", e),
        }
    }
    println!("  Total streamed: {} bytes in {} chunks", total_bytes, chunk_count);
    
    // Demonstrate connection pool stats
    println!("\n=== Connection Pool Statistics ===");
    let stats = server.connection_pool.stats().await;
    for (host, count) in stats {
        println!("  {}: {} idle connections", host, count);
    }
    
    // Demonstrate health check
    println!("\n=== Health Check ===");
    let healthy = server.health_check().await;
    println!("  Server health: {}", if healthy { "OK" } else { "DEGRADED" });
    
    // Demonstrate custom future with rate limiting
    println!("\n=== Rate Limited Future Demo ===");
    let limiter = server.rate_limiter.clone();
    let future = async {
        // Simulate work
        tokio::time::sleep(Duration::from_millis(10)).await;
        "Hello from rate limited future"
    };
    
    let result = future.with_rate_limit(limiter).await;
    match result {
        Ok(msg) => println!("  Rate limited future completed: {}", msg),
        Err(e) => println!("  Rate limited future failed: {}", e),
    }
    
    // Demonstrate metrics endpoint
    println!("\n=== Prometheus Metrics ===");
    let metrics = server.get_metrics().await;
    let metric_lines: Vec<&str> = metrics.lines().take(10).collect();
    for line in metric_lines {
        if line.starts_with('#') {
            println!("  {}", line);
        }
    }
    println!("  ... (more metrics available)");
    
    println!("\n=== Server Shutdown ===");
    drop(server);
    println!("  Server dropped, cleaning up connections...");
    
    Ok(())
}
